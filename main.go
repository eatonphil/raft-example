package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type kvFsm struct {
	db *pebble.DB
}

type setPayload struct {
	Key   string
	Value []byte
}

func (kf *kvFsm) Apply(log *raft.Log) any {
	switch log.Type {
	case raft.LogCommand:
		var sp setPayload
		err := json.Unmarshal(log.Data, &sp)
		if err != nil {
			return fmt.Errorf("Could not parse payload: %w", err)
		}

		err = kf.db.Set([]byte(sp.Key), sp.Value, pebble.Sync)
		if err != nil {
			return fmt.Errorf("Could not set key-value: %w", err)
		}
	default:
		return fmt.Errorf("Unknown raft log type: %#v", log.Type)
	}

	return nil
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error { return nil }
func (sn snapshotNoop) Release()                          {}

func (kf *kvFsm) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

func (kf *kvFsm) Restore(rc io.ReadCloser) error {
	// deleting first isn't really necessary since there's no exposed DELETE operation anyway.
	// so any changes over time will just get naturally overwritten

	decoder := json.NewDecoder(rc)

	for decoder.More() {
		var sp setPayload
		err := decoder.Decode(&sp)
		if err != nil {
			return fmt.Errorf("Could not decode payload: %w", err)
		}

		err = kf.db.Set([]byte(sp.Key), sp.Value, pebble.Sync)
		if err != nil {
			return fmt.Errorf("Could not set")
		}
	}

	return rc.Close()
}

func setupRaft(dir, nodeId, raftPort string, kf *kvFsm) (*raft.Raft, error) {
	store, err := raftboltdb.NewBoltStore(path.Join(dir, "raft_store"))
	if err != nil {
		return nil, fmt.Errorf("Could not create bolt store: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(path.Join(dir, "raft_snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("Could not create snapshot store: %w", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftPort)
	if err != nil {
		return nil, fmt.Errorf("Could not resolve address: %w", err)
	}

	transport, err := raft.NewTCPTransport(raftPort, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("Could not create tcp transport: %w", err)
	}

	r, err := raft.NewRaft(raft.DefaultConfig(), kf, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("Could not create raft instance: %w", err)
	}

	// Cluster consists of unjoined leaders. Picking a leader and
	// creating a real cluster is done manually after startup.
	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(nodeId),
				Address: transport.LocalAddr(),
			},
		},
	})

	return r, nil
}

type httpServer struct {
	db *pebble.DB
	r  *raft.Raft
}

func (hs httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Could not read key-value in http request: %w", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	future := hs.r.Apply(bs, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		log.Printf("Could not write key-value: %w", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	err = future.Response().(error)
	if err != nil {
		log.Printf("Could not write key-value: %w", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	valBytes, closer, err := hs.db.Get([]byte(r.URL.Query().Get("key")))
	if err != nil {
		log.Printf("Could not encode key-value in http response: %w", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer closer.Close()

	rsp := struct {
		Data string `json:"data"`
	}{string(valBytes)}
	err = json.NewEncoder(w).Encode(rsp)
	if err != nil {
		log.Printf("Could not encode key-value in http response: %w", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

type config struct {
	id       string
	httpPort string
	raftPort string
}

func getConfig() config {
	cfg := config{}
	for i, arg := range os.Args[1:] {
		if arg == "--node-id" {
			cfg.id = os.Args[i+1]
			i++
			continue
		}

		if arg == "--http-port" {
			cfg.httpPort = os.Args[i+1]
			i++
			continue
		}

		if arg == "--raft-port" {
			cfg.raftPort = os.Args[i+1]
			i++
			continue
		}
	}

	if cfg.id == "" {
		log.Fatal("Missing required parameter: --node-id")
	}

	if cfg.raftPort == "" {
		log.Fatal("Missing required parameter: --raft-port")
	}

	if cfg.httpPort == "" {
		log.Fatal("Missing required parameter: --http-port")
	}

	return cfg
}

func main() {
	cfg := getConfig()

	db, err := pebble.Open("data"+cfg.id, &pebble.Options{})
	if err != nil {
		log.Fatalf("Could not open pebble db: %w", err)
	}

	kf := &kvFsm{db}

	r, err := setupRaft("raft"+cfg.id, cfg.id, cfg.raftPort, kf)
	if err != nil {
		log.Fatal(err)
	}

	hs := httpServer{db, r}

	http.HandleFunc("/set", hs.setHandler)
	http.HandleFunc("/get", hs.getHandler)
	http.ListenAndServe(":"+cfg.httpPort, nil)
}
