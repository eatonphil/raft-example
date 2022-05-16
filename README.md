# Minimal hashicorp/raft example

Example:

Terminal 1:

```bash
$ go build
$ ./raft-example --node-id node1 --raft-port 2222 --http-port 8222
```

Terminal 2:

```bash
$ go build
$ ./raft-example --node-id node2 --raft-port 2223 --http-port 8223
```

Terminal 3, tell 1 to have 2 follow it:

```bash
$ curl 'localhost:8222/join?followerAddr=localhost:2223&followerId=node2'
```

Terminal 3, now add a key:

```bash
$ curl -X POST 'localhost:8222/set' -d '{"key": "x", "value": "23"}' -H 'content-type: application/json'
```

Terminal 3, now get the key from either server:

```bash
$ curl 'localhost:8222/get?key=x'
{"data":"23"}
$ curl 'localhost:8223/get?key=x'
{"data":"23"}
```

References:

* https://yusufs.medium.com/creating-distributed-kv-database-by-implementing-raft-consensus-using-golang-d0884eef2e28
* https://github.com/Jille/raft-grpc-example
* https://github.com/otoolep/hraftd
* https://pkg.go.dev/github.com/hashicorp/raft
