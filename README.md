# qtest

Asynq queue test

To run the test app:

1. Ensure you have [go installed](https://go.dev/doc/install).
2. Clone this repo locally.

Then:

```zsh
cd /PATH/TO/github.com/charl/qtest
go run cmd/qtest/main.go -r redis-ingest-staging.mist.pvt:6379 -j 10
```

To get help:

```zsh
go run cmd/qtest/main.go --help
Usage of /var/folders/sf/jbxl83ks6mv0_9v8dt6kdgzc0000gn/T/go-build3865798883/b001/exe/main:
  -cc int
        Number of concurrent consumers, defaults to 2048 (default 2048)
  -j int
        Number of numJto submit, defaults to 1 (default 1)
  -r string
        Redis server address, defaults to 127.0.0.1:6379 (default "127.0.0.1:6379")
```
