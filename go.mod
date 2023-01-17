module github.com/cometbft/cometbft-db

go 1.18

require (
	github.com/dgraph-io/badger/v2 v2.2007.4
	github.com/gogo/protobuf v1.3.2
	github.com/google/btree v1.1.2
	github.com/jmhodges/levigo v1.0.0
	github.com/stretchr/testify v1.8.1
	github.com/syndtr/goleveldb v1.0.1-0.20200815110645-5c35d600f0ca
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	go.etcd.io/bbolt v1.3.6
	google.golang.org/grpc v1.52.0
)

require (
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.0.3-0.20200630154024-f66de99634de // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/klauspost/compress v1.12.3 // indirect
	github.com/pkg/errors v0.8.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.4.0 // indirect
	golang.org/x/sys v0.3.0 // indirect
	golang.org/x/text v0.5.0 // indirect
	google.golang.org/genproto v0.0.0-20221118155620-16455021b5e6 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Breaking changes were released with the wrong tag (use v0.6.6 or later).
retract v0.6.5

// Note: gorocksdb bindings for OptimisticTransactionDB are not merged upstream, so we use a fork
// See https://github.com/tecbot/gorocksdb/pull/216
replace github.com/tecbot/gorocksdb => github.com/cosmos/gorocksdb v1.1.1
