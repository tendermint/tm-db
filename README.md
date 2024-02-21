# CometBFT DB

[![version](https://img.shields.io/github/tag/cometbft/cometbft-db.svg)](https://github.com/cometbft/cometbft-db/releases/latest)
[![license](https://img.shields.io/github/license/cometbft/cometbft-db.svg)](https://github.com/cometbft/cometbft-db/blob/main/LICENSE)
[![API Reference](https://camo.githubusercontent.com/915b7be44ada53c290eb157634330494ebe3e30a/68747470733a2f2f676f646f632e6f72672f6769746875622e636f6d2f676f6c616e672f6764646f3f7374617475732e737667)](https://pkg.go.dev/github.com/cometbft/cometbft-db)
[![codecov](https://codecov.io/gh/cometbft/cometbft-db/branch/main/graph/badge.svg)](https://codecov.io/gh/cometbft/cometbft-db)
![Lint](https://github.com/cometbft/cometbft-db/workflows/Lint/badge.svg?branch=main)
![Test](https://github.com/cometbft/cometbft-db/workflows/Test/badge.svg?branch=main)

A fork of [tm-db].

Common database interface for various database backends. Primarily meant for
applications built on [CometBFT], such as the [Cosmos SDK].

**NB:** As per [cometbft/cometbft\#48], the CometBFT team plans on eventually
totally deprecating and removing this library from CometBFT. As such, we do not
recommend depending on this library for new projects.

## Minimum Go Version

Go 1.21+

## Supported Database Backends

- **[GoLevelDB](https://github.com/syndtr/goleveldb) [stable]**: A pure Go
  implementation of [LevelDB](https://github.com/google/leveldb) (see below).
  Currently the default on-disk database used in the Cosmos SDK.

- **[PebbleDB](https://github.com/cockroachdb/pebble) [experimental]:** Pebble
  is a LevelDB/RocksDB inspired key-value store focused on performance and
  internal usage by CockroachDB. Pebble inherits the RocksDB file formats and a
  few extensions such as range deletion tombstones, table-level bloom filters,
  and updates to the MANIFEST format.

  CAVEAT: there are reports of broken upgrade process when using [Cosmos
  SDK](https://github.com/cosmos/cosmos-sdk).

## Meta-databases

- **PrefixDB [stable]:** A database which wraps another database and uses a
  static prefix for all keys. This allows multiple logical databases to be
  stored in a common underlying databases by using different namespaces. Used by
  the Cosmos SDK to give different modules their own namespaced database in a
  single application database.

## Tests

To test common databases, run `make test`. If all databases are available on the
local machine, use `make test-all` to test them all.

To test all databases within a Docker container, run:

```bash
make docker-test-image
make docker-test
```

[tm-db]: https://github.com/tendermint/tm-db
[CometBFT]: https://github.com/cometbft/cometbft-db
[Cosmos SDK]: https://github.com/cosmos/cosmos-sdk
[cometbft/cometbft\#48]: https://github.com/cometbft/cometbft/issues/48
