# Tendermint DB

Database abstractions to be used in applications. These abstractions are not only meant to be
used in applications built on [Tendermint](https://github.com/tendermint/tendermint), but can be
used in a variety of applications.

### Minimum Go Version

Go 1.13+

## Supported Databases

* **[GoLevelDB](https://github.com/syndtr/goleveldb) [stable]**: A pure Go implementation of [LevelDB](https://github.com/google/leveldb) (see below). Currently the default on-disk database used in the Cosmos SDK.

* **MemDB [stable]:** An in-memory database using [Google's B-tree package](https://github.com/google/btree). Has very high performance both for reads, writes, and range scans, but is not durable and will lose all data on process exit. Does not support transactions. Suitable for e.g. caches, working sets, and tests. Used for [IAVL](https://github.com/tendermint/iavl) working sets when the pruning strategy allows it.

* **[LevelDB](https://github.com/google/leveldb) [experimental]:** A [Go wrapper](https://github.com/jmhodges/levigo) around [LevelDB](https://github.com/google/leveldb). Uses LSM-trees for on-disk storage, which have good performance for write-heavy workloads, particularly on spinning disks, but requires periodic compaction. Does not support transactions.

* **[BoltDB](https://github.com/etcd-io/bbolt) [experimental]:** A [fork](https://github.com/etcd-io/bbolt) of [BoltDB](https://github.com/boltdb/bolt). Uses B+trees for on-disk storage, which have good performance for range scans and read-heavy workloads. Supports serializable ACID transactions.

* **[RocksDB](https://github.com/tecbot/gorocksdb) [experimental]:** A [Go wrapper](https://github.com/tecbot/gorocksdb) around [RocksDB](https://rocksdb.org). Similarly to LevelDB (above) it uses LSM-trees for on-disk storage, but is optimized for fast storage media such as SSDs and memory. Supports atomic transactions, but not full ACID transactions.

## Meta-databases

* **PrefixDB [stable]:** A database which wraps another database and uses a static prefix for all keys. This allows multiple logical databases to be stored in a common underlying databases by giving them different namespaces. Used by the Cosmos SDK to give different modules their own namespaced database in a single application database.

* **RemoteDB [experimental]:** A database that connects to distributed Tendermint db instances via [gRPC](https://grpc.io/). This can help with detaching difficult deployments such as LevelDB, and can also ease dependency management for Tendermint developers.

## Tests

To test common databases, run `make test`. If all databases are available on the local machine, use `make test-all` to test them all.
