# Bitcask-rs

Bitcask-rs is a Rust implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf), the hash-based, log-structured key-value store.
Long Term Goals

- Immutable data files
- Compaction and hint files
- Pluggable storage backends
- Clustering
- Consensus based on RAFT
- Distributed membership based on the RAPID distributed membership paper

Currently Implemented

- Durable write-ahead log storage
- Atomic get, put, remove operations
- Thread-safe by default
