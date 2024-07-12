# Bitcask architecture

- Current version is a proof of concept, with no support for compaction. But we have a few of the features from the original paper working:
    - Get
    - Put
    - Delete
    - KeyDir creation on startup.
- This version is fully single threaded and does not have any notion of concurrent access.

## Possible future

- My idea is to have an embeddable db in the form of a library. 
- `get`, `put` and `delete` are threadsafe and atomic operations.
- Flushes to the file are handled via a background threadpool to ensure expensive file writes are not in the synchronous path.
    - Raises a question: calling `write()` syscall is expensive, and doing so for every insert would be a major bottleneck. Batching comes to mind, but how would you handle it in the face of a crash?
- Compaction is _also_ handled via the same threadpool. Though rocksdb seems to be using separate threadpools for compaction and FS flushes as the latter takes priority.
- We probably need an FS abstraction, to control the writes and reads to the right files.
