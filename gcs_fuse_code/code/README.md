## What does workers.cpp do?

1. Workers.cpp creates multiple threads.

2. Each thread (check writer function) accesses rocksdb concurrently.

3. Every thread uses a single key, but value to be **Put** is generated randomly.

4. Each thread performs **Put** operation a random number of times. 


## Why was this code written this way?

It was written to check whether concurrent reads and writes are atomic for GCSFUSE. This was not verified because of another issue. Check the README.md in **gcs_fuse_directory** for issue encountered.