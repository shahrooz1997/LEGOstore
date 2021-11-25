## What does workers.cpp do?

1. Workers.cpp creates multiple threads.

2. Each thread (check writer function) accesses rocksdb concurrently.

3. Every thread uses a single key, but value to be **Put** is generated randomly.

4. Each thread performs **Put** operation a random number of times. 