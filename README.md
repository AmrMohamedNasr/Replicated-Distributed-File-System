# Replicated Distributed File System
---
## Overview
It is required to implement a replicated file system. There will be one main server (master) and, data will be replicated on multiple replicaServers. This file system allows its concurrent users to perform transactions, while guaranteeing ACID properties. This means that the following need to be ensured:
* The master server maintains metadata about the replicas and their locations.
* The user can submit multiple operations that are performed atomically on the shared files stored in the distributed file system.
* Files are not partitioned.
* Assumption: each transaction access only one file.
* Each file stored on the distributed file system has a primary replica. This means that you will need to implement sequential consistency through a protocol similar to the passive (primary-backup) replication protocol.
* After performing a set of operations, it is guaranteed that the file system will remain in
consistent state.
* A lock manager is maintained at each replicaServer.
* Once any transaction is committed, its mutations to files stored in the file system are required to be durable.

Further details can be found in the PDFs present in this repository.

---