# A minimal one-sided buffer based on MPI-3 RMA

A simple one-sided RMA buffer supporting non-blocking concurrent writes. The
idea is to use this as a building block in applications that need one-sided
aggregation of small messages (for example, in implementing an active messaage
scheme).

## Implementation

The whole idea is quite simple: each rank on the selected communicator exposes
a buffer of fixed length to accommodate incoming messages (also fixed length).
Any rank can insert a message into the buffer on any other rank, but only the
owner rank of the buffer can remove messages from it.

By default, the message `put` operation is non-blocking: in the event that the
target buffer is full and cannot accommodate additional messages, the `put`
simply returns and indicates that it did not complete. A blocking mode is also
available, but care must be taken to avoid deadlock (namely, to ensure that the
target rank makes progress in emptying its buffer).

Mutual exclusion is enforced using a pair of counters for each buffer: one for
reserving an index into the buffer to which a message may be written, and
another for indicating completion of the write at that index. If the
reservation counter is greater than or equal to the length of the buffer, the
latter is considered locked (unavailable for writes) on that rank. In this
sense, the owner rank is able to explicitly lock the buffer by setting the
reservation counter as such.

The `put` operation on a remote buffer proceeds as follows:

1. Atomically get-and-increment the reservation counter (`MPI_Fetch_and_op`)
2. If the counter is `>=` the buffer length, we either (a) fail and return or
   (b) sleep and retry (i.e. block)
3. Write the message to the reserved index on the target
4. Atomically increment the completion counter (`MPI_Accumulate`)

The `get` operation on the local buffer proceeds as follows:

1. Atomically get-and-set the reservation counter to the buffer length 
   (`MPI_Fetch_and_op`)
2. If the reservation counter was non-zero, block until the completion counter
   reaches this value, otherwise return
3. Once it has, copy out the message list
4. Zero the completion counter for later reuse
5. Zero the reservation counter, thereby making the buffer once again available
   for writes (i.e. unlocked)
