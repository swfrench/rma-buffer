/**
 * \defgroup RMABuff One-sided buffer based on MPI-3 RMA
 * @{ */

#pragma once

#include <algorithm>
#include <chrono>
#include <thread>

#include <cstdio>
#include <cstring>
#include <cassert>

#include <mpi.h>

#define mpi_assert(X) assert(X == MPI_SUCCESS)

/**
 * A minimal one-sided buffer implementation based on MPI-3 RMA
 *
 * Maintains a fixed-length local buffer of fixed length messages. Messages are
 * enqueued on the desired target using the \c put method, while accumulated
 * messages are retrieved from the local buffer using the \c get method.
 *
 * @tparam T Message type (messages are fixed length arrays of type \c T)
 */
template<typename T>
class rma_buff
{
  int _msg_len, _buff_len, _rank;
  int *_reserve;
  int *_complete;

  T *_buff;

  MPI_Comm _comm;
  MPI_Win _win_reserve, _win_complete, _win_buff;

  /*
   * Internal: Only if the completion counter (`_complete`) is equal to
   * `complete_thresh` (typically the value of the reservation counter prior to
   * the buffer having been closed for writes):
   *  1. copy the current buffer into buff_out;
   *  2. zero `_complete`; and
   *  3. zero `_reserve`.
   * Reminder: the buffer remains locked for writes until `_reserve` is below
   * `_buff_len`. This therefore needs to be the very last operation.
   */
  int _get_thresh(T* buff_out, int complete_thresh)
  {
    int curr_complete, zero = 0;

    assert(complete_thresh > 0);

    // get the current value of the completion counter
    mpi_assert(MPI_Win_lock(MPI_LOCK_EXCLUSIVE, _rank, 0, _win_complete));
    curr_complete = *_complete;
    mpi_assert(MPI_Win_unlock(_rank, _win_complete));

    // if the buffer does not have at least complete_thresh messages in it,
    // do nothing and return 0
    if (curr_complete < complete_thresh)
      return 0;

    // sanity check: _complete should never be larger than the anticipated
    // value from _reserve (or _buff_len if _reserve has already exceeded the
    // latter and the buffer is locked)
    assert(curr_complete == complete_thresh);

    // the buffer is full ...

    // copy to buff_out (assumed pre-allocated)
    mpi_assert(MPI_Win_lock(MPI_LOCK_EXCLUSIVE, _rank, 0, _win_buff));
    mpi_assert(MPI_Get(buff_out,
                       _buff_len * _msg_len * sizeof(T), MPI_BYTE, _rank, 0,
                       _buff_len * _msg_len * sizeof(T), MPI_BYTE,
                       _win_buff));
    mpi_assert(MPI_Win_unlock(_rank, _win_buff));

    // zero the completion counter and reservation count in *that* order ...

    mpi_assert(MPI_Win_lock(MPI_LOCK_EXCLUSIVE, _rank, 0, _win_complete));
    mpi_assert(MPI_Put(&zero, 1, MPI_INT, _rank, 0, 1, MPI_INT, _win_complete));
    mpi_assert(MPI_Win_unlock(_rank, _win_complete));

    mpi_assert(MPI_Win_lock(MPI_LOCK_EXCLUSIVE, _rank, 0, _win_reserve));
    mpi_assert(MPI_Put(&zero, 1, MPI_INT, _rank, 0, 1, MPI_INT, _win_reserve));
    mpi_assert(MPI_Win_unlock(_rank, _win_reserve));

    // return the number of messages copied out
    return curr_complete;
  }

 public:

  /**
   * Initialize the buffer object
   *
   * \param msg_len  Number of elements of type \c T comprising each message
   * \param buff_len Number of message slots to expose in the buffer
   * \param comm     MPI communicator over the set of ranks participating in
   *                 buffer operations (a local \b duplicate of this
   *                 communicator will be used internally)
   */
  rma_buff(int msg_len, int buff_len, MPI_Comm comm) :
    _msg_len(msg_len), _buff_len(buff_len)
  {
    mpi_assert(MPI_Comm_dup(comm, &_comm));

    mpi_assert(MPI_Comm_rank(_comm, &_rank));

    mpi_assert(MPI_Alloc_mem(sizeof(int), MPI_INFO_NULL, &_reserve));
    mpi_assert(MPI_Alloc_mem(sizeof(int), MPI_INFO_NULL, &_complete));
    mpi_assert(MPI_Alloc_mem(_msg_len * _buff_len * sizeof(T), MPI_INFO_NULL,
                             &_buff));

    *_reserve = 0;
    *_complete = 0;

    mpi_assert(MPI_Win_create(_reserve, sizeof(int), sizeof(int),
                              MPI_INFO_NULL,
                              _comm, &_win_reserve));
    mpi_assert(MPI_Win_create(_complete, sizeof(int), sizeof(int),
                              MPI_INFO_NULL,
                              _comm, &_win_complete));
    mpi_assert(MPI_Win_create(_buff,
                              _msg_len * _buff_len * sizeof(T),
                              _msg_len * sizeof(T),
                              MPI_INFO_NULL,
                              _comm, &_win_buff));

    mpi_assert(MPI_Barrier(_comm));
  }

  ~rma_buff()
  {
    mpi_assert(MPI_Barrier(_comm));
    mpi_assert(MPI_Win_free(&_win_reserve));
    mpi_assert(MPI_Win_free(&_win_complete));
    mpi_assert(MPI_Win_free(&_win_buff));
    mpi_assert(MPI_Free_mem(_reserve));
    mpi_assert(MPI_Free_mem(_complete));
    mpi_assert(MPI_Free_mem(_buff));
  }

  /**
   * Get messages from the local buffer
   *
   * Messages are copied to \c buff_out and the reservation counter is zeroed,
   * allowing the local buffer to be overwritten. This routine returns the
   * number of messages copied to \c buff_out.
   *
   * The default behavior is to do the above *only* if the buffer is full (i.e.
   * contains \c buff_len messages). This can be modified using the optional
   * \c min_msg parameter, which specifies the minimum number of messages that
   * must be waiting in the buffer (must be > 0).
   *
   * \param buff_out output buffer to which messages are copied (assumed
   *                 preallocated to accomodate at least \c buff_len messages)
   * \param min_msg  optional: minimum number of messages to get
   */
  int get(T* buff_out, int min_msg = 0)
  {
    int curr_reserve;

    // sanity check on min_msg
    assert(min_msg >= 0);
    if (min_msg == 0)
      min_msg = _buff_len;

    // push _reserve over the writable threshold if it is currently non-zero but
    // below the threshold, thereby marking the buffer as locked; remember
    // previous value
    mpi_assert(MPI_Win_lock(MPI_LOCK_EXCLUSIVE, _rank, 0, _win_reserve));
    mpi_assert(MPI_Fetch_and_op(&_buff_len, &curr_reserve, MPI_INT, _rank, 0,
                                MPI_MAX, _win_reserve));
    mpi_assert(MPI_Win_unlock(_rank, _win_reserve));

    // return if curr_reserve is zero (there are no messages)
    if (curr_reserve < min_msg) {
      mpi_assert(MPI_Win_lock(MPI_LOCK_EXCLUSIVE, _rank, 0, _win_reserve));
      mpi_assert(MPI_Put(&curr_reserve, 1, MPI_INT, _rank, 0, 1, MPI_INT,
                         _win_reserve));
      mpi_assert(MPI_Win_unlock(_rank, _win_reserve));
      return 0;
    }

    // wait for _complete to meet curr_reserve prior to our increasing it,
    // using the machinery in _get_thresh to achieve this
    // reminder: curr_reserve may already be above the threshold, so we need to
    // account for this (_complete can only ever be as large as _buff_len)
    int n;
    while ((n = _get_thresh(buff_out, std::min(curr_reserve, _buff_len))) == 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return n;
  }

  /**
   * Write the message \c msg to the buffer at rank \c target
   *
   * By default, this routine is non-blocking - if the buffer on \c target is
   * already full, then \c put fails and returns \c false. Otherwise, on
   * success, the routine turns \c true.
   *
   * Optionally, you can tell \c put to block until the target buffer is
   * available for writes (i.e. non-full). See the \c block argument.
   *
   * \param target target rank for buffer
   * \param msg    pointer to message (array of \c msg_len elems of type \c T)
   * \param block  optional: whether to block until the \c put completes
   */
  bool put(int target, T *msg, bool block = false)
  {
    int off, inc = 1;

    // reserve an offset in the buffer
    do {
      mpi_assert(MPI_Win_lock(MPI_LOCK_SHARED, target, 0, _win_reserve));
      mpi_assert(MPI_Fetch_and_op(&inc, &off, MPI_INT, target, 0, MPI_SUM,
                                  _win_reserve));
      mpi_assert(MPI_Win_unlock(target, _win_reserve));
      if (off < _buff_len) break;
      if (! block) return false;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (1);

#ifdef DEBUG
    printf("%4.4i: put on rank %i at offset %i\n", _rank, target, off);
#endif

    // write msg to the remote buffer at the reserved offset
    mpi_assert(MPI_Win_lock(MPI_LOCK_EXCLUSIVE, target, 0, _win_buff));
    mpi_assert(MPI_Put(msg,
                       _msg_len * sizeof(T), MPI_BYTE, target, off,
                       _msg_len * sizeof(T), MPI_BYTE,
                       _win_buff));
    mpi_assert(MPI_Win_unlock(target, _win_buff));
    
    // increment the completion counter and return true (success)
    mpi_assert(MPI_Win_lock(MPI_LOCK_SHARED, target, 0, _win_complete));
    mpi_assert(MPI_Accumulate(&inc, 1, MPI_INT, target, 0, 1, MPI_INT,
                              MPI_SUM, _win_complete));
    mpi_assert(MPI_Win_unlock(target, _win_complete));
    return true;
  }
};

/** @} */
