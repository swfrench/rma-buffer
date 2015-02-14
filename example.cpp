#include <cstdio>

#include "rma_buff.hpp"

#define mpi_assert(X) assert(X == MPI_SUCCESS)

int main(int argc, char **argv)
{
  int requested, provided;
  requested = MPI_THREAD_MULTIPLE;
  mpi_assert(MPI_Init_thread(&argc, &argv, requested, &provided));
  assert(requested == provided);
  {
    const int msg_len = 4, buff_len = 2;
    int rank;
    int *msg = new int [msg_len];
    int *msg_buff = new int [msg_len * buff_len];
    rma_buff<int> b(msg_len, buff_len, MPI_COMM_WORLD);

    mpi_assert(MPI_Comm_rank(MPI_COMM_WORLD, &rank));

    if (rank == 0) {
      for (int i = 0; i < msg_len; i++)
        msg[i] = i;

      for (int j = 0; j < buff_len + 1; j++) {
        if (b.put(1, msg))
          printf("%4.4i: put %i succeeded\n", rank, j);
        else
          printf("%4.4i: put %i failed\n", rank, j);

        for (int i = 0; i < msg_len; i++)
          msg[i] += msg_len;
      }
    }

    mpi_assert(MPI_Barrier(MPI_COMM_WORLD));

    int nf = b.get(msg_buff);
    printf("%4.4i: got %i messages in sync\n", rank, nf);
    if (nf > 0) {
      for (int j = 0; j < nf; j++) {
        printf("%4.4i: msg %i {%i", rank, j, msg_buff[j * msg_len]);
        for (int i = 1; i < msg_len; i++)
          printf(", %i", msg_buff[j * msg_len + i]);
        printf("}\n");
      }
    }
  }
  MPI_Finalize();
  return 0;
}
