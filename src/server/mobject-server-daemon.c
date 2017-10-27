/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <mpi.h>
#include <margo.h>
#include <ssg.h>

#include "mobject-server.h"

void usage(void)
{
    fprintf(stderr, "Usage: mobject-server-daemon <listen_addr> <cluster_file>\n");
    fprintf(stderr, "  <listen_addr>    the Mercury address to listen on\n");
    fprintf(stderr, "  <cluster_file>   the file to write mobject cluster connect info to\n");
    exit(-1);
}

int main(int argc, char *argv[])
{
    char *listen_addr;
    char *cluster_file;
    margo_instance_id mid;
    int ret;

    /* check args */
    if (argc != 3)
        usage();
    listen_addr = argv[1];
    cluster_file = argv[2];

    /* XXX: MPI required for SSG bootstrapping */
    MPI_Init(&argc, &argv);

    mid = margo_init(listen_addr, MARGO_SERVER_MODE, 0, -1);
    if (mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: Unable to initialize margo\n");
        return -1;
    }

    ret = mobject_server_init(mid, cluster_file);
    if (ret != 0)
    {
        fprintf(stderr, "Error: Unable to initialize mobject server\n");
        margo_finalize(mid);
        return -1;
    }

    /* wait for shutdown signal, then clean up */
    mobject_server_wait_for_shutdown();
    margo_finalize(mid);
    MPI_Finalize();

    return 0;
}
