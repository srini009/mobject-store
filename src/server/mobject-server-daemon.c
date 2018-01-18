/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <mpi.h>
#include <margo.h>
#include <ssg.h>
#include <bake-client.h>
#include <bake-server.h>

#include "mobject-server.h"

void usage(void)
{
    fprintf(stderr, "Usage: mobject-server-daemon <listen_addr> <cluster_file>\n");
    fprintf(stderr, "  <listen_addr>    the Mercury address to listen on\n");
    fprintf(stderr, "  <cluster_file>   the file to write mobject cluster connect info to\n");
    exit(-1);
}

typedef struct {
    bake_client_t          client;
    bake_provider_handle_t provider_handle;
} bake_client_data;

static void finalize_bake_client_cb(void* data);

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

    /* Margo initialization */
    mid = margo_init(listen_addr, MARGO_SERVER_MODE, 0, -1);
    if (mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: Unable to initialize margo\n");
        return -1;
    }

    /* SSG initialization */
    ret = ssg_init(mid);
    if(ret != 0) {
        fprintf(stderr, "Error: Unable to initialize SSG\n");
        margo_finalize(mid);
        return -1;
    }

    /* Get self address */
    hg_addr_t self_addr;
    margo_addr_self(mid, &self_addr);

    /* Bake provider initialization */
    /* XXX mplex id and target name should be taken from config file */
    uint8_t bake_mplex_id = 1;
    const char* bake_target_name = "/dev/shm/mobject.dat";
    bake_provider_t bake_prov;
    bake_target_id_t bake_tid;
    bake_provider_register(mid, bake_mplex_id, BAKE_ABT_POOL_DEFAULT, &bake_prov);
    bake_provider_add_storage_target(bake_prov, bake_target_name, &bake_tid);
    /* TODO check return value of above calls */

    /* Bake provider handle initialization from self addr */
    bake_client_data bake_clt_data;
    bake_client_init(mid, &(bake_clt_data.client));
    bake_provider_handle_create(bake_clt_data.client, self_addr, bake_mplex_id, &(bake_clt_data.provider_handle));
    margo_push_finalize_callback(mid, &finalize_bake_client_cb, (void*)&bake_clt_data);

    /* Mobject provider initialization */
    mobject_provider_t mobject_prov;
    ret = mobject_provider_register(mid, 1, MOBJECT_ABT_POOL_DEFAULT, 
            bake_clt_data.provider_handle, cluster_file, &mobject_prov);
    if (ret != 0)
    {
        fprintf(stderr, "Error: Unable to initialize mobject provider\n");
        margo_finalize(mid);
        return -1;
    }

    margo_wait_for_finalize(mid);

    MPI_Finalize();

    return 0;
}

static void finalize_bake_client_cb(void* data)
{
    bake_client_data* clt_data = (bake_client_data*)data;
    bake_provider_handle_release(clt_data->provider_handle);
    bake_client_finalize(clt_data->client);
}
