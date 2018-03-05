/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <unistd.h>
#include <mpi.h>
#include <margo.h>
#include <ssg.h>
#include <ssg-mpi.h>
#include <bake-client.h>
#include <bake-server.h>
#include <sdskv-client.h>
#include <sdskv-server.h>

#include "mobject-server.h"
#include "src/server/core/key-types.h"

#define ASSERT(__cond, __msg, ...) { if(!(__cond)) { fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, __VA_ARGS__); exit(-1); } }

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

typedef struct {
    sdskv_client_t         client;
    sdskv_provider_handle_t provider_handle;
} sdskv_client_data;

static void finalize_ssg_cb(void* data);
static void finalize_bake_client_cb(void* data);
static void finalize_sdskv_client_cb(void* data);

/* comparison functions for SDSKV */
static int oid_map_compare(const void*, size_t, const void*, size_t);
static int name_map_compare(const void*, size_t, const void*, size_t);
static int seg_map_compare(const void*, size_t, const void*, size_t);
static int omap_map_compare(const void*, size_t, const void*, size_t);

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

    /* MPI required for SSG bootstrapping */
    MPI_Init(&argc, &argv);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Margo initialization */
    mid = margo_init(listen_addr, MARGO_SERVER_MODE, 0, -1);
    if (mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: Unable to initialize margo\n");
        return -1;
    }
    margo_enable_remote_shutdown(mid);

    /* SSG initialization */
    ret = ssg_init(mid);
    ASSERT(ret == 0, "ssg_init() failed (ret = %d)\n", ret);

#if 0
    // XXX group name and file should be defined in a config file
    ssg_group_id_t gid = ssg_group_create_mpi("mobject-providers", MPI_COMM_WORLD, NULL, NULL);
    ASSERT(gid != SSG_GROUP_ID_NULL, "%s", "ssg_group_create_mpi() failed");
    ret = ssg_group_id_store("mobject.ssg", gid);
    ASSERT(ret == 0, "ssg_group_id_store() failed (ret = %d)\n", ret);
    margo_push_finalize_callback(mid, &finalize_ssg_cb, (void*)&gid);

    /* Get self address */
    ssg_member_id_t self_id = ssg_get_group_self_id(gid);
    hg_addr_t self_addr = ssg_get_addr(gid, self_id);
#endif
    hg_addr_t self_addr;
    margo_addr_self(mid, &self_addr);

    /* Bake provider initialization */
    /* XXX mplex id and target name should be taken from config file */
    uint8_t bake_mplex_id = 1;
    char bake_target_name[128];
    sprintf(bake_target_name, "/dev/shm/mobject.%d.dat", rank);
    /* create the bake target if it does not exist */
    if(-1 == access(bake_target_name, F_OK)) {
        // XXX creating a pool of 10MB - this should come from a config file
        ret = bake_makepool(bake_target_name, 10*1024*1024, 0664);
        ASSERT(ret == 0, "bake_makepool() failed (ret = %d)\n", ret);
    }
    bake_provider_t bake_prov;
    bake_target_id_t bake_tid;
    ret = bake_provider_register(mid, bake_mplex_id, BAKE_ABT_POOL_DEFAULT, &bake_prov);
    ASSERT(ret == 0, "bake_provider_register() failed (ret = %d)\n", ret);
    ret = bake_provider_add_storage_target(bake_prov, bake_target_name, &bake_tid);
    ASSERT(ret == 0, "bake_provider_add_storage_target() failed to add target %s (ret = %d)\n",
            bake_target_name, ret);

    /* Bake provider handle initialization from self addr */
    bake_client_data bake_clt_data;
    ret = bake_client_init(mid, &(bake_clt_data.client));
    ASSERT(ret == 0, "bake_client_init() failed (ret = %d)\n", ret);
    ret = bake_provider_handle_create(bake_clt_data.client, self_addr, bake_mplex_id, &(bake_clt_data.provider_handle));
    ASSERT(ret == 0, "bake_provider_handle_create() failed (ret = %d)\n", ret);
    margo_push_finalize_callback(mid, &finalize_bake_client_cb, (void*)&bake_clt_data);

    /* SDSKV provider initialization */
    uint8_t sdskv_mplex_id = 1;
    sdskv_provider_t sdskv_prov;
    sdskv_database_id_t oid_map_id, name_map_id, seg_map_id, omap_map_id;
    ret = sdskv_provider_register(mid, sdskv_mplex_id, SDSKV_ABT_POOL_DEFAULT, &sdskv_prov);
    ASSERT(ret == 0, "sdskv_provider_register() failed (ret = %d)\n", ret);

    ret = sdskv_provider_add_database(sdskv_prov, "oid_map",  KVDB_MAP, &oid_map_compare,  &oid_map_id);
    ASSERT(ret == 0, "sdskv_provider_add_database() failed to add database \"oid_map\" (ret = %d)\n", ret);
    ret = sdskv_provider_add_database(sdskv_prov, "name_map", KVDB_MAP, &name_map_compare, &name_map_id);
    ASSERT(ret == 0, "sdskv_provider_add_database() failed to add database \"name_map\" (ret = %d)\n", ret);
    ret = sdskv_provider_add_database(sdskv_prov, "seg_map",  KVDB_MAP, &seg_map_compare,  &seg_map_id);
    ASSERT(ret == 0, "sdskv_provider_add_database() failed to add database \"seg_map\" (ret = %d)\n", ret);
    ret = sdskv_provider_add_database(sdskv_prov, "omap_map", KVDB_MAP, &omap_map_compare, &omap_map_id);
    ASSERT(ret == 0, "sdskv_provider_add_database() failed to add database \"omap_map\" (ret = %d)\n", ret);

    /* SDSKV provider handle initialization from self addr */
    sdskv_client_data sdskv_clt_data;
    ret = sdskv_client_init(mid, &(sdskv_clt_data.client));
    ASSERT(ret == 0, "sdskv_client_init() failed (ret = %d)\n", ret);
    ret = sdskv_provider_handle_create(sdskv_clt_data.client, self_addr, sdskv_mplex_id, &(sdskv_clt_data.provider_handle));
    ASSERT(ret == 0, "sdskv_provider_handle_create() failed (ret = %d)\n", ret);
    margo_push_finalize_callback(mid, &finalize_sdskv_client_cb, (void*)&sdskv_clt_data);

    /* Mobject provider initialization */
    mobject_provider_t mobject_prov;
    ret = mobject_provider_register(mid, 1, 
            MOBJECT_ABT_POOL_DEFAULT, 
            bake_clt_data.provider_handle, 
            sdskv_clt_data.provider_handle, 
            cluster_file, &mobject_prov);
    if (ret != 0)
    {
        fprintf(stderr, "Error: Unable to initialize mobject provider\n");
        margo_finalize(mid);
        return -1;
    }

    margo_addr_free(mid, self_addr);

    margo_wait_for_finalize(mid);

    MPI_Finalize();

    return 0;
}

static void finalize_ssg_cb(void* data)
{
    ssg_group_id_t* gid = (ssg_group_id_t*)data;
    ssg_group_destroy(*gid);
}

static void finalize_bake_client_cb(void* data)
{
    bake_client_data* clt_data = (bake_client_data*)data;
    bake_provider_handle_release(clt_data->provider_handle);
    bake_client_finalize(clt_data->client);
}

static void finalize_sdskv_client_cb(void* data)
{
    sdskv_client_data* clt_data = (sdskv_client_data*)data;
    sdskv_provider_handle_release(clt_data->provider_handle);
    sdskv_client_finalize(clt_data->client);
}

static int oid_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    oid_t x = *((oid_t*)k1);
    oid_t y = *((oid_t*)k2);
    if(x == y) return 0;
    if(x < y) return -1;
    return 1;
}

static int name_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    const char* n1 = (const char*)k1;
    const char* n2 = (const char*)k2;
    return strcmp(n1,n2);
}

static int seg_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    const segment_key_t* seg1 = (const segment_key_t*)k1;
    const segment_key_t* seg2 = (const segment_key_t*)k2;
    if(seg1->oid < seg2->oid) return -1;
    if(seg1->oid > seg2->oid) return 1;
    if(seg1->timestamp > seg2->timestamp) return -1;
    if(seg1->timestamp < seg2->timestamp) return 1;
    return 0;
}

static int omap_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    const omap_key_t* ok1 = (const omap_key_t*)k1;
    const omap_key_t* ok2 = (const omap_key_t*)k2;
    if(ok1->oid < ok2->oid) return -1;
    if(ok1->oid > ok2->oid) return 1;
    return strcmp(ok1->key, ok2->key);
}
