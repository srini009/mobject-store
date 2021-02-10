/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <unistd.h>
#include <getopt.h>
#include <mpi.h>
#include <margo.h>
#include <ssg.h>
#include <ssg-mpi.h>
#include <bake-client.h>
#include <bake-server.h>
#include <sdskv-client.h>
#include <sdskv-server.h>
#include <symbiomon/symbiomon-server.h>

#include "mobject-server.h"

#define ASSERT(__cond, __msg, ...) { if(!(__cond)) { fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, __VA_ARGS__); exit(-1); } }

typedef struct {
    bake_client_t          client;
    bake_provider_handle_t provider_handle;
} bake_client_data;

typedef struct {
    sdskv_client_t         client;
    sdskv_provider_handle_t provider_handle;
} sdskv_client_data;

typedef struct {
    char*           listen_addr;
    char*           cluster_file;
    int             handler_xstreams;
    char *          pool_file;
    size_t          pool_size;
    char *          kv_path;
    sdskv_db_type_t kv_backend;
    int             disable_pipelining;
} mobject_server_options;

static void usage(void)
{
    fprintf(stderr, "Usage: mobject-server-daemon [OPTIONS] <listen_addr> <cluster_file>\n");
    fprintf(stderr, "  <listen_addr>            the Mercury address to listen on\n");
    fprintf(stderr, "  <cluster_file>           the file to write mobject cluster connect info to\n");
    fprintf(stderr, "  OPTIONS:\n");
    fprintf(stderr, "    --handler-xstreams     Number of xtreams to user for RPC handlers [default: 4]\n"); 
    fprintf(stderr, "    --pool-file            Bake pool location [default: /dev/shm/mobject.dat]\n");
    fprintf(stderr, "    --pool-size            Bake pool size for each server [default: 1GiB]\n");
    fprintf(stderr, "    --kv-backend           SDSKV backend to use (mapdb, leveldb, berkeleydb) [default: stdmap]\n");
    fprintf(stderr, "    --kv-path              SDSKV storage location [default: /dev/shm]\n");
    fprintf(stderr, "    --disable-pipelining   Disable use of Bake pipelining\n");
    exit(-1);
}

static void parse_args(int argc, char **argv, mobject_server_options *opts)
{
    int c;
    char *short_options = "x:f:s:p:k:d";
    struct option long_options[] = {
        {"handler-xstreams", required_argument, 0, 'x'},
        {"pool-file", required_argument, 0, 'f'},
        {"pool-size", required_argument, 0, 's'},
        {"kv-path", required_argument, 0, 'p'},
        {"kv-backend", required_argument, 0, 'k'},
        {"disable-pipelining", no_argument, 0, 'd'},
    };

    while ((c = getopt_long(argc, argv, short_options, long_options, NULL)) != -1)
    {
        switch (c)
        {
            case 'x':
                opts->handler_xstreams = atoi(optarg);
                break;
            case 'f':
                opts->pool_file = optarg;
                break;
            case 's':
                opts->pool_size = strtoul(optarg, NULL, 0);
                break;
            case 'p':
                opts->kv_path = optarg;
                break;
            case 'k':
                if(strcmp(optarg, "mapdb") == 0)
                    opts->kv_backend = KVDB_MAP;
                else if(strcmp(optarg, "leveldb") == 0)
                    opts->kv_backend = KVDB_LEVELDB;
                else if(strcmp(optarg, "berkeleydb") == 0)
                    opts->kv_backend = KVDB_BERKELEYDB;
                else
                    usage();
                break;
            case 'd':
                opts->disable_pipelining = 1;
                break;
            default:
                usage();
        }
    }

    if ((argc - optind) != 2)
        usage();
    opts->listen_addr = argv[optind++];
    opts->cluster_file = argv[optind++];

    return;
}

static void finalize_ssg_cb(void* data);
static void finalize_bake(void* data);
static void finalize_sdskv(void* data);
static void finalize_bake_client_cb(void* data);
static void finalize_sdskv_client_cb(void* data);

int main(int argc, char *argv[])
{
    mobject_server_options server_opts = {
        .handler_xstreams = 4, /* default to 4 rpc handler xstreams */
        .pool_file = "/dev/shm/mobject.dat", /* default bake pool file */
        .pool_size = 1*1024*1024*1024, /* 1 GiB default */
        .kv_path = "/dev/shm", /* default sdskv path */
        .kv_backend = KVDB_MAP, /* in-memory map default */
        .disable_pipelining = 0, /* use pipelining by default */
    }; 
    margo_instance_id mid;
    ssg_group_config_t group_config = SSG_GROUP_CONFIG_INITIALIZER;
    int ret;

    parse_args(argc, argv, &server_opts);

    /* MPI required for SSG bootstrapping */
    MPI_Init(&argc, &argv);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Margo initialization */
    mid = margo_init(server_opts.listen_addr, MARGO_SERVER_MODE, 0, 
        server_opts.handler_xstreams);
    if (mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: Unable to initialize margo\n");
        return -1;
    }
    margo_enable_remote_shutdown(mid);

    /* SSG initialization */
    ret = ssg_init();
    ASSERT(ret == 0, "ssg_init() failed (ret = %d)\n", ret);

    hg_addr_t self_addr;
    margo_addr_self(mid, &self_addr);
    /* initialize SYMBIOMON */
    struct symbiomon_provider_args args = SYMBIOMON_PROVIDER_ARGS_INIT;
    args.push_finalize_callback = 0;

    symbiomon_provider_t metric_provider;
    ret = symbiomon_provider_register(mid, 42, &args, &metric_provider);
    if(ret != 0)
        fprintf(stderr, "Error: symbiomon_provider_register() failed. Continuing on.\n");

    /* Bake provider initialization */
    /* XXX mplex id and target name should be taken from config file */
    uint8_t bake_mplex_id = 1;
    /* create the bake target if it does not exist */
    if(-1 == access(server_opts.pool_file, F_OK)) {
        // XXX creating a pool of 10MB - this should come from a config file
        ret = bake_makepool(server_opts.pool_file, server_opts.pool_size, 0664);
        if (ret != 0) bake_perror("bake_makepool", ret);
        ASSERT(ret == 0, "bake_makepool() failed (ret = %d)\n", ret);
    }
    bake_provider_t bake_prov;
    bake_target_id_t bake_tid;
    ret = bake_provider_register(mid, bake_mplex_id, BAKE_ABT_POOL_DEFAULT, &bake_prov);
    if (ret != 0) bake_perror("bake_provider_register", ret);
    ASSERT(ret == 0, "bake_provider_register() failed (ret = %d)\n", ret);
    ret = bake_provider_set_symbiomon(bake_prov, metric_provider);
    if(ret != 0)
        fprintf(stderr, "Error: bake_provider_set_symbiomon() failed. Contuinuing on.\n");
    ret = bake_provider_add_storage_target(bake_prov, server_opts.pool_file, &bake_tid);
    if (ret != 0) bake_perror("bake_provider_add_storage_target", ret);
    ASSERT(ret == 0, "bake_provider_add_storage_target() failed to add target %s (ret = %d)\n",
            server_opts.pool_file, ret);
    if (!server_opts.disable_pipelining)
        bake_provider_set_conf(bake_prov, "pipeline_enabled", "1");

    /* Bake provider handle initialization from self addr */
    bake_client_data bake_clt_data;
    ret = bake_client_init(mid, &(bake_clt_data.client));
    if (ret != 0) bake_perror("bake_client_init", ret);
    ASSERT(ret == 0, "bake_client_init() failed (ret = %d)\n", ret);
    ret = bake_provider_handle_create(bake_clt_data.client, self_addr, bake_mplex_id, &(bake_clt_data.provider_handle));
    if (ret != 0) bake_perror("bake_provider_handle_create", ret);
    ASSERT(ret == 0, "bake_provider_handle_create() failed (ret = %d)\n", ret);
    margo_push_finalize_callback(mid, &finalize_bake_client_cb, (void*)&bake_clt_data);

    /* SDSKV provider initialization */
    uint8_t sdskv_mplex_id = 2;
    sdskv_provider_t sdskv_prov;
    ret = sdskv_provider_register(mid, sdskv_mplex_id, SDSKV_ABT_POOL_DEFAULT, &sdskv_prov);
    ASSERT(ret == 0, "sdskv_provider_register() failed (ret = %d)\n", ret);

    ret = sdskv_provider_set_symbiomon(sdskv_prov, metric_provider);
    if(ret != 0)
        fprintf(stderr, "Error: sdskv_provider_set_symbiomon() failed. Contuinuing on.\n");

    ret = mobject_sdskv_provider_setup(sdskv_prov, server_opts.kv_path, server_opts.kv_backend);

    /* SDSKV provider handle initialization from self addr */
    sdskv_client_data sdskv_clt_data;
    ret = sdskv_client_init(mid, &(sdskv_clt_data.client));
    ASSERT(ret == 0, "sdskv_client_init() failed (ret = %d)\n", ret);
    ret = sdskv_provider_handle_create(sdskv_clt_data.client, self_addr, sdskv_mplex_id, &(sdskv_clt_data.provider_handle));
    ASSERT(ret == 0, "sdskv_provider_handle_create() failed (ret = %d)\n", ret);
    margo_push_finalize_callback(mid, &finalize_sdskv_client_cb, (void*)&sdskv_clt_data);

    /* SSG group creation */
    group_config.swim_period_length_ms = 10000; /* 10-second period length ... */
    ssg_group_id_t gid = ssg_group_create_mpi(mid, MOBJECT_SERVER_GROUP_NAME, MPI_COMM_WORLD, &group_config, NULL, NULL);
    ASSERT(gid != SSG_GROUP_ID_INVALID, "ssg_group_create_mpi() failed (ret = %s)","SSG_GROUP_ID_NULL");
    margo_push_prefinalize_callback(mid, &finalize_ssg_cb, (void*)&gid);

    /* Mobject provider initialization */
    mobject_provider_t mobject_prov;
    ret = mobject_provider_register(mid, 1, 
            MOBJECT_ABT_POOL_DEFAULT, 
            bake_clt_data.provider_handle, 
            sdskv_clt_data.provider_handle,
            gid, server_opts.cluster_file, &mobject_prov);
    if (ret != 0)
    {
        fprintf(stderr, "Error: Unable to initialize mobject provider\n");
        margo_finalize(mid);
        return -1;
    }

    margo_addr_free(mid, self_addr);
    margo_push_prefinalize_callback(mid, &finalize_sdskv, (void*)&sdskv_prov);
    margo_push_prefinalize_callback(mid, &finalize_bake, (void*)&bake_prov);




    margo_wait_for_finalize(mid);

    MPI_Finalize();

    return 0;
}

static void finalize_sdskv(void *data)
{

  sdskv_provider_t * p = (sdskv_provider_t *)data;
  sdskv_provider_finalize(*p);
}

static void finalize_bake(void *data)
{

  bake_provider_t * p = (bake_provider_t *)data;
  bake_provider_finalize(*p);
}

static void finalize_ssg_cb(void* data)
{
    ssg_group_id_t* gid = (ssg_group_id_t*)data;
    ssg_group_destroy(*gid);
    ssg_finalize();
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
