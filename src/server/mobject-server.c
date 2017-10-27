/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <assert.h>
#include <mpi.h>
#include <abt.h>
#include <margo.h>
//#include <sds-keyval.h>
//#include <bake-bulk-server.h>
//#include <libpmemobj.h>
#include <ssg-mpi.h>

#include "mobject-server.h"
#include "src/rpc-types/write-op.h"
#include "src/rpc-types/read-op.h"
#include "src/server/exec-write-op.h"
#include "src/server/exec-read-op.h"

typedef struct mobject_server_context
{
    /* margo, bake, sds-keyval, ssg state */
    margo_instance_id mid;
    /* TODO bake, sds-keyval stuff */
    ssg_group_id_t gid;

    /* server shutdown conditional logic */
    ABT_mutex shutdown_mutex;
    ABT_cond shutdown_cond;
    int shutdown_flag;
    int ref_count;
} mobject_server_context_t;

static int mobject_server_register(mobject_server_context_t *srv_ctx);
static void mobject_server_cleanup(mobject_server_context_t *srv_ctx);

DECLARE_MARGO_RPC_HANDLER(mobject_write_op_ult)
DECLARE_MARGO_RPC_HANDLER(mobject_read_op_ult)
DECLARE_MARGO_RPC_HANDLER(mobject_shutdown_ult)

/* mobject RPC IDs */
static hg_id_t mobject_write_op_rpc_id;
static hg_id_t mobject_read_op_rpc_id;
static hg_id_t mobject_shutdown_rpc_id;

/* XXX one global mobject server state struct */
static mobject_server_context_t *g_srv_ctx = NULL;


int mobject_server_init(margo_instance_id mid, const char *cluster_file)
{
    mobject_server_context_t *srv_ctx;
    int my_id;
    int ret;

    if (g_srv_ctx)
    {
        fprintf(stderr, "Error: mobject server has already been initialized\n");
        return -1;
    }

    srv_ctx = calloc(1, sizeof(*srv_ctx));
    if (!srv_ctx)
        return -1;
    srv_ctx->mid = mid;
    srv_ctx->ref_count = 1;
    ABT_mutex_create(&srv_ctx->shutdown_mutex);
    ABT_cond_create(&srv_ctx->shutdown_cond);

    /* TODO bake-bulk */
    /* TODO sds-keyval */
# if 0
    kv_context *metadata;
    struct bake_pool_info *pool_info;
    pool_info = bake_server_makepool(poolname);
#endif

    ret = ssg_init(mid);
    if (ret != SSG_SUCCESS)
    {
        free(srv_ctx);
        fprintf(stderr, "Error: Unable to initialize SSG\n");
        return -1;
    }

    /* server group create */
    srv_ctx->gid = ssg_group_create_mpi(MOBJECT_SERVER_GROUP_NAME, MPI_COMM_WORLD,
        NULL, NULL); /* XXX membership update callbacks unused currently */
    if (srv_ctx->gid == SSG_GROUP_ID_NULL)
    {
        fprintf(stderr, "Error: Unable to create the mobject server group\n");
        ssg_finalize();
        free(srv_ctx);
        return -1;
    }
    my_id = ssg_get_group_self_id(srv_ctx->gid);

    /* register mobject & friends RPC handlers */
    mobject_server_register(srv_ctx);

    /* one proccess writes cluster connect info to file for clients to find later */
    if (my_id == 0)
    {
        ret = ssg_group_id_store(cluster_file, srv_ctx->gid);
        if (ret != 0)
        {
            fprintf(stderr, "Error: unable to store mobject cluster info to file %s\n",
                cluster_file);
            /* XXX: this call is performed by one process, and we do not currently
             * have an easy way to propagate this error to the entire cluster group
             */
            ssg_group_destroy(srv_ctx->gid);
            ssg_finalize();
            free(srv_ctx);
            return -1;
        }
    }

    g_srv_ctx = srv_ctx;

    return 0;
}

void mobject_server_shutdown(margo_instance_id mid)
{
    mobject_server_context_t *srv_ctx = g_srv_ctx;
    int do_cleanup;

    assert(srv_ctx);

    ABT_mutex_lock(srv_ctx->shutdown_mutex);
    srv_ctx->shutdown_flag = 1;
    ABT_cond_broadcast(srv_ctx->shutdown_cond);

    srv_ctx->ref_count--;
    do_cleanup = srv_ctx->ref_count == 0;

    ABT_mutex_unlock(srv_ctx->shutdown_mutex);

    if (do_cleanup)
        mobject_server_cleanup(srv_ctx);

    return;
}

void mobject_server_wait_for_shutdown()
{
    mobject_server_context_t *srv_ctx = g_srv_ctx;
    int do_cleanup;

    assert(srv_ctx);

    ABT_mutex_lock(srv_ctx->shutdown_mutex);

    srv_ctx->ref_count++;
    while(!srv_ctx->shutdown_flag)
        ABT_cond_wait(srv_ctx->shutdown_cond, srv_ctx->shutdown_mutex);
    srv_ctx->ref_count--;
    do_cleanup = srv_ctx->ref_count == 0;

    ABT_mutex_unlock(srv_ctx->shutdown_mutex);

    if (do_cleanup)
        mobject_server_cleanup(srv_ctx);

    return;
}

static int mobject_server_register(mobject_server_context_t *srv_ctx)
{
    int ret=0;
    margo_instance_id mid = srv_ctx->mid;

    mobject_write_op_rpc_id = MARGO_REGISTER(mid, "mobject_write_op", 
	write_op_in_t, write_op_out_t, mobject_write_op_ult);

    mobject_read_op_rpc_id  = MARGO_REGISTER(mid, "mobject_read_op",
        read_op_in_t, read_op_out_t, mobject_read_op_ult)

    mobject_shutdown_rpc_id = MARGO_REGISTER(mid, "mobject_shutdown",
        void, void, mobject_shutdown_ult);

#if 0
    bake_server_register(mid, pool_info);
    metadata = kv_server_register(mid);
#endif

    return ret;
}

static hg_return_t mobject_write_op_ult(hg_handle_t h)
{
    hg_return_t ret;

    write_op_in_t in;
    write_op_out_t out;

    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* Deserialize the input from the received handle. */
    ret = margo_get_input(h, &in);
    assert(ret == HG_SUCCESS);

    /* Execute the operation chain */
    execute_write_op(in.write_op, in.object_name);

    // set the return value of the RPC
    out.ret = 0;

    ret = margo_respond(h, &out);
    assert(ret == HG_SUCCESS);

    /* Free the input data. */
    ret = margo_free_input(h, &in);
    assert(ret == HG_SUCCESS);

    /* We are not going to use the handle anymore, so we should destroy it. */
    ret = margo_destroy(h);
 
    return ret;
}
DEFINE_MARGO_RPC_HANDLER(mobject_write_op_ult)

/* Implementation of the RPC. */
static hg_return_t mobject_read_op_ult(hg_handle_t h)
{
    hg_return_t ret;

    read_op_in_t in;
    read_op_out_t out;

    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* Deserialize the input from the received handle. */
    ret = margo_get_input(h, &in);
    assert(ret == HG_SUCCESS);

    /* Create a response list matching the input actions */
    read_response_t resp = build_matching_read_responses(in.read_op);

    /* Compute the result. */
    execute_read_op(in.read_op, in.object_name);

    out.responses = resp;

    ret = margo_respond(h, &out);
    assert(ret == HG_SUCCESS);

    free_read_responses(resp);

    /* Free the input data. */
    ret = margo_free_input(h, &in);
    assert(ret == HG_SUCCESS);

    /* We are not going to use the handle anymore, so we should destroy it. */
    ret = margo_destroy(h);
    assert(ret == HG_SUCCESS);

    return ret;
}
DEFINE_MARGO_RPC_HANDLER(mobject_read_op_ult)

static void mobject_shutdown_ult(hg_handle_t h)
{
    hg_return_t ret;

    margo_instance_id mid = margo_hg_handle_get_instance(h);

    printf("Got shutdown signal!\n");

    ret = margo_respond(h, NULL);
    assert(ret == HG_SUCCESS);

    ret = margo_destroy(h);
    assert(ret == HG_SUCCESS);

    mobject_server_shutdown(mid);

    return;
}
DEFINE_MARGO_RPC_HANDLER(mobject_shutdown_ult)

static void mobject_server_cleanup(mobject_server_context_t *srv_ctx)
{
    ssg_group_destroy(srv_ctx->gid);
    ssg_finalize();

    //pmemobj_close(NULL);

    ABT_mutex_free(&srv_ctx->shutdown_mutex);
    ABT_cond_free(&srv_ctx->shutdown_cond);
    free(srv_ctx);

    return;
}