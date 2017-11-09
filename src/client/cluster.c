/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include "mobject-store-config.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <margo.h>
#include <ssg.h>

#include "libmobject-store.h"
#include "src/client/cluster.h"
#include "src/io-chain/prepare-write-op.h"
#include "src/io-chain/prepare-read-op.h"
#include "src/rpc-types/write-op.h"
#include "src/rpc-types/read-op.h"
#include "src/util/log.h"


// global variables for RPC ids
hg_id_t mobject_write_op_rpc_id;
hg_id_t mobject_read_op_rpc_id;
hg_id_t mobject_shutdown_rpc_id;

static void mobject_store_register(margo_instance_id mid);
static int mobject_store_shutdown_servers(struct mobject_store_handle *cluster_handle);

int mobject_store_create(mobject_store_t *cluster, const char * const id)
{
    struct mobject_store_handle *cluster_handle;
    char *cluster_file;
    int ret;

    (void)id; /* XXX: id unused in mobject */

    /* allocate a new cluster handle and set some fields */
    cluster_handle = (struct mobject_store_handle*)calloc(1,sizeof(*cluster_handle));
    if (!cluster_handle)
        return -1;

    /* use env variable to determine how to connect to the cluster */
    /* NOTE: this is the _only_ method for specifying a cluster for now... */
    cluster_file = getenv(MOBJECT_CLUSTER_FILE_ENV);
    if (!cluster_file)
    {
        fprintf(stderr, "Error: %s env variable must point to mobject cluster file\n",
            MOBJECT_CLUSTER_FILE_ENV);
        free(cluster_handle);
        return -1;
    }

    ret = ssg_group_id_load(cluster_file, &cluster_handle->gid);
    if (ret != 0)
    {
        fprintf(stderr, "Error: Unable to load mobject cluster info from file %s\n",
            cluster_file);
        free(cluster_handle);
        return -1;
    }

    /* set the returned cluster handle */
    *cluster = cluster_handle;

    return 0;
}

int mobject_store_connect(mobject_store_t cluster)
{
    struct mobject_store_handle *cluster_handle;
    char *svr_addr_str;
    char proto[24] = {0};
    int i;
    int ret;

    cluster_handle = (struct mobject_store_handle *)cluster;
    assert(cluster_handle);

    if (cluster_handle->connected)
        return 0;

    /* figure out protocol to connect with using address information 
     * associated with the SSG group ID
     */
    svr_addr_str = ssg_group_id_get_addr_str(cluster_handle->gid);
    if (!svr_addr_str)
    {
        fprintf(stderr, "Error: Unable to obtain cluster group server address\n");
        ssg_group_id_free(cluster_handle->gid);
        free(cluster_handle);
        return -1;
    }
    /* we only need to get the proto portion of the address to initialize */
    for(i=0; i<24 && svr_addr_str[i] != '\0' && svr_addr_str[i] != ':'; i++)
        proto[i] = svr_addr_str[i];

    /* intialize margo */
    /* XXX: probably want to expose some way of tweaking threading parameters */
    cluster_handle->mid = margo_init(proto, MARGO_CLIENT_MODE, 0, -1);
    if (cluster_handle->mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: Unable to initialize margo\n");
        free(svr_addr_str);
        ssg_group_id_free(cluster_handle->gid);
        free(cluster_handle);
        return -1;
    }

    /* register mobject RPCs for this cluster */
    mobject_store_register(cluster_handle->mid);

    /* initialize ssg */
    /* XXX: we need to think about how to do this once per-client... clients could connect to mult. clusters */
    ret = ssg_init(cluster_handle->mid);
    if (ret != SSG_SUCCESS)
    {
        fprintf(stderr, "Error: Unable to initialize SSG\n");
        margo_finalize(cluster_handle->mid);
        free(svr_addr_str);
        ssg_group_id_free(cluster_handle->gid);
        free(cluster_handle);
        return -1;
    }

    /* attach to the cluster group */
    ret = ssg_group_attach(cluster_handle->gid);
    if (ret != SSG_SUCCESS)
    {
        fprintf(stderr, "Error: Unable to attach to the mobject cluster group\n");
        ssg_finalize();
        margo_finalize(cluster_handle->mid);
        free(svr_addr_str);
        ssg_group_id_free(cluster_handle->gid);
        free(cluster_handle);
        return -1;
    }
    cluster_handle->connected = 1;

    free(svr_addr_str);

    return 0;
}

void mobject_store_shutdown(mobject_store_t cluster)
{
    struct mobject_store_handle *cluster_handle;
    char *svr_kill_env_str;
    int ret;

    cluster_handle = (struct mobject_store_handle *)cluster;
    assert(cluster_handle != NULL);

    if (!cluster_handle->connected)
        return;

    svr_kill_env_str = getenv(MOBJECT_CLUSTER_SHUTDOWN_KILL_ENV);
    if (svr_kill_env_str && !strcmp(svr_kill_env_str, "true"))
    {
        /* kill server cluster if requested */
        ret = mobject_store_shutdown_servers(cluster_handle);
        if (ret != 0)
        {
            fprintf(stderr, "Warning: Unable to send shutdown signal \
                to mobject server cluster\n");
        }
    }

    ssg_group_detach(cluster_handle->gid);
    ssg_finalize();
    margo_finalize(cluster_handle->mid);
    ssg_group_id_free(cluster_handle->gid);
    free(cluster_handle);

    return;
}

int mobject_store_pool_create(mobject_store_t cluster, const char * pool_name)
{
    /* XXX: this is a NOOP -- we don't implement pools currently */
    (void)cluster;
    (void)pool_name;
    return 0;
}

int mobject_store_pool_delete(mobject_store_t cluster, const char * pool_name)
{
    /* XXX: this is a NOOP -- we don't implement pools currently */
    (void)cluster;
    (void)pool_name;
    return 0;
}

int mobject_store_ioctx_create(
    mobject_store_t cluster,
    const char * pool_name,
    mobject_store_ioctx_t *ioctx)
{
    *ioctx = (mobject_store_ioctx_t)calloc(1, sizeof(**ioctx));
    (*ioctx)->pool_name = strdup(pool_name);
    (*ioctx)->cluster   = cluster;
    return 0;
}

void mobject_store_ioctx_destroy(mobject_store_ioctx_t ioctx)
{
    if(ioctx) free(ioctx->pool_name);
    free(ioctx);
}

int mobject_store_write_op_operate(mobject_store_write_op_t write_op,
                                   mobject_store_ioctx_t io,
                                   const char *oid,
                                   time_t *mtime,
                                   int flags)
{
    hg_return_t ret;

    write_op_in_t in;
    in.object_name = oid;
    in.pool_name   = io->pool_name;
    in.write_op    = write_op;
    // TODO take mtime into account

    prepare_write_op(io->cluster->mid, write_op);

    hg_addr_t svr_addr = ssg_get_addr(io->cluster->gid, 0); // XXX pick other servers using ch-placement
    MOBJECT_ASSERT(svr_addr != HG_ADDR_NULL, "NULL server address");

    hg_handle_t h;
    ret = margo_create(io->cluster->mid, svr_addr, mobject_write_op_rpc_id, &h);
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not create RPC handle");

    ret = margo_forward(h, &in);
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not forward RPC");

    write_op_out_t resp;
    ret = margo_get_output(h, &resp); 
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not get RPC output");

    ret = margo_free_output(h,&resp); 
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not free RPC output");

    ret = margo_destroy(h);
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not destroy RPC handle");
    return 0;
}

int mobject_store_read_op_operate(mobject_store_read_op_t read_op,
                                  mobject_store_ioctx_t ioctx,
                                  const char *oid,
                                  int flags)
{
    hg_return_t ret;

    read_op_in_t in; 
    in.object_name = oid;
    in.pool_name   = ioctx->pool_name;
    in.read_op     = read_op;

    prepare_read_op(ioctx->cluster->mid, read_op);

    hg_addr_t svr_addr = ssg_get_addr(ioctx->cluster->gid, 0); // XXX pick other servers using ch-placement
    MOBJECT_ASSERT(svr_addr != HG_ADDR_NULL, "NULL server address");

    hg_handle_t h;
    ret = margo_create(ioctx->cluster->mid, svr_addr, mobject_read_op_rpc_id, &h);
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not create RPC handle");
    ret = margo_forward(h, &in);
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not forward RPC");

    read_op_out_t resp; 
    ret = margo_get_output(h, &resp); 
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not get RPC output");

    feed_read_op_pointers_from_response(read_op, resp.responses);

    ret = margo_free_output(h,&resp); 
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not free RPC output");
    ret = margo_destroy(h);
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not destroy RPC handle");

    return 0;
}

/* internal helper routines */

// register mobject RPCs
static void mobject_store_register(margo_instance_id mid)
{
    /* XXX i think ultimately these need to be stored in per-mid containers instead of global... */
    mobject_write_op_rpc_id = 
    MARGO_REGISTER(mid, "mobject_write_op", write_op_in_t, write_op_out_t, NULL);
    mobject_read_op_rpc_id = 
    MARGO_REGISTER(mid, "mobject_read_op",  read_op_in_t,  read_op_out_t, NULL);
    mobject_shutdown_rpc_id =
    MARGO_REGISTER(mid, "mobject_shutdown", void, void, NULL);
}

// send a shutdown signal to a server cluster
static int mobject_store_shutdown_servers(struct mobject_store_handle *cluster_handle)
{
    hg_addr_t svr_addr;
    hg_handle_t h;
    hg_return_t ret;

    /* get the address of the first server */
    svr_addr = ssg_get_addr(cluster_handle->gid, 0);
    if (svr_addr == HG_ADDR_NULL)
    {
        fprintf(stderr, "Error: Unable to obtain address for mobject server\n");
        return -1;
    }

    ret = margo_create(cluster_handle->mid, svr_addr, mobject_shutdown_rpc_id, &h);
    if (ret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: Unable to create margo handle\n");
        return -1;
    }

    /* send shutdown signal */
    ret = margo_forward(h, NULL);
    if (ret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: Unable to forward margo handle\n");
        margo_destroy(h);
        return -1;
    }

    margo_destroy(h);

    return 0;
}

