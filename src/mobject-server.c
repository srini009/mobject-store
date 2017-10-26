/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <assert.h>
#include <mpi.h>
#include <margo.h>
//#include <sds-keyval.h>
//#include <bake-bulk-server.h>
//#include <libpmemobj.h>
#include <ssg-mpi.h>

#include "mobject-server.h"


typedef struct mobject_server_context
{
    /* XXX bake, sds-keyval stuff */
    ssg_group_id_t gid;
} mobject_server_context_t;

static int mobject_server_store_cluster_info(const char *file_name);

/* XXX one global mobject server state struct */
mobject_server_context_t *g_srv_ctx = NULL;

int mobject_server_init(margo_instance_id mid, const char *cluster_file)
{
    int ret;

    if (g_srv_ctx)
    {
        fprintf(stderr, "Error: mobject server has already been initialized\n");
        return -1;
    }

    g_srv_ctx = malloc(sizeof(*g_srv_ctx));
    if (!g_srv_ctx)
        return -1;
    memset(g_srv_ctx, 0, sizeof(*g_srv_ctx));

    /* TODO bake-bulk */
    /* TODO sds-keyval */

    ret = ssg_init(mid);
    if (ret != SSG_SUCCESS)
    {
        fprintf(stderr, "Error: Unable to initialize SSG\n");
        return -1;
    }

    /* server group create */
    g_srv_ctx->gid = ssg_group_create_mpi(MOBJECT_SERVER_GROUP_NAME, MPI_COMM_WORLD,
        NULL, NULL); /* XXX membership change callbacks unused currently */
    if (g_srv_ctx->gid == SSG_GROUP_ID_NULL)
    {
        fprintf(stderr, "Error: Unable to create the mobject server group\n");
        ssg_finalize();
        return -1;
    }

    /* write cluster connect info to file for clients to find later */
    ret = mobject_server_store_cluster_info(cluster_file);
    if (ret != 0)
    {
        fprintf(stderr, "Error: unable to store mobject cluster info to file %s\n",
            cluster_file);
        /* XXX: this call is performed by one process under the covers, and we
         * don't currently have a way to propagate this error to the entire cluster
         */
        ssg_group_destroy(g_srv_ctx->gid);
        ssg_finalize();
        return -1;
    }

    /* XXX cleanup? */

    return 0;
}

int mobject_server_register(margo_instance_id mid, const char *poolname)
{
    int ret=0;
    kv_context *metadata;
    struct bake_pool_info *pool_info;

    pool_info = bake_server_makepool(poolname);

    bake_server_register(mid, pool_info);
    metadata = kv_server_register(mid);
    return ret;
}
#endif

void mobject_server_shutdown(margo_instance_id mid)
{
    assert(g_srv_ctx);

    ssg_group_destroy(g_srv_ctx->gid);
    ssg_finalize();

    //margo_wait_for_finalize(mid);
    //pmemobj_close(NULL);

    return;
}

static int mobject_server_store_cluster_info(const char *file_name)
{
    int my_id;
    int ret;

    assert(g_srv_ctx);

    /* only have one group member do this */
    my_id = ssg_get_group_self_id(g_srv_ctx->gid);
    if (my_id != 0)
        return 0;

    ret = ssg_group_id_store(file_name, g_srv_ctx->gid);
    return ret;
}
