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

/* XXX one global mobject server state struct */
mobject_server_context_t *g_srv_ctx = NULL;

int mobject_server_init(margo_instance_id mid)
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

    /* XXX cleanup? */

    return 0;
}

#if 0
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
