/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "libmobject-store.h"
#include "src/client/io-context.h"
#include "src/client/cluster-handle.h"

int mobject_store_ioctx_create(
    mobject_store_t cluster,
    const char * pool_name,
    mobject_store_ioctx_t *ioctx)
{
    (void)pool_name; /* XXX pool is ignored for now and instead use one global "pool" */

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
