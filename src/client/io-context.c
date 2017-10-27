/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "libmobject-store.h"
#include "src/client/io-context.h"

int mobject_store_ioctx_create(
    mobject_store_t cluster,
    const char * pool_name,
    mobject_store_ioctx_t *ioctx)
{
    // TODO take mid from cluster parameter
    *ioctx = (mobject_store_ioctx_t)calloc(1, sizeof(**ioctx));
    (*ioctx)->pool_name = strdup(pool_name);
}

void mobject_store_ioctx_destroy(mobject_store_ioctx_t ioctx)
{
    if(ioctx) free(ioctx->pool_name);
    free(ioctx);
}
