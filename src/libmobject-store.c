/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <margo.h>
#include <ssg.h>

#include "mobject-store.h"

struct mobject_store_handle
{
    int test;
};

int rados_create(rados_t *cluster, const char * const id)
{
    struct mobject_store_handle *cluster_handle;
    (void)id; /* XXX: id unused in mobject */

    /* allocate a new cluster handle and set some fields */
    cluster_handle = malloc(sizeof(*cluster_handle));
    if (!cluster_handle)
        return(-1); /* TODO: error codes */
    cluster_handle->test = 123;

    /* XXX find the SSG group ID for the mobject cluster group */

    return(0);
}

#if 0
int mobject_store_connect(mobject_store_t cluster)
{
    /* XXX ssg attach to mobject cluster group id */
}
#endif

void rados_shutdown(rados_t cluster)
{
    /* XXX ssg detatch from mobject cluster group id */

    /* XXX free handle */
}

