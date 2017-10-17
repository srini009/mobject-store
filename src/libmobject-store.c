/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include "mobject-store-config.h"

#include <stdio.h>
#include <assert.h>

#include <margo.h>
#include <ssg.h>

#include "libmobject-store.h"

struct mobject_store_handle
{
    int test;
};

int mobject_store_create(mobject_store_t *cluster, const char * const id)
{
    struct mobject_store_handle *cluster_handle;
    (void)id; /* XXX: id unused in mobject */

    /* allocate a new cluster handle and set some fields */
    cluster_handle = malloc(sizeof(*cluster_handle));
    if (!cluster_handle)
        return(-1); /* TODO: error codes */
    cluster_handle->test = 123;

    /* TODO find the SSG group ID for the mobject cluster group */

    /* set the returned cluster handle */
    *cluster = cluster_handle;

    return(0);
}

#if 0
int mobject_store_connect(mobject_store_t cluster)
{
    /* TODO ssg attach to mobject cluster group id */
}
#endif

void mobject_store_shutdown(mobject_store_t cluster)
{
    struct mobject_store_handle *cluster_handle =
        (struct mobject_store_handle *)cluster;
    assert(cluster_handle != NULL);

    /* TODO ssg detatch from mobject cluster group id */

    free(cluster_handle);

    return;
}

