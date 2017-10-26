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

#define MOBJECT_CLUSTER_FILE_ENV "MOBJECT_CLUSTER_FILE"

typedef struct mobject_store_handle
{
    ssg_group_id_t gid;
} mobject_store_handle_t;


int mobject_store_create(mobject_store_t *cluster, const char * const id)
{
    mobject_store_handle_t *cluster_handle;
    char *cluster_file;
    int ret;

    (void)id; /* XXX: id unused in mobject */

    /* allocate a new cluster handle and set some fields */
    cluster_handle = (mobject_store_handle_t*)calloc(1,sizeof(*cluster_handle));
    if (!cluster_handle)
        return -1;

    /* use env variable to determine how to connect to the cluster */
    /* NOTE: this is the _only_ method for specifying a cluster for now... */
    cluster_file = getenv(MOBJECT_CLUSTER_FILE_ENV);
    if (!cluster_file)
    {
        fprintf(stderr, "Error: %s env variable must point to mobject cluster file\n",
            MOBJECT_CLUSTER_FILE_ENV);
        return -1;
    }

    ret = ssg_group_id_load(cluster_file, &cluster_handle->gid);
    if (ret != 0)
    {
        fprintf(stderr, "Error: Unable to load mobject cluster info from file %s\n",
            cluster_file);
        return -1;
    }

    /* set the returned cluster handle */
    *cluster = cluster_handle;

    return 0;
}

int mobject_store_connect(mobject_store_t cluster)
{
    /* TODO ssg attach to mobject cluster group id */

    return 0;
}

void mobject_store_shutdown(mobject_store_t cluster)
{
    mobject_store_handle_t *cluster_handle =
        (mobject_store_handle_t *)cluster;
    assert(cluster_handle != NULL);

    /* TODO ssg detatch from mobject cluster group id. free gid? */

    free(cluster_handle);

    return;
}
