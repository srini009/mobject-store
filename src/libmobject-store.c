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
    margo_instance_id mid;
    ssg_group_id_t gid;
    int connected;
} mobject_store_handle_t;


int mobject_store_create(mobject_store_t *cluster, const char * const id)
{
    mobject_store_handle_t *cluster_handle;
    char *cluster_file;
    int ret;

    (void)id; /* XXX: id unused in mobject */

    /* allocate a new cluster handle and set some fields */
    cluster_handle = malloc(sizeof(*cluster_handle));
    if (!cluster_handle)
        return -1;
    memset(cluster_handle, 0, sizeof(cluster_handle));

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
    mobject_store_handle_t *cluster_handle = (mobject_store_handle_t *)cluster;
    char *srv_addr;
    char proto[24] = {0};
    int i;
    int ret;

    if (cluster_handle->connected)
        return 0;

    /* figure out protocol to connect with using address information 
     * associated with the SSG group ID
     */
    srv_addr = ssg_group_id_get_addr_str(cluster_handle->gid);
    if (!srv_addr)
    {
        fprintf(stderr, "Error: Unable to obtain cluster group server address\n");
        ssg_group_id_free(cluster_handle->gid);
        free(cluster_handle);
        return -1;
    }
    /* we only need to the proto portion of the address to initialize */
    for(i=0; i<24 && srv_addr[i] != '\0' && srv_addr[i] != ':'; i++)
        proto[i] = srv_addr[i];

    /* intialize margo */
    /* XXX: probably want to expose some way of tweaking threading parameters */
    cluster_handle->mid = margo_init(proto, MARGO_CLIENT_MODE, 0, -1);
    if (cluster_handle->mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: Unable to initialize margo\n");
        free(srv_addr);
        ssg_group_id_free(cluster_handle->gid);
        free(cluster_handle);
        return -1;
    }

    /* initialize ssg */
    ret = ssg_init(cluster_handle->mid);
    if (ret != SSG_SUCCESS)
    {
        fprintf(stderr, "Error: Unable to initialize SSG\n");
        margo_finalize(cluster_handle->mid);
        free(srv_addr);
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
        free(srv_addr);
        ssg_group_id_free(cluster_handle->gid);
        free(cluster_handle);
        return -1;
    }
    cluster_handle->connected = 1;

    free(srv_addr);

    return 0;
}

void mobject_store_shutdown(mobject_store_t cluster)
{
    mobject_store_handle_t *cluster_handle =
        (mobject_store_handle_t *)cluster;
    assert(cluster_handle != NULL);

    if (!cluster_handle->connected)
        return;

    ssg_group_detach(cluster_handle->gid);
    ssg_finalize();
    margo_finalize(cluster_handle->mid);
    ssg_group_id_free(cluster_handle->gid);
    free(cluster_handle);

    return;
}
