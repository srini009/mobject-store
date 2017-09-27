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

};

const char *mobject_global_pool = "mobject-global-pool";

int mobject_store_create(mobject_store_t *cluster)
{
    /* XXX alloc, set handle */

    /* XXX find the SSG group ID for the mobject cluster group */
}

int mobject_store_connect(mobject_store_t cluster)
{
    /* XXX ssg attach to mobject cluster group id */
}

void mobject_store_shutdown(mobject_store_t cluster)
{
    /* XXX ssg detatch from mobject cluster group id */

    /* XXX free handle */
}

