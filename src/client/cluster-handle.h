/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_STORE_HANDLE_H
#define __MOBJECT_STORE_HANDLE_H

#include <margo.h>
#include <ssg.h>

typedef struct mobject_store_handle
{
    margo_instance_id mid;
    ssg_group_id_t gid;
    int connected;
} mobject_store_handle_t;

#endif
