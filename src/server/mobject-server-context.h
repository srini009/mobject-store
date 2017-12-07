/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SERVER_MOBJECT_CONTEXT_H
#define __SERVER_MOBJECT_CONTEXT_H

#include <margo.h>
//#include <sds-keyval.h>
#include <bake-bulk-server.h>
#include <bake-bulk-client.h>
//#include <libpmemobj.h>
#include <ssg-mpi.h>

#ifdef __cplusplus
extern "C" {
#endif

struct mobject_server_context
{
    /* margo, bake, sds-keyval, ssg state */
    margo_instance_id mid;
    /* TODO bake, sds-keyval stuff */
    ssg_group_id_t gid;
    bake_target_id_t bake_id;
    /* server shutdown conditional logic */
    ABT_mutex shutdown_mutex;
    ABT_cond shutdown_cond;
    int shutdown_flag;
    int ref_count;
};

#ifdef __cplusplus
}
#endif

#endif
