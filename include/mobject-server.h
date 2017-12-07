/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef MOBJECT_SERVER_H
#define MOBJECT_SERVER_H

#include <margo.h>
/* server-side utilities and routines.  Clients are looking for either
 * libmobject-store.h or librados-mobject-store.h */

#define MOBJECT_SERVER_GROUP_NAME "mobject-store-servers"

typedef struct mobject_server_context mobject_server_context_t;

/**
 * Start a mobject server instance
 *
 * @param[in] mid           margo instance id
 * @param[in] cluster_file  file name to write cluster connect info to
 * @returns 0 on success, negative error code on failure
 */
mobject_server_context_t* mobject_server_init(margo_instance_id mid, const char *cluster_file);

/**
 * Shutdown a mobject server instance
 * 
 * @param[in] mid   margo instance id
 */
void mobject_server_shutdown(mobject_server_context_t* svr_ctx);

/**
 * Wait for a mobject server instance to get a shutdown request.
 */
void mobject_server_wait_for_shutdown(mobject_server_context_t* srv_ctx);

#endif
