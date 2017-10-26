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

int mobject_server_init(margo_instance_id mid, const char *cluster_file);

/**
 * Start a mobject server instance
 *
 * @param[in] mid
 * @param[in poolname
 * @returns 0 on success, negative error code on failure */
int mobject_server_register(margo_instance_id mid, const char *poolname);

void mobject_server_shutdown(margo_instance_id mid);

#endif
