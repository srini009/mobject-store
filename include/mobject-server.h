/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef MOBJECT_SERVER_H
#define MOBJECT_SERVER_H

#include <margo.h>
#include <bake-client.h>
#include <sdskv-client.h>
/* server-side utilities and routines.  Clients are looking for either
 * libmobject-store.h or librados-mobject-store.h */

#define MOBJECT_SERVER_GROUP_NAME "mobject-store-servers"
#define MOBJECT_ABT_POOL_DEFAULT ABT_POOL_NULL

typedef struct mobject_server_context* mobject_provider_t;

/**
 * Start a mobject server instance
 *
 * @param[in] mid           margo instance id
 * @param[in] mplex_id      multiplex id of the provider
 * @param[in] pool          Argobots pool for the provider
 * @param[in] bake_ph       Bake provider handle to use to write/read data
 * @param[in] sdskv_ph      SDSKV provider handle to use to access metadata
 * @param[in] cluster_file  file name to write cluster connect info to
 * @param[out] provider     resulting provider
 * 
 * @returns 0 on success, negative error code on failure
 */
int mobject_provider_register(
        margo_instance_id mid,
        uint8_t mplex_id,
        ABT_pool pool,
        bake_provider_handle_t bake_ph,
        sdskv_provider_handle_t sdskv_ph,
        const char *cluster_file,
        mobject_provider_t* provider);

#endif
