/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __MOBJECT_STORE
#define __MOBJECT_STORE

#ifdef __cplusplus
extern "C" {
#endif

// derived from: http://docs.ceph.com/docs/master/rados/api/librados/


/* KEY TERMS
 * 
 * Pool:
 *      - an object container providing separate namespaces for objects
 *      - pools can have different placement & replication strategies
 *
 * Placement group (PG):
 *      - Groups of objects within a pool that share the same OSDs.
 *
 * CRUSH:
 *      - Data placement algorithm for RADOS objects, mapping OIDs to PGs.
 */

/* ASSUMPTIONS/QUESTIONS
 * - Initially we enforce one global pool for each mobject store instance for simplicity.
 *
 * - Does ch-placement offer an algorithm similar to crush (e.g., PG concept)? 
 *
 * - We will not implement any replication.
 *
 * - RADOS seems to expose object versions, but I only see them in the 'object operation' portion of their API, which is big and awkward (i.e., would love to not implement that). 
 *
 * - Do we need any of the following from RADOS:
 *      - async I/O operations?
 *      - cursor stuff?
 *      - snapshots?
 *      - extended attributes?
 * 
 * - RADOS API includes a watch/notify API that sounds like something we could build on top of a generic SSG pub-sub service, if we wanted.
 *
 */


/* opaque handle for interacting with a mobject cluster */
typedef struct mobject_store_handle *mobject_store_t;

/* opaque mobject store i/o context type */
typedef struct mobject_store_ioctx *mobject_store_ioctx_t;

extern const char *mobject_global_pool;

/*****************************************
 * mobject store setup/teardown routines *
 *****************************************/

/**
 * Create a handle to a mobject cluster.
 *
 * @param[in/out] cluster   pointer to store mobject cluster handle at
 * @returns 0 on success, negative error code on failure
 *
 * NOTES:
 *      - from rados_create(rados_t * cluster, const char *const user_id)
 *          - drop user_id from API
 *      - may eventually need conf. file, env variable checking, etc.
 */
int mobject_store_create(
    mobject_store_t *cluster);

/**
 * Connect to a mobject cluster.
 *
 * @param[in] cluster   handle to mobject cluster
 * @returns 0 on success, negative error code on failure
 */
int mobject_store_connect(
    mobject_store_t cluster);

/**
 * Disconnects from a mobject cluster and destroys the cluster handle.
 * 
 * @param[in] cluster   handle to mobject cluster
 */
void mobject_store_shutdown(
    mobject_store_t cluster);

/*****************************************************
 * mobject store i/o context setup/teardown routines *
 *****************************************************/

/**
 * Create an I/O context for a mobject cluster.
 *
 * @param[in] cluster       handle to mobject cluster
 * @param[in] pool_name     name of the pool
 * @param[in/out] ioctx     pointer to store the io context at
 * @returns 0 on success, negative error code on failure
 *
 * NOTES:
 *      - only one global pool exists and is defined at the top of this header
 */
int mobject_store_ioctx_create(
    mobject_store_t cluster,
    const char * pool_name,
    mobject_store_ioctx_t *ioctx);

/**
 * Destroy an I/O context.
 *
 * @param[in] ioctx     io context to destroy
 */
void mobject_store_ioctx_destroy(
    mobject_store_ioctx_t ioctx);

/******************************
 * mobject store i/o routines *
 ******************************/

/**
 * Write @p len bytes from @p buf into object @p oid starting at offset @p off.
 *
 * @param[in] ioctx     io context
 * @param[in] oid       name of the object
 * @param[in] buf       buffer containing data to write
 * @param[in] len       length of the data
 * @param[in] off       byte offset within object to write at
 * @returns 0 on success, negative error code on failure
 */
mobject_store_write(
    mobject_store_ioctx_t ioctx,
    const char * oid,
    const char * buf,
    size_t len,
    uint64_t off);

/**
 * Write @p len bytes from @p buf into object @p oid. The object is filled with
 * the provided data; if the object exists, it is truncated first.
 *
 * @param[in] ioctx     io context
 * @param[in] oid       name of the object
 * @param[in] buf       buffer containing data to write
 * @param[in] len       length of the data
 * @returns 0 on success, negative error code on failure
 */
mobject_store_write_full(
    mobject_store_ioctx_t ioctx,
    const char * oid,
    const char * buf,
    size_t len);

/**
 * Append @p len bytes from @p buf into object @p oid.
 *
 * @param[in] ioctx     io context
 * @param[in] oid       name of the object
 * @param[in] buf       buffer containing data to append
 * @param[in] len       length of the data
 * @returns 0 on success, negative error code on failure
 */
mobject_store_append(
    mobject_store_ioctx_t ioctx,
    const char * oid,
    const char * buf,
    size_t len);

/**
 * Read @p len bytes from object @p oid starting at offset @p off into @p buf.
 *
 * @param[in] ioctx     io context
 * @param[in] oid       name of the object
 * @param[in] buf       buffer containing data to write
 * @param[in] len       length of the data
 * @param[in] off       byte offset within object to write at
 * @returns 0 on success, negative error code on failure
 */
mobject_store_read(
    mobject_store_ioctx_t ioctx,
    const char * oid,
    const char * buf,
    size_t len,
    uint64_t off);

/**
 * Deletes an object.
 *
 * @param[in] ioctx     io context
 * @param[in] oid       name of the object
 * @returns 0 on success, negative error code on failure
 */
mobject_store_remove(
    mobject_store_ioctx_t ioctx,
    const char * oid);

/**
 * Resize an object. If this enlarges the object, the new area is logically
 * filled with zeroes. If this shrinks the object, the excess data is removed.
 *
 * @param[in] ioctx     io context
 * @param[in] oid       name of the object
 * @param[in] size      new size of the object
 * @returns 0 on success, negative error code on failure
 */
mobject_store_trunc(
    mobject_store_ioctx_t ioctx,
    const char * oid,
    uint64_t size);

/**
 * Get object stats.
 *
 * @param[in] ioctx         io context
 * @param[in] oid           name of the object
 * @param[in/out] psize     pointer to store object size
 * @param[in/out] pmtime    pointer to store object modification time
 * @returns 0 on success, negative error code on failure
 */
mobject_store_stat(
    mobject_store_ioctx_t ioctx,
    const char * oid,
    uint64_t * psize,
    time_t * pmtime);

#ifdef __cplusplus
}
#endif

#endif /* __MOBJECT_STORE */

