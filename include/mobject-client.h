/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __MOBJECT_CLIENT_H
#define __MOBJECT_CLIENT_H

#include <stdint.h>
#include <margo.h>

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct mobject_client* mobject_client_t;
    typedef struct mobject_provider_handle* mobject_provider_handle_t;
    typedef struct mobject_store_write_op *mobject_store_write_op_t;
    typedef struct mobject_store_read_op *mobject_store_read_op_t;
    typedef struct mobject_store_omap_iter *mobject_store_omap_iter_t;
    typedef struct mobject_store_completion* mobject_store_completion_t;
    typedef struct mobject_request* mobject_request_t;

#define MOBJECT_CLIENT_NULL          ((mobject_client_t)NULL)
#define MOBJECT_PROVIDER_HANDLE_NULL ((mobject_provider_handle_t)NULL)
#define MOBJECT_WRITE_OP_NULL        ((mobject_store_write_op_t)NULL)
#define MOBJECT_READ_OP_NULL         ((mobject_store_read_op_t)NULL)
#define MOBJECT_REQUEST_NULL         ((mobject_request_t)NULL)

    /**
     * Creates a Mobject client attached to the given margo instance.
     * This will effectively register the RPC needed by BAKE into
     * the margo instance. The client must be freed with
     * mobject_client_finalize.
     *
     * @param[in] mid margo instance
     * @param[out] client resulting mobject client object
     *
     * @return 0 on success, -1 on failure
     */
    int mobject_client_init(margo_instance_id mid, mobject_client_t* client);

    /**
     * Finalizes a Mobject client.
     * WARNING: This function must not be called after Margo has been
     * finalized. If you need to finalize a Mobject client when Margo is
     * finalized, use margo_push_finalize_callback.
     *
     * @param client Mobject client to destroy
     *
     * @return 0 on success, -1 on failure
     */
    int mobject_client_finalize(mobject_client_t client);

    /**
     * Creates a provider handle to point to a particular Mobject provider.
     *
     * @param client client managing the provider handle
     * @param addr address of the provider
     * @param mplex_id multiplex id of the provider
     * @param handle resulting handle
     *
     * @return 0 on success, -1 on failure
     */
    int mobject_provider_handle_create(
            mobject_client_t client,
            hg_addr_t addr,
            uint8_t mplex_id,
            mobject_provider_handle_t* handle);

    /**
     * Increment the reference counter of the provider handle
     *
     * @param handle provider handle
     *
     * @return 0 on success, -1 on failure
     */
    int mobject_provider_handle_ref_incr(mobject_provider_handle_t handle);

    /**
     * Decrement the reference counter of the provider handle,
     * effectively freeing the provider handle when the reference count
     * is down to 0.
     *
     * @param handle provider handle
     *
     * @return 0 on success, -1 on failure
     */
    int mobject_provider_handle_release(mobject_provider_handle_t handle);

    int mobject_shutdown(mobject_client_t client, hg_addr_t addr);

    /**
     * Create a new mobject_store_write_op_t write operation.
     * This will store all actions to be performed atomically.
     * You must call mobject_store_release_write_op when you are
     * finished with it.
     *
     * @returns non-NULL on success, NULL on memory allocation error.
     */
    mobject_store_write_op_t mobject_create_write_op(void);

    /**
     * Free a mobject_store_write_op_t, must be called when you're done with it.
     * @param write_op operation to deallocate
     */
    void mobject_release_write_op(mobject_store_write_op_t write_op);

    /**
     * Create the object
     * @param write_op operation to add this action to
     * @param exclusive set to either LIBMOBJECT_CREATE_EXCLUSIVE or
     * LIBMOBJECT_CREATE_IDEMPOTENT
     * will error if the object already exists.
     * @param category category string (DEPRECATED, HAS NO EFFECT)
     */
    void mobject_write_op_create(
            mobject_store_write_op_t write_op,
            int exclusive,
            const char* category);

    /**
     * Write to offset
     * @param write_op operation to add this action to
     * @param offset offset to write to
     * @param buffer bytes to write
     * @param len length of buffer
     */
    void mobject_write_op_write(
            mobject_store_write_op_t write_op,
            const char *buffer,
            uint64_t offset,
            size_t len);

    /**
     * Write whole object, atomically replacing it.
     * @param write_op operation to add this action to
     * @param buffer bytes to write
     * @param len length of buffer
     */
    void mobject_write_op_write_full(
            mobject_store_write_op_t write_op,
            const char *buffer,
            size_t len);

    /**
     * Write the same buffer multiple times
     * @param write_op operation to add this action to
     * @param buffer bytes to write
     * @param data_len length of buffer
     * @param write_len total number of bytes to write, as a multiple of @data_len
     * @param offset offset to write to
     */
    void mobject_write_op_write_same(
            mobject_store_write_op_t write_op,
            const char *buffer,
            uint64_t offset,
            size_t data_len,
            size_t write_len);

    /**
     * Append to end of object.
     * @param write_op operation to add this action to
     * @param buffer bytes to write
     * @param len length of buffer
     */
    void mobject_write_op_append(
            mobject_store_write_op_t write_op,
            const char *buffer,
            size_t len);

    /**
     * Remove object
     * @param write_op operation to add this action to
     */
    void mobject_write_op_remove(mobject_store_write_op_t write_op);

    /**
     * Truncate an object
     * @param write_op operation to add this action to
     * @param offset Offset to truncate to
     */
    void mobject_write_op_truncate(
            mobject_store_write_op_t write_op,
            uint64_t offset);

    /**
     * Zero part of an object
     * @param write_op operation to add this action to
     * @param offset Offset to zero
     * @param len length to zero
     */
    void mobject_write_op_zero(
            mobject_store_write_op_t write_op,
            uint64_t offset,
            uint64_t len);

    /**
     * Set key/value pairs on an object
     *
     * @param write_op operation to add this action to
     * @param keys array of null-terminated char arrays representing keys to set
     * @param vals array of pointers to values to set
     * @param lens array of lengths corresponding to each value
     * @param num number of key/value pairs to set
     */
    void mobject_write_op_omap_set(
            mobject_store_write_op_t write_op,
            char const* const* keys,
            char const* const* vals,
            const size_t *lens,
            size_t num);

    /**
     * Remove key/value pairs from an object
     *
     * @param write_op operation to add this action to
     * @param keys array of null-terminated char arrays representing keys to remove
     * @param keys_len number of key/value pairs to remove
     */
    void mobject_write_op_omap_rm_keys(
            mobject_store_write_op_t write_op,
            char const* const* keys,
            size_t keys_len);

    /**
     * Perform a write operation synchronously
     * @param write_op operation to perform
     * @param pool_name the name of the pool in which to write 
     * @param oid the object id
     * @param mtime the time to set the mtime to, NULL for the current time
     * @param flags flags to apply to the entire operation (LIBMOBJECT_OPERATION_*)
     */
    int mobject_write_op_operate(
            mobject_provider_handle_t handle,
            mobject_store_write_op_t write_op,
            const char* pool_name,
            const char *oid,
            time_t *mtime,
            int flags);

    /**
     * Perform a write operation asynchronously
     * @param write_op operation to perform
     * @param pool_name the name of the pool in which to write 
     * @param oid the object id
     * @param mtime the time to set the mtime to, NULL for the current time
     * @param flags flags to apply to the entire operation (LIBMOBJECT_OPERATION_*)
     * @param req resulting request
     */
    int mobject_aio_write_op_operate(
            mobject_provider_handle_t handle,
            mobject_store_write_op_t write_op,
            const char* pool_name,
            const char *oid,
            time_t *mtime,
            int flags,
            mobject_request_t* req);

    /**
     * Create a new mobject_store_read_op_t write operation. This will store all
     * actions to be performed atomically. You must call
     * mobject_store_release_read_op when you are finished with it (after it
     * completes, or you decide not to send it in the first place).
     *
     * @returns non-NULL on success, NULL on memory allocation error.
     */
    mobject_store_read_op_t mobject_create_read_op(void);

    /**
     * Free a mobject_store_read_op_t, must be called when you're done with it.
     * @param read_op operation to deallocate, created with mobject_create_read_op
     */
    void mobject_release_read_op(mobject_store_read_op_t read_op);

    /**
     * Get object size and mtime
     * @param read_op operation to add this action to
     * @param psize where to store object size
     * @param pmtime where to store modification time
     * @param prval where to store the return value of this action
     */
    void mobject_read_op_stat(
            mobject_store_read_op_t read_op,
            uint64_t *psize,
            time_t *pmtime,
            int *prval);
    /**
     * Read bytes from offset into buffer.
     *
     * prlen will be filled with the number of bytes read if successful.
     * A short read can only occur if the read reaches the end of the
     * object.
     *
     * @param read_op operation to add this action to
     * @param offset offset to read from
     * @param len length of buffer
     * @param buffer where to put the data
     * @param bytes_read where to store the number of bytes read by this action
     * @param prval where to store the return value of this action
     */
    void mobject_read_op_read(
            mobject_store_read_op_t read_op,
            char *buffer,
            uint64_t offset,
            size_t len,
            size_t *bytes_read,
            int *prval);

    /**
     * Start iterating over keys on an object.
     *
     * They will be returned sorted by key, and the iterator
     * will fill in NULL for all values if specified.
     *
     * @param read_op operation to add this action to
     * @param start_after list keys starting after start_after
     * @param max_return list no more than max_return keys
     * @param iter where to store the iterator
     * @param prval where to store the return value from this action
     */
    void mobject_read_op_omap_get_keys(
            mobject_store_read_op_t read_op,
            const char *start_after,
            uint64_t max_return,
            mobject_store_omap_iter_t *iter,
            int *prval);

    /**
     * Start iterating over key/value pairs on an object.
     *
     * They will be returned sorted by key.
     *
     * @param read_op operation to add this action to
     * @param start_after list keys starting after start_after
     * @param filter_prefix list only keys beginning with filter_prefix
     * @param max_return list no more than max_return key/value pairs
     * @param iter where to store the iterator
     * @param prval where to store the return value from this action
     */
    void mobject_read_op_omap_get_vals(
            mobject_store_read_op_t read_op,
            const char *start_after,
            const char *filter_prefix,
            uint64_t max_return,
            mobject_store_omap_iter_t *iter,
            int *prval);

    /**
     * Start iterating over specific key/value pairs
     *
     * They will be returned sorted by key.
     *
     * @param read_op operation to add this action to
     * @param keys array of pointers to null-terminated keys to get
     * @param keys_len the number of strings in keys
     * @param iter where to store the iterator
     * @param prval where to store the return value from this action
     */
    void mobject_read_op_omap_get_vals_by_keys(
            mobject_store_read_op_t read_op,
            char const* const* keys,
            size_t keys_len,
            mobject_store_omap_iter_t *iter,
            int *prval);


    /**
     * Perform a read operation synchronously
     * @param read_op operation to perform
     * @param pool_name the pool that the object is in
     * @param oid the object id
     * @param flags flags to apply to the entire operation (LIBMOBJECT_OPERATION_*)
     */
    int mobject_read_op_operate(
            mobject_provider_handle_t handle,
            mobject_store_read_op_t read_op,
            const char *pool_name,
            const char *oid,
            int flags);

    /**
     * Perform a read operation asynchronously
     * @param read_op operation to perform
     * @param pool_name the pool that the object is in
     * @param completion what to do when operation has been attempted
     * @param oid the object id
     * @param flags flags to apply to the entire operation (LIBMOBJECT_OPERATION_*)
     */
    int mobject_aio_read_op_operate(
            mobject_provider_handle_t handle,
            mobject_store_read_op_t read_op,
            const char *pool_name,
            const char *oid,
            int flags,
            mobject_request_t* req);

    int mobject_aio_wait(mobject_request_t req, int* ret);

    int mobject_aio_test(mobject_request_t req, int* flag);

#ifdef __cplusplus
}
#endif

#endif /* __MOBJECT_CLIENT_H */
