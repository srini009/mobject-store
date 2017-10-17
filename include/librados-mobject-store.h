/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __RADOS_MOBJECT_STORE
#define __RADOS_MOBJECT_STORE

#ifdef __cplusplus
extern "C" {
#endif

#include "libmobject-store.h"

typedef mobject_store_t rados_t;

#define LIBRADOS_OPERATION_NOFLAG             LIBMOBJECT_OPERATION_NOFLAG
#define LIBRADOS_OPERATION_BALANCE_READS      LIBMOBJECT_OPERATION_BALANCE_READS
#define LIBRADOS_OPERATION_LOCALIZE_READS     LIBMOBJECT_OPERATION_LOCALIZE_READS
#define LIBRADOS_OPERATION_ORDER_READS_WRITES LIBMOBJECT_OPERATION_ORDER_READS_WRITES
#define LIBRADOS_OPERATION_IGNORE_CACHE       LIBMOBJECT_OPERATION_IGNORE_CACHE
#define LIBRADOS_OPERATION_SKIPRWLOCKS        LIBMOBJECT_OPERATION_SKIPRWLOCKS
#define LIBRADOS_OPERATION_IGNORE_OVERLAY     LIBMOBJECT_OPERATION_IGNORE_OVERLAY
#define LIBRADOS_OPERATION_FULL_TRY           LIBMOBJECT_OPERATION_FULL_TRY
#define LIBRADOS_OPERATION_FULL_FORCE         LIBMOBJECT_OPERATION_FULL_FORCE
#define LIBRADOS_OPERATION_IGNORE_REDIRECT    LIBMOBJECT_OPERATION_IGNORE_REDIRECT

typedef mobject_store_ioctx_t      rados_ioctx_t;
typedef mobject_store_omap_iter_t  rados_omap_iter_t;
typedef mobject_store_write_op_t   rados_write_op_t;
typedef mobject_store_read_op_t    rados_read_op_t;
typedef mobject_store_completion_t rados_completion_t;
typedef mobject_store_callback_t   rados_callback_t;

#define rados_create                        mobject_store_create
#define rados_connect                       mobject_store_connect
#define rados_shutdown                      mobject_store_shutdown
#define rados_ioctx_create                  mobject_store_ioctx_create
#define rados_ioctx_destroy                 mobject_store_ioctx_destroy
#define rados_create_write_op               mobject_store_create_write_op
#define rados_release_write_op              mobject_store_release_write_op
#define rados_write_op_create               mobject_store_write_op_create
#define rados_write_op_write                mobject_store_write_op_write
#define rados_write_op_write_full           mobject_store_write_op_write_full
#define rados_write_op_writesame            mobject_store_write_op_writesame
#define rados_write_op_append               mobject_store_write_op_append
#define rados_write_op_remove               mobject_store_write_op_remove
#define rados_write_op_truncate             mobject_store_write_op_truncate
#define rados_write_op_zero                 mobject_store_write_op_zero
#define rados_write_op_omap_set             mobject_store_write_op_omap_set
#define rados_write_op_omap_rm_keys         mobject_store_write_op_omap_rm_keys
#define rados_write_op_operate              mobject_store_write_op_operate
#define rados_aio_write_op_operate          mobject_store_aio_write_op_operate
#define rados_create_read_op                mobject_store_create_read_op
#define rados_release_read_op               mobject_store_release_read_op
#define rados_read_op_stat                  mobject_store_read_op_stat
#define rados_read_op_read                  mobject_store_read_op_read
#define rados_read_op_omap_get_vals         mobject_store_read_op_omap_get_vals
#define rados_read_op_omap_get_vals_by_keys mobject_store_read_op_omap_get_vals_by_keys
#define rados_read_op_operate               mobject_store_read_op_operate
#define rados_aio_read_op_operate           mobject_store_aio_read_op_operate
#define rados_omap_get_next                 mobject_store_omap_get_next
#define rados_omap_get_end                  mobject_store_omap_get_end
#define rados_aio_create_completion         mobject_store_aio_create_completion
#define rados_aio_wait_for_complete         mobject_store_aio_wait_for_complete
#define rados_aio_is_complete               mobject_store_aio_is_complete
#define rados_aio_get_return_value          mobject_store_aio_get_return_value
#define rados_aio_release                   mobject_store_aio_release

#ifdef __cplusplus
}
#endif

#endif /* __RADOS_MOBJECT_STORE */

