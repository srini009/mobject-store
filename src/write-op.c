/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "log.h"
#include "write-op.h"
#include "completion.h"

mobject_store_write_op_t mobject_store_create_write_op(void)
{
	mobject_store_write_op_t write_op = 
		(mobject_store_write_op_t)calloc(1, sizeof(struct mobject_store_write_op));
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "Could not allocate write_op");
	write_op->num_actions = 0;
	write_op->actions = (wr_action_base_t)0;
	return write_op;
}

void mobject_store_release_write_op(mobject_store_write_op_t write_op)
{
	if(write_op == MOBJECT_WRITE_OP_NULL) return;
	// TODO free internal fields
	free(write_op);
}

void mobject_store_write_op_create(mobject_store_write_op_t write_op,
                                   int exclusive,
                                   const char* category)
{
	wr_action_create_t action = (wr_action_create_t)calloc(1, sizeof(*action));
}

void mobject_store_write_op_write(mobject_store_write_op_t write_op,
                                  const char *buffer,
                                  size_t len,
                                  uint64_t offset)
{

}

void mobject_store_write_op_write_full(mobject_store_write_op_t write_op,
                                       const char *buffer,
                                       size_t len)
{

}

void mobject_store_write_op_writesame(mobject_store_write_op_t write_op,
                                      const char *buffer,
                                      size_t data_len,
                                      size_t write_len,
                                      uint64_t offset)
{

}

void mobject_store_write_op_append(mobject_store_write_op_t write_op,
                                   const char *buffer,
                                   size_t len)
{

}

void mobject_store_write_op_remove(mobject_store_write_op_t write_op)
{

}

void mobject_store_write_op_truncate(mobject_store_write_op_t write_op,
                                     uint64_t offset)
{

}

void mobject_store_write_op_zero(mobject_store_write_op_t write_op,
                                 uint64_t offset,
                                 uint64_t len)
{

}

void mobject_store_write_op_omap_set(mobject_store_write_op_t write_op,
                                     char const* const* keys,
                                     char const* const* vals,
                                     const size_t *lens,
                                     size_t num)
{

}

void mobject_store_write_op_omap_rm_keys(mobject_store_write_op_t write_op,
                                         char const* const* keys,
                                         size_t keys_len)
{

}

int mobject_store_write_op_operate(mobject_store_write_op_t write_op,
                                   mobject_store_ioctx_t io,
                                   const char *oid,
                                   time_t *mtime,
                                   int flags)
{

}

int mobject_store_aio_write_op_operate(mobject_store_write_op_t write_op,
                                       mobject_store_ioctx_t io,
                                       mobject_store_completion_t completion,
                                       const char *oid,
                                       time_t *mtime,
                                       int flags)
{

}
