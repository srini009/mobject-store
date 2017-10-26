/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include <string.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/io-chain/write-op-impl.h"
#include "src/aio/completion.h"
#include "src/util/utlist.h"
#include "src/util/log.h"

mobject_store_write_op_t mobject_store_create_write_op(void)
{
	mobject_store_write_op_t write_op = 
		(mobject_store_write_op_t)calloc(1, sizeof(struct mobject_store_write_op));
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "Could not allocate write_op");
	write_op->actions     = (wr_action_base_t)0;
	write_op->bulk_handle = HG_BULK_NULL;
	write_op->num_actions = 0;
	write_op->ready       = 0;
	return write_op;
}

void mobject_store_release_write_op(mobject_store_write_op_t write_op)
{
	if(write_op == MOBJECT_WRITE_OP_NULL) return;

	if(write_op->bulk_handle != HG_BULK_NULL) 
		margo_bulk_free(write_op->bulk_handle);
	
	wr_action_base_t action, tmp;

	DL_FOREACH_SAFE(write_op->actions, action, tmp) {
		DL_DELETE(write_op->actions, action);
		free(action);
	}

	free(write_op);
}

void mobject_store_write_op_create(mobject_store_write_op_t write_op,
                                   int exclusive,
                                   const char* category)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	MOBJECT_ASSERT(!(write_op->ready), "can't modify a write_op that is ready to be processed");

	wr_action_create_t action = (wr_action_create_t)calloc(1, sizeof(*action));
	action->base.type         = WRITE_OPCODE_CREATE;
	action->exclusive         = exclusive;

	WRITE_ACTION_UPCAST(base, action);
	DL_APPEND(write_op->actions, base);

	write_op->num_actions += 1;
}

void mobject_store_write_op_write(mobject_store_write_op_t write_op,
                                  const char *buffer,
                                  size_t len,
                                  uint64_t offset)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	MOBJECT_ASSERT(!(write_op->ready), "can't modify a write_op that is ready to be processed");

	wr_action_write_t action  = (wr_action_write_t)calloc(1, sizeof(*action));
	action->base.type         = WRITE_OPCODE_WRITE;
	action->buffer.as_pointer = buffer;
	action->len               = len;
	action->offset            = offset;
	
	WRITE_ACTION_UPCAST(base, action);
	DL_APPEND(write_op->actions, base);

	write_op->num_actions += 1;
}

void mobject_store_write_op_write_full(mobject_store_write_op_t write_op,
                                       const char *buffer,
                                       size_t len)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	MOBJECT_ASSERT(!(write_op->ready), "can't modify a write_op that is ready to be processed");
	
	wr_action_write_full_t action = (wr_action_write_full_t)calloc(1, sizeof(*action));
	action->base.type             = WRITE_OPCODE_WRITE_FULL;
	action->buffer.as_pointer     = buffer;
	action->len                   = len;

/* POTENTIAL OPTIMIZATION (INCOMPLETE)
	// a write_full will replace the entire content of the object
	// so we can try and optimize by removing some operations that will
	// be overwritten anyway
	wr_action_base_t current, temp;
	DL_FOREACH_SAFE(write_op->actions, current, temp) {
		if(current->type == WRITE_OPCODE_WRITE
		|| current->type == WRITE_OPCODE_WRITE_FULL
		|| current->type == WRITE_OPCODE_WRITE_SAME
		|| current->type == WRITE_OPCODE_APPEND
		|| current->type == WRITE_OPCODE_TRUNCATE
		|| current->type == WRITE_OPCODE_ZERO) {
			DL_DELETE(write_op->actions, current);
			free(current);
		}
	}
*/

	WRITE_ACTION_UPCAST(base, action);
	DL_APPEND(write_op->actions, base);

	write_op->num_actions += 1;
}

void mobject_store_write_op_writesame(mobject_store_write_op_t write_op,
                                      const char *buffer,
                                      size_t data_len,
                                      size_t write_len,
                                      uint64_t offset)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	MOBJECT_ASSERT(!(write_op->ready), "can't modify a write_op that is ready to be processed");

	wr_action_write_same_t action = (wr_action_write_same_t)calloc(1, sizeof(*action));
	action->base.type             = WRITE_OPCODE_WRITE_SAME;
	action->buffer.as_pointer     = buffer;
	action->data_len              = data_len;
	action->write_len             = write_len;
	action->offset                = offset;

	WRITE_ACTION_UPCAST(base, action);
	DL_APPEND(write_op->actions, base);

	write_op->num_actions += 1;
}

void mobject_store_write_op_append(mobject_store_write_op_t write_op,
                                   const char *buffer,
                                   size_t len)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	MOBJECT_ASSERT(!(write_op->ready), "can't modify a write_op that is ready to be processed");

	wr_action_append_t action = (wr_action_append_t)calloc(1, sizeof(*action));
	action->base.type         = WRITE_OPCODE_APPEND;
	action->buffer.as_pointer = buffer;
	action->len               = len;

	WRITE_ACTION_UPCAST(base, action);
	DL_APPEND(write_op->actions, base);

	write_op->num_actions += 1;
}

void mobject_store_write_op_remove(mobject_store_write_op_t write_op)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	MOBJECT_ASSERT(!(write_op->ready), "can't modify a write_op that is ready to be processed");

	wr_action_remove_t action = (wr_action_remove_t)calloc(1, sizeof(*action));
	action->base.type         = WRITE_OPCODE_REMOVE;

/* THE FOLLOWING IS A POTENTIAL (INCOMPLETE) OPTIMIZATION
	// a remove operation will make all previous operations unecessary
	// so we can delete all previously posted operations (and potentially
	// not even post the remove)	
	wr_action_base_t current, temp;
	int do_not_even_post = 0;
	DL_FOREACH_SAFE(write_op->actions, current, temp) {
		if(current->type == WRITE_OPCODE_CREATE)
			do_not_even_post = 1;
		DL_DELETE(write_op->actions, current);
		free(current);
	}

	if(!do_not_even_post) {
		WRITE_ACTION_UPCAST(base, action);
		DL_APPEND(write_op->actions, base);
	}
*/
	WRITE_ACTION_UPCAST(base, action);
	DL_APPEND(write_op->actions, base);

	write_op->num_actions += 1;
}

void mobject_store_write_op_truncate(mobject_store_write_op_t write_op,
                                     uint64_t offset)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	MOBJECT_ASSERT(!(write_op->ready), "can't modify a write_op that is ready to be processed");

	wr_action_truncate_t action = (wr_action_truncate_t)calloc(1, sizeof(*action));
	action->base.type           = WRITE_OPCODE_TRUNCATE;
	action->offset              = offset;
	
	WRITE_ACTION_UPCAST(base, action);
	DL_APPEND(write_op->actions, base);

	write_op->num_actions += 1;
}

void mobject_store_write_op_zero(mobject_store_write_op_t write_op,
                                 uint64_t offset,
                                 uint64_t len)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	MOBJECT_ASSERT(!(write_op->ready), "can't modify a write_op that is ready to be processed");

	wr_action_zero_t action = (wr_action_zero_t)calloc(1, sizeof(*action));
	action->base.type       = WRITE_OPCODE_ZERO;
	action->offset          = offset;
	action->len             = len;
	
	WRITE_ACTION_UPCAST(base, action);
	DL_APPEND(write_op->actions, base);

	write_op->num_actions += 1;
}

void mobject_store_write_op_omap_set(mobject_store_write_op_t write_op,
                                     char const* const* keys,
                                     char const* const* vals,
                                     const size_t *lens,
                                     size_t num)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	MOBJECT_ASSERT(!(write_op->ready), "can't modify a write_op that is ready to be processed");

	// compute the size required to embed the keys and values
	size_t i;
	size_t extra_size = sizeof(lens[0])*num;
	for(i = 0; i < num; i++) {
		extra_size += strlen(keys[i])+1;
		extra_size += lens[i];
	}

	wr_action_omap_set_t action = (wr_action_omap_set_t)calloc(1, sizeof(*action)-1+extra_size);
	action->base.type           = WRITE_OPCODE_OMAP_SET;
	action->num                 = num;
	action->data_size           = extra_size;

	char* data = action->data;
	for(i = 0; i < num; i++) {
		// serialize key
		strcpy(data, keys[i]);
		data += strlen(keys[i])+1;
		// serialize size of value
		memcpy(data, &lens[i], sizeof(lens[i]));
		data += sizeof(lens[i]);
		// serialize value
		memcpy(data, vals[i], lens[i]);
		data += lens[i];
	}

	WRITE_ACTION_UPCAST(base, action);
	DL_APPEND(write_op->actions, base);

	write_op->num_actions += 1;
}

void mobject_store_write_op_omap_rm_keys(mobject_store_write_op_t write_op,
                                         char const* const* keys,
                                         size_t keys_len)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	MOBJECT_ASSERT(!(write_op->ready), "can't modify a write_op that is ready to be processed");

	// find out the extra memory to allocate
	size_t i;
	size_t extra_mem = 0;
	for(i = 0; i < keys_len; i++) {
		extra_mem += strlen(keys[i])+1;
	}

	wr_action_omap_rm_keys_t action = (wr_action_omap_rm_keys_t)calloc(1, sizeof(*action)-1+extra_mem);
	action->base.type               = WRITE_OPCODE_OMAP_RM_KEYS;
	action->num_keys                = keys_len;
	action->data_size               = extra_mem;

	char* data = action->data;
	// serialize the keys
	for(i = 0; i < keys_len; i++) {
		strcpy(data, keys[i]);
		data += strlen(keys[i])+1;
	}

	WRITE_ACTION_UPCAST(base, action);
	DL_APPEND(write_op->actions, base);

	write_op->num_actions += 1;
}

int mobject_store_write_op_operate(mobject_store_write_op_t write_op,
                                   mobject_store_ioctx_t io,
                                   const char *oid,
                                   time_t *mtime,
                                   int flags)
{
	int r;
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	mobject_store_completion_t completion = MOBJECT_COMPLETION_NULL;
	r = mobject_store_aio_create_completion(NULL, NULL, NULL, &completion);
	MOBJECT_ASSERT(0 == r, "Could not create completion object");
	r = mobject_store_aio_write_op_operate(write_op, io, completion, oid, mtime, flags);
	MOBJECT_ASSERT(0 == r, "Call to mobject_store_aio_write_op_operate failed");
	r = mobject_store_aio_wait_for_complete(completion);
	MOBJECT_ASSERT(0 == r, "Could not wait for completion");
	int ret = mobject_store_aio_get_return_value(completion);
	mobject_store_aio_release(completion);
	return ret;
}

int mobject_store_aio_write_op_operate(mobject_store_write_op_t write_op,
                                       mobject_store_ioctx_t io,
                                       mobject_store_completion_t completion,
                                       const char *oid,
                                       time_t *mtime,
                                       int flags)
{
	MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL, "invalid mobject_store_write_op_t obect");
	// TODO
}
