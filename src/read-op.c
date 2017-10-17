/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include <string.h>
#include "mobject-store-config.h"
#include "utlist.h"
#include "libmobject-store.h"
#include "log.h"
#include "read-op.h"
#include "completion.h"

mobject_store_read_op_t mobject_store_create_read_op(void)
{
	mobject_store_read_op_t read_op = 
		(mobject_store_read_op_t)calloc(1, sizeof(*read_op));
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "Could not allocate read_op");
	read_op->actions = (rd_action_base_t)0;
	return read_op;
}

void mobject_store_release_read_op(mobject_store_read_op_t read_op)
{
	if(read_op == MOBJECT_READ_OP_NULL) return;
	
	rd_action_base_t action, tmp;

	DL_FOREACH_SAFE(read_op->actions, action, tmp) {
		DL_DELETE(read_op->actions, action);
		free(action);
	}

	free(read_op);
}

void mobject_store_read_op_stat(mobject_store_read_op_t read_op,
                                uint64_t *psize,
                                time_t *pmtime,
                                int *prval)
{
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");

	rd_action_stat_t action = (rd_action_stat_t)calloc(1, sizeof(*action));
	action->base.type       = READ_OPCODE_STAT;
	action->psize           = psize;
	action->pmtime          = pmtime;
    action->prval           = prval;

	READ_ACTION_UPCAST(base, action);
	DL_APPEND(read_op->actions, base);
}

void mobject_store_read_op_read(mobject_store_read_op_t read_op,
                                uint64_t offset,
                                size_t len,
                                char *buffer,
                                size_t *bytes_read,
                                int *prval)
{
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");

	rd_action_read_t action = (rd_action_read_t)calloc(1, sizeof(*action));
	action->base.type       = READ_OPCODE_READ;
	action->offset          = offset;
	action->len             = len;
	action->buffer          = buffer;
	action->bytes_read      = bytes_read;
	action->prval           = prval;

	READ_ACTION_UPCAST(base, action);
	DL_APPEND(read_op->actions, base);
}

void mobject_store_read_op_omap_get_vals(mobject_store_read_op_t read_op,
                                         const char *start_after,
                                         const char *filter_prefix,
                                         uint64_t max_return,
                                         mobject_store_omap_iter_t *iter,
                                         int *prval)
{
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");

	// compute required size for embedded data
	size_t strl1 = strlen(start_after)+1;
	size_t strl2 = strlen(filter_prefix)+1;
	size_t extra_mem = strl1+strl2;

	rd_action_omap_get_vals_t action = (rd_action_omap_get_vals_t)calloc(1, sizeof(*action)-1+extra_mem);
	action->base.type                = READ_OPCODE_OMAP_GET_VALS;
	action->start_after              = action->data;
	action->filter_prefix            = action->data + strl1;
	action->max_return               = max_return;
	action->iter                     = iter;
	action->prval                    = prval;
	strcpy(action->data, start_after);
	strcpy(action->data + strl1, filter_prefix);

	READ_ACTION_UPCAST(base, action);
	DL_APPEND(read_op->actions, base);
}

void mobject_store_read_op_omap_get_vals_by_keys(mobject_store_read_op_t read_op,
                                                 char const* const* keys,
                                                 size_t keys_len,
                                                 mobject_store_omap_iter_t *iter,
                                                 int *prval)
{
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");
	
	// computing extra memory required to hold keys
	size_t extra_mem = 0;
	size_t i;
	for(i = 0; i < keys_len; i++) {
		extra_mem += strlen(keys[i]) + 1;
	}

	rd_action_omap_get_vals_by_keys_t action =
		(rd_action_omap_get_vals_by_keys_t)calloc(1, sizeof(*action) - 1 + extra_mem);
	action->base.type = READ_OPCODE_OMAP_GET_VALS_BY_KEYS;
	action->keys_len  = keys_len;
	action->iter      = iter;
	action->prval     = prval;
	char* s = action->keys;
	for(i = 0; i < keys_len; i++) {
		strcpy(s, keys[i]);
		s += strlen(keys[i]) + 1;
	}
	
	READ_ACTION_UPCAST(base, action);
	DL_APPEND(read_op->actions, base);
}

int mobject_store_read_op_operate(mobject_store_read_op_t read_op,
                                  mobject_store_ioctx_t io,
                                  const char *oid,
                                  int flags)
{
	int r;
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");
	mobject_store_completion_t completion = MOBJECT_COMPLETION_NULL;
	r = mobject_store_aio_create_completion(NULL, NULL, NULL, &completion);
	MOBJECT_ASSERT(0 == r, "Could not create completion object");
	r = mobject_store_aio_read_op_operate(read_op, io, completion, oid, flags);
	MOBJECT_ASSERT(0 == r, "Call to mobject_store_aio_read_op_operate failed");
	r = mobject_store_aio_wait_for_complete(completion);
	MOBJECT_ASSERT(0 == r, "Could not wait for completion");
	int ret = mobject_store_aio_get_return_value(completion);
	mobject_store_aio_release(completion);
	return ret;
}

int mobject_store_aio_read_op_operate(mobject_store_read_op_t read_op,
                                      mobject_store_ioctx_t io,
                                      mobject_store_completion_t completion,
                                      const char *oid,
                                      int flags)
{
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");
	// TODO
}

