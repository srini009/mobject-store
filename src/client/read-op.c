/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include <string.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/io-chain/read-op-impl.h"
#include "src/util/utlist.h"
#include "src/util/log.h"

mobject_store_read_op_t mobject_store_create_read_op(void)
{
	return create_read_op();
}

void mobject_store_release_read_op(mobject_store_read_op_t read_op)
{
    release_read_op(read_op);
}

void mobject_store_read_op_stat(mobject_store_read_op_t read_op,
                                uint64_t *psize,
                                time_t *pmtime,
                                int *prval)
{
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");
	MOBJECT_ASSERT(!(read_op->ready), "can't modify a read_op that is ready to be processed");

	rd_action_stat_t action = (rd_action_stat_t)calloc(1, sizeof(*action));
	action->base.type       = READ_OPCODE_STAT;
	action->psize           = psize;
	action->pmtime          = pmtime;
    action->prval           = prval;

	READ_ACTION_UPCAST(base, action);
	DL_APPEND(read_op->actions, base);

	read_op->num_actions += 1;
}

void mobject_store_read_op_read(mobject_store_read_op_t read_op,
                                uint64_t offset,
                                size_t len,
                                char *buffer,
                                size_t *bytes_read,
                                int *prval)
{
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");
	MOBJECT_ASSERT(!(read_op->ready), "can't modify a read_op that is ready to be processed");

	rd_action_read_t action   = (rd_action_read_t)calloc(1, sizeof(*action));
	action->base.type         = READ_OPCODE_READ;
	action->offset            = offset;
	action->len               = len;
	action->buffer.as_pointer = buffer;
	action->bytes_read        = bytes_read;
	action->prval             = prval;

	READ_ACTION_UPCAST(base, action);
	DL_APPEND(read_op->actions, base);

	read_op->num_actions += 1;
}

void mobject_store_read_op_omap_get_keys(mobject_store_read_op_t read_op,
				                         const char *start_after,
				                         uint64_t max_return,
				                         mobject_store_omap_iter_t *iter,
                                         int *prval)
{
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");
	MOBJECT_ASSERT(!(read_op->ready), "can't modify a read_op that is ready to be processed");

	size_t strl = strlen(start_after);
	
	rd_action_omap_get_keys_t action = (rd_action_omap_get_keys_t)calloc(1, sizeof(*action)+strl);
	action->base.type                = READ_OPCODE_OMAP_GET_KEYS;
	action->start_after              = action->data;
	action->max_return               = max_return;
	action->iter                     = iter;
	action->prval                    = prval;
	action->data_size                = strl+1;
	strcpy(action->data, start_after);

	READ_ACTION_UPCAST(base, action);
    DL_APPEND(read_op->actions, base);

	read_op->num_actions += 1;
}

void mobject_store_read_op_omap_get_vals(mobject_store_read_op_t read_op,
                                         const char *start_after,
                                         const char *filter_prefix,
                                         uint64_t max_return,
                                         mobject_store_omap_iter_t *iter,
                                         int *prval)
{
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");
	MOBJECT_ASSERT(!(read_op->ready), "can't modify a read_op that is ready to be processed");

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
	action->data_size                = extra_mem;
	strcpy(action->data, start_after);
	strcpy(action->data + strl1, filter_prefix);

	READ_ACTION_UPCAST(base, action);
	DL_APPEND(read_op->actions, base);

	read_op->num_actions += 1;
}

void mobject_store_read_op_omap_get_vals_by_keys(mobject_store_read_op_t read_op,
                                                 char const* const* keys,
                                                 size_t keys_len,
                                                 mobject_store_omap_iter_t *iter,
                                                 int *prval)
{
	MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t obect");
	MOBJECT_ASSERT(!(read_op->ready), "can't modify a read_op that is ready to be processed");
	
	// computing extra memory required to hold keys
	size_t extra_mem = 0;
	size_t i;
	for(i = 0; i < keys_len; i++) {
		extra_mem += strlen(keys[i]) + 1;
	}

	rd_action_omap_get_vals_by_keys_t action =
		(rd_action_omap_get_vals_by_keys_t)calloc(1, sizeof(*action) - 1 + extra_mem);
	action->base.type = READ_OPCODE_OMAP_GET_VALS_BY_KEYS;
	action->num_keys  = keys_len;
	action->iter      = iter;
	action->prval     = prval;
	action->data_size = extra_mem;
	char* s = action->data;
	for(i = 0; i < keys_len; i++) {
		strcpy(s, keys[i]);
		s += strlen(keys[i]) + 1;
	}
	
	READ_ACTION_UPCAST(base, action);
	DL_APPEND(read_op->actions, base);

	read_op->num_actions += 1;
}

/*
typedef struct read_op_ult_args {
    mobject_store_read_op_t    read_op;
    mobject_store_ioctx_t      ioctx;
    mobject_store_completion_t completion
    char*                      oid;
    int                        flags;
} read_op_ult_args;

static void aio_read_op_operate_ult(read_op_ult_args* args) {

    read_op_in_t in;
    in.object_name = args->oid;
    in.pool_name   = args->ioctx->pool_name;
    in.read_op     = args->read_op;

    prepare_read_op(io->mid, read_op);

    // TODO: svr_addr should be computed based on the pool name, object name,
    // and SSG structures accessible via the io context
    hg_handle_t h;
    margo_create(io->mid, io->svr_addr, mobject_read_op_rpc_id, &h);
    margo_forward(h, &in);

    read_op_out_t resp;
    margo_get_output(h, &resp);

    feed_read_op_pointers_from_response(read_op, resp.responses);

    margo_free_output(h,&resp);
    margo_destroy(h);

    free(args->oid);
    
    ABT_rwlock_wrlock(args->completion->lock);
    int ret = 0; // TODO change that depending on results of the read_op
    ABT_eventual_set (args->completion->eventual, &ret, sizeof(int));
    mobject_store_callback_t cb_complete = args->completion->cb_complete;
    void* cb_arg = args->completion->cb_arg;
    ABT_rwlock_unlock(args->completion->lock);

    if(complete_cb)
        complete_cb(args->completion, cb_arg);

    free(args);

    return 0;
}
*/
int mobject_store_aio_read_op_operate(mobject_store_read_op_t read_op,
                                      mobject_store_ioctx_t io,
                                      mobject_store_completion_t completion,
                                      const char *oid,
                                      int flags)
{
/*    MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL, "invalid mobject_store_read_op_t object");
    // TODO this is not great, we should use the margo non-blocking API instead
    ABT_xstream self_es;
    ABT_xstream_self(&self_es);
    ABT_pool pool;
    ABT_xstream_get_main_pools(self_es, 1, &pool);
    ABT_thread ult;
    read_op_ult_args* args = (read_op_ult_args*)calloc(1, sizeof(*args);
    args->read_op          = read_op;
    args->ioctx            = io;
    args->completion       = completion;
    args->oid              = strdup(oid);
    args->flags            = flags;
    ABT_thread_create(pool, aio_read_op_operate_ult, args, ABT_THREAD_ATTR_NULL, &ult);
    completion->ult        = ult;
*/
}
