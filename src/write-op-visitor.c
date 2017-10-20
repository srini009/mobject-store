/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "libmobject-store.h"
#include "write-op-visitor.h"
#include "write-op-impl.h"
#include "utlist.h"

static void execute_write_op_visitor_on_create(write_op_visitor_t visitor, wr_action_create_t a);
static void execute_write_op_visitor_on_write(write_op_visitor_t visitor, wr_action_write_t a);
static void execute_write_op_visitor_on_write_full(write_op_visitor_t visitor, wr_action_write_full_t a);
static void execute_write_op_visitor_on_write_same(write_op_visitor_t visitor, wr_action_write_same_t a);
static void execute_write_op_visitor_on_append(write_op_visitor_t visitor, wr_action_append_t a);
static void execute_write_op_visitor_on_remove(write_op_visitor_t visitor, wr_action_remove_t a);
static void execute_write_op_visitor_on_truncate(write_op_visitor_t visitor, wr_action_truncate_t a);
static void execute_write_op_visitor_on_zero(write_op_visitor_t visitor, wr_action_zero_t a);
static void execute_write_op_visitor_on_omap_set(write_op_visitor_t visitor, wr_action_omap_set_t a);
static void execute_write_op_visitor_on_omap_rm_keys(write_op_visitor_t visitor, wr_action_omap_rm_keys_t a);

typedef void (*dispatch_fn)(write_op_visitor_t, wr_action_base_t);

static dispatch_fn visitor_dispatch[_WRITE_OPCODE_END_ENUM_] = {
	NULL,
	(dispatch_fn)execute_write_op_visitor_on_create,
	(dispatch_fn)execute_write_op_visitor_on_write,
	(dispatch_fn)execute_write_op_visitor_on_write_full,
	(dispatch_fn)execute_write_op_visitor_on_write_same,
	(dispatch_fn)execute_write_op_visitor_on_append,
	(dispatch_fn)execute_write_op_visitor_on_remove,
	(dispatch_fn)execute_write_op_visitor_on_truncate,
	(dispatch_fn)execute_write_op_visitor_on_zero,
	(dispatch_fn)execute_write_op_visitor_on_omap_set,
	(dispatch_fn)execute_write_op_visitor_on_omap_rm_keys
};

void execute_write_op_visitor(write_op_visitor_t visitor, mobject_store_write_op_t write_op)
{
	void* uargs = visitor->uargs;
	wr_action_base_t a;

	visitor->visit_begin(uargs);
	
	DL_FOREACH((write_op->actions), a) {
		visitor_dispatch[a->type](visitor, a);
	}

	visitor->visit_end(uargs);
}

////////////////////////////////////////////////////////////////////////////////
//                          STATIC FUNCTIONS BELOW                            //
////////////////////////////////////////////////////////////////////////////////

static void execute_write_op_visitor_on_create(write_op_visitor_t visitor, wr_action_create_t a)
{
	visitor->visit_create(visitor->uargs, a->exclusive);
}

static void execute_write_op_visitor_on_write(write_op_visitor_t visitor, wr_action_write_t a)
{
	visitor->visit_write(visitor->uargs, a->buffer, a->len, a->offset);
}

static void execute_write_op_visitor_on_write_full(write_op_visitor_t visitor, wr_action_write_full_t a)
{
	visitor->visit_write_full(visitor->uargs, a->buffer, a->len);
}

static void execute_write_op_visitor_on_write_same(write_op_visitor_t visitor, wr_action_write_same_t a)
{
	visitor->visit_writesame(visitor->uargs, a->buffer, a->data_len, a->write_len, a->offset);
}

static void execute_write_op_visitor_on_append(write_op_visitor_t visitor, wr_action_append_t a)
{
	visitor->visit_append(visitor->uargs, a->buffer, a->len);
}

static void execute_write_op_visitor_on_remove(write_op_visitor_t visitor, wr_action_remove_t a)
{
	visitor->visit_remove(visitor->uargs);
}

static void execute_write_op_visitor_on_truncate(write_op_visitor_t visitor, wr_action_truncate_t a)
{
	visitor->visit_truncate(visitor->uargs, a->offset);
}

static void execute_write_op_visitor_on_zero(write_op_visitor_t visitor, wr_action_zero_t a)
{
	visitor->visit_zero(visitor->uargs, a->offset, a->len);
}

static void execute_write_op_visitor_on_omap_set(write_op_visitor_t visitor, wr_action_omap_set_t a)
{
	size_t num = a->num;
	size_t lens[num];
	const char* keys[num];
	const char* vals[num];

	unsigned i;
	const char* ptr = a->data;
	for(i=0; i < num; i++) {
		// ptr is pointing to a key now
		keys[i] = ptr;
		// get size of key
		size_t s = strlen(keys[i])+1;
		// jump pointer right after key
		ptr += s;
		// ptr is pointing to a length of data, now
		memcpy(lens+i, ptr, sizeof(size_t));
		ptr += sizeof(size_t);
		vals[i] = ptr;
		ptr += lens[i];
	}

	visitor->visit_omap_set(visitor->uargs, keys, vals, lens, num);
}

static void execute_write_op_visitor_on_omap_rm_keys(write_op_visitor_t visitor, wr_action_omap_rm_keys_t a)
{
	size_t num_keys = a->num_keys;
	const char* keys[num_keys];

	unsigned i;
	const char* ptr = a->data;
	for(i=0; i < num_keys; i++) {
		keys[i] = ptr;
		ptr += strlen(keys[i])+1;
	}

	visitor->visit_omap_rm_keys(visitor->uargs, keys, num_keys);
}
