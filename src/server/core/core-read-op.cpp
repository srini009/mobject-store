#include <map>
#include <string>
#include <iostream>
#include "src/server/core/core-read-op.h"
#include "src/server/visitor-args.h"
#include "src/io-chain/read-op-visitor.h"
#include "src/io-chain/read-resp-impl.h"
#include "src/omap-iter/omap-iter-impl.h"

static void read_op_exec_begin(void*);
static void read_op_exec_stat(void*, uint64_t*, time_t*, int*);
static void read_op_exec_read(void*, uint64_t, size_t, buffer_u, size_t*, int*);
static void read_op_exec_omap_get_keys(void*, const char*, uint64_t, mobject_store_omap_iter_t*, int*);
static void read_op_exec_omap_get_vals(void*, const char*, const char*, uint64_t, mobject_store_omap_iter_t*, int*);
static void read_op_exec_omap_get_vals_by_keys(void*, char const* const*, size_t, mobject_store_omap_iter_t*, int*);
static void read_op_exec_end(void*);

static struct read_op_visitor read_op_exec = {
	.visit_begin                 = read_op_exec_begin,
	.visit_stat                  = read_op_exec_stat,
	.visit_read                  = read_op_exec_read,
	.visit_omap_get_keys         = read_op_exec_omap_get_keys,
	.visit_omap_get_vals         = read_op_exec_omap_get_vals,
	.visit_omap_get_vals_by_keys = read_op_exec_omap_get_vals_by_keys,
	.visit_end                   = read_op_exec_end
};

extern "C" void core_read_op(mobject_store_read_op_t read_op, server_visitor_args_t vargs)
{
	execute_read_op_visitor(&read_op_exec, read_op, (void*)vargs);
}

void read_op_exec_begin(void* u)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
}

void read_op_exec_stat(void* u, uint64_t* psize, time_t* pmtime, int* prval)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
}

void read_op_exec_read(void* u, uint64_t offset, size_t len, buffer_u buf, size_t* bytes_read, int* prval)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
}

void read_op_exec_omap_get_keys(void* u, const char* start_after, uint64_t max_return, 
				mobject_store_omap_iter_t* iter, int* prval)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
    omap_iter_create(iter);
}

void read_op_exec_omap_get_vals(void* u, const char* start_after, const char* filter_prefix, uint64_t max_return, mobject_store_omap_iter_t* iter, int* prval)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
    omap_iter_create(iter);
}

void read_op_exec_omap_get_vals_by_keys(void* u, char const* const* keys, size_t num_keys, mobject_store_omap_iter_t* iter, int* prval)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
    omap_iter_create(iter);
}

void read_op_exec_end(void* u)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
}
