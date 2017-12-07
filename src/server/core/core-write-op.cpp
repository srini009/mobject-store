#include <map>
#include <string>
#include <iostream>
#include <bake-bulk-client.h>
#include "src/server/visitor-args.h"
#include "src/io-chain/write-op-visitor.h"

static void write_op_exec_begin(void*);
static void write_op_exec_end(void*);
static void write_op_exec_create(void*, int);
static void write_op_exec_write(void*, buffer_u, size_t, uint64_t);
static void write_op_exec_write_full(void*, buffer_u, size_t);
static void write_op_exec_writesame(void*, buffer_u, size_t, size_t, uint64_t);
static void write_op_exec_append(void*, buffer_u, size_t);
static void write_op_exec_remove(void*);
static void write_op_exec_truncate(void*, uint64_t);
static void write_op_exec_zero(void*, uint64_t, uint64_t);
static void write_op_exec_omap_set(void*, char const* const*, char const* const*, const size_t*, size_t);
static void write_op_exec_omap_rm_keys(void*, char const* const*, size_t);

static struct write_op_visitor write_op_exec = {
	.visit_begin        = write_op_exec_begin,
	.visit_create       = write_op_exec_create,
	.visit_write        = write_op_exec_write,
	.visit_write_full   = write_op_exec_write_full,
	.visit_writesame    = write_op_exec_writesame,
	.visit_append       = write_op_exec_append,
	.visit_remove       = write_op_exec_remove,
	.visit_truncate     = write_op_exec_truncate,
	.visit_zero         = write_op_exec_zero,
	.visit_omap_set     = write_op_exec_omap_set,
	.visit_omap_rm_keys = write_op_exec_omap_rm_keys,
	.visit_end          = write_op_exec_end
};

extern "C" void core_write_op(mobject_store_write_op_t write_op, server_visitor_args_t vargs)
{
	/* Execute the operation chain */
	execute_write_op_visitor(&write_op_exec, write_op, (void*)vargs);
}

void write_op_exec_begin(void* u)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
}

void write_op_exec_end(void* u)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
}

void write_op_exec_create(void* u, int exclusive)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
}

void write_op_exec_write(void* u, buffer_u buf, size_t len, uint64_t offset)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    bake_target_id_t bti = vargs->srv_ctx->bake_id;
    bake_bulk_region_id_t rid;
    hg_bulk_t remote_bulk = vargs->bulk_handle;
    const char* remote_addr = vargs->client_addr.as_string;
    int ret;

    // TODO: check return values of those calls
    ret = bake_bulk_create(bti, len, &rid);
    ret = bake_bulk_proxy_write(bti, rid, 0, remote_bulk, buf.as_offset, remote_addr, len);
    ret = bake_bulk_persist(bti, rid);
   
    // TODO: write [offset,len,rid] in sds-keyval for the specified object
}

void write_op_exec_write_full(void* u, buffer_u buf, size_t len)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    bake_target_id_t bti = vargs->srv_ctx->bake_id;
    bake_bulk_region_id_t rid;
    hg_bulk_t remote_bulk = vargs->bulk_handle;
    const char* remote_addr = vargs->client_addr.as_string;
    int ret;

    // TODO: check return values of those calls
    ret = bake_bulk_create(bti, len, &rid);
    ret = bake_bulk_proxy_write(bti, rid, 0, remote_bulk, buf.as_offset, remote_addr, len);
    ret = bake_bulk_persist(bti, rid);

    // TODO: write [offset,len,rid] in sds-keyval for the specified object
}

void write_op_exec_writesame(void* u, buffer_u buf, size_t data_len, size_t write_len, uint64_t offset)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    bake_target_id_t bti = vargs->srv_ctx->bake_id;
    bake_bulk_region_id_t rid;
    hg_bulk_t remote_bulk = vargs->bulk_handle;
    const char* remote_addr = vargs->client_addr.as_string;
    int ret;

    // TODO: check return values of those calls
    ret = bake_bulk_create(bti, data_len, &rid);
    ret = bake_bulk_proxy_write(bti, rid, 0, remote_bulk, buf.as_offset, remote_addr, data_len);
    ret = bake_bulk_persist(bti, rid);

    // TODO: write [offset,len,rid] in sds-keyval for the specified object
}

void write_op_exec_append(void* u, buffer_u buf, size_t len)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    bake_target_id_t bti = vargs->srv_ctx->bake_id;
    bake_bulk_region_id_t rid;
    hg_bulk_t remote_bulk = vargs->bulk_handle;
    const char* remote_addr = vargs->client_addr.as_string;
    int ret;

    // TODO: check return values of those calls
    ret = bake_bulk_create(bti, len, &rid);
    ret = bake_bulk_proxy_write(bti, rid, 0, remote_bulk, buf.as_offset, remote_addr, len);
    ret = bake_bulk_persist(bti, rid);

    // TODO: write [offset,len,rid] in sds-keyval for the specified object
}

void write_op_exec_remove(void* u)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
}

void write_op_exec_truncate(void* u, uint64_t offset)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
}

void write_op_exec_zero(void* u, uint64_t offset, uint64_t len)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
}

void write_op_exec_omap_set(void* u, char const* const* keys,
                                     char const* const* vals,
                                     const size_t *lens,
                                     size_t num)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
}

void write_op_exec_omap_rm_keys(void* u, char const* const* keys, size_t num_keys)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
}

