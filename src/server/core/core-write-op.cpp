#include <map>
#include <cstring>
#include <string>
#include <iostream>
#include <limits>
#include <bake-client.h>
#include "src/server/visitor-args.h"
#include "src/io-chain/write-op-visitor.h"
#include "src/server/core/fake-kv.hpp"

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

static oid_t get_or_create_oid(const char* name);
static void insert_region_log_entry(oid_t oid, uint64_t offset, uint64_t len, bake_region_id_t* region, double ts = -1.0);
static void insert_small_region_log_entry(oid_t oid, uint64_t offset, uint64_t len, const char* data, double ts = -1.0);
static void insert_zero_log_entry(oid_t oid, uint64_t offset, uint64_t len, double ts=-1.0);
static void insert_punch_log_entry(oid_t oid, uint64_t offset, double ts=-1.0);
static uint64_t compute_size(oid_t oid, double ts);

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
    get_or_create_oid(vargs->object_name);
}

void write_op_exec_write(void* u, buffer_u buf, size_t len, uint64_t offset)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    oid_t oid = get_or_create_oid(vargs->object_name);

    bake_provider_handle_t bake_ph = vargs->srv_ctx->bake_ph;
    bake_target_id_t bti = vargs->srv_ctx->bake_tid;
    bake_region_id_t rid;
    hg_bulk_t remote_bulk = vargs->bulk_handle;
    const char* remote_addr_str = vargs->client_addr_str;
    hg_addr_t   remote_addr     = vargs->client_addr;
    int ret;

    if(len > SMALL_REGION_THRESHOLD) {
        // TODO: check return values of those calls
        ret = bake_create(bake_ph, bti, len, &rid);
        ret = bake_proxy_write(bake_ph, rid, 0, remote_bulk, buf.as_offset, remote_addr_str, len);
        ret = bake_persist(bake_ph, rid);
   
        insert_region_log_entry(oid, offset, len, &rid);
    } else {
        margo_instance_id mid = vargs->srv_ctx->mid;
        char data[SMALL_REGION_THRESHOLD];
        void* buf_ptrs[1] = {(void*)(&data[0])};
        hg_size_t buf_sizes[1] = {len};
        hg_bulk_t handle;
        ret = margo_bulk_create(mid,1, buf_ptrs, buf_sizes, HG_BULK_WRITE_ONLY, &handle);
        ret = margo_bulk_transfer(mid, HG_BULK_PULL, remote_addr, remote_bulk, buf.as_offset, handle, 0, len);
        ret = margo_bulk_free(handle);

        insert_small_region_log_entry(oid, offset, len, data);
    }
}

void write_op_exec_write_full(void* u, buffer_u buf, size_t len)
{
    // TODO: this function will not be valid if the new object is
    // smaller than its previous version. Instead we should remove the object
    // and re-create it.

	auto vargs = static_cast<server_visitor_args_t>(u);
    oid_t oid = get_or_create_oid(vargs->object_name);

    bake_provider_handle_t bph = vargs->srv_ctx->bake_ph;
    bake_target_id_t bti = vargs->srv_ctx->bake_tid;
    bake_region_id_t rid;
    hg_bulk_t remote_bulk = vargs->bulk_handle;
    const char* remote_addr_str = vargs->client_addr_str;
    hg_addr_t   remote_addr     = vargs->client_addr;
    int ret;

    unsigned i;
    fprintf(stderr, "In Mobject, input bti is ");
    for(i=0; i<16; i++) fprintf(stderr, "%d ", bti.id[i]);
    fprintf(stderr, "\n");
    // TODO: check return values of those calls
    ret = bake_create(bph, bti, len, &rid);
    ret = bake_proxy_write(bph, rid, 0, remote_bulk, buf.as_offset, remote_addr_str, len);
    ret = bake_persist(bph, rid);
    insert_region_log_entry(oid, 0, len, &rid);
}

void write_op_exec_writesame(void* u, buffer_u buf, size_t data_len, size_t write_len, uint64_t offset)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
    oid_t oid = get_or_create_oid(vargs->object_name);

    bake_provider_handle_t bph = vargs->srv_ctx->bake_ph;
    bake_target_id_t bti = vargs->srv_ctx->bake_tid;
    bake_region_id_t rid;
    hg_bulk_t remote_bulk = vargs->bulk_handle;
    const char* remote_addr_str = vargs->client_addr_str;
    hg_addr_t   remote_addr     = vargs->client_addr;
    int ret;

    unsigned ii;
    fprintf(stderr, "In Mobject, input bti is ");
    for(ii=0; ii<16; ii++) fprintf(stderr, "%d ", bti.id[ii]);
    fprintf(stderr, "\n");

    // TODO: check return values of those calls
    ret = bake_create(bph, bti, data_len, &rid);
    ret = bake_proxy_write(bph, rid, 0, remote_bulk, buf.as_offset, remote_addr_str, data_len);
    ret = bake_persist(bph, rid);

    size_t i;

    //double ts = ABT_get_wtime();
    for(i=0; i < write_len; i += data_len) {
        segment_key_t seg;
        // TODO normally we should have the same timestamps but right now it bugs...
        insert_region_log_entry(oid, offset+i, std::min(data_len, write_len - i), &rid);//, ts);
    }
}

void write_op_exec_append(void* u, buffer_u buf, size_t len)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    oid_t oid = get_or_create_oid(vargs->object_name);

    bake_provider_handle_t bph = vargs->srv_ctx->bake_ph;
    bake_target_id_t bti = vargs->srv_ctx->bake_tid;
    bake_region_id_t rid;
    hg_bulk_t remote_bulk = vargs->bulk_handle;
    const char* remote_addr_str = vargs->client_addr_str;
    hg_addr_t   remote_addr     = vargs->client_addr;
    int ret;

    unsigned i;
    fprintf(stderr, "In Mobject, input bti is ");
    for(i=0; i<16; i++) fprintf(stderr, "%d ", bti.id[i]);
    fprintf(stderr, "\n");

    ret = bake_create(bph, bti, len, &rid);
    ret = bake_proxy_write(bph, rid, 0, remote_bulk, buf.as_offset, remote_addr_str, len);
    ret = bake_persist(bph, rid);

    // find out the current length of the object
    double ts = ABT_get_wtime();
    uint64_t offset = compute_size(oid,ts);
    
    insert_region_log_entry(oid, offset, len, &rid, ts);
}

void write_op_exec_remove(void* u)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    write_op_exec_truncate(u,0);
    // TODO: technically should mark the object as removed
}

void write_op_exec_truncate(void* u, uint64_t offset)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    oid_t oid = get_or_create_oid(vargs->object_name);

    insert_punch_log_entry(oid, offset);
}

void write_op_exec_zero(void* u, uint64_t offset, uint64_t len)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    oid_t oid = get_or_create_oid(vargs->object_name);

    insert_zero_log_entry(oid, offset, len);
}

void write_op_exec_omap_set(void* u, char const* const* keys,
                                     char const* const* vals,
                                     const size_t *lens,
                                     size_t num)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    oid_t oid = get_or_create_oid(vargs->object_name);

    for(auto i=0; i<num; i++) {
        std::vector<char> val(vals[i], vals[i]+lens[i]);
        omap_key_t omk;
        omk.oid = oid;
        omk.key = std::string(keys[i]);
        omap_map[std::move(omk)] = std::move(val);
    }
}

void write_op_exec_omap_rm_keys(void* u, char const* const* keys, size_t num_keys)
{
	auto vargs = static_cast<server_visitor_args_t>(u);
    oid_t oid = get_or_create_oid(vargs->object_name);
    
    for(auto i=0; i < num_keys; i++) {
        omap_key_t omk;
        omk.oid = oid;
        omk.key = std::string(keys[i]);
        omap_map.erase(omk);
    }
}

oid_t get_or_create_oid(const char* object_name) 
{
    oid_t oid = 0;
    std::string name(object_name);
    // check that the object exists, if not, create the object
    if(name_map.count(name) == 0) {
        std::hash<std::string> hash_fn;
        oid = hash_fn(name);
        while(oid_map.count(oid) != 0 || oid == 0) {
            oid += 1;
        }
        name_map[name] = oid;
        oid_map[oid]   = name;
    } else {
        oid = name_map[name];
    }
    return oid;
}

static void insert_region_log_entry(oid_t oid, uint64_t offset, uint64_t len, bake_region_id_t* region, double ts)
{
    segment_key_t seg;
    seg.oid       = oid;
    seg.timestamp = ts < 0 ? ABT_get_wtime() : ts;
    seg.start_index    = offset;
    seg.end_index      = offset+len;
    seg.type      = seg_type_t::BAKE_REGION;
    segment_map[seg] = *region;
}

static void insert_small_region_log_entry(oid_t oid, uint64_t offset, uint64_t len, const char* data, double ts)
{
    segment_key_t seg;
    seg.oid       = oid;
    seg.timestamp = ts < 0 ? ABT_get_wtime() : ts;
    seg.start_index    = offset;
    seg.end_index      = offset+len;
    seg.type      = seg_type_t::SMALL_REGION;
    void* b = static_cast<void*>(&segment_map[seg]);
    std::memcpy(b, data, len);
}

static void insert_zero_log_entry(oid_t oid, uint64_t offset, uint64_t len, double ts)
{
    segment_key_t seg;
    seg.oid       = oid;
    seg.timestamp = ts < 0 ? ABT_get_wtime() : ts;
    seg.start_index    = offset;
    seg.end_index      = offset+len;
    seg.type      = seg_type_t::ZERO;
    segment_map[seg] = bake_region_id_t();
}

static void insert_punch_log_entry(oid_t oid, uint64_t offset, double ts)
{
    segment_key_t seg;
    seg.oid       = oid;
    seg.timestamp = ts < 0 ? ABT_get_wtime() : ts;
    seg.start_index = offset;
    seg.end_index  = std::numeric_limits<uint64_t>::max();
    seg.type      = seg_type_t::TOMBSTONE;
    segment_map[seg] = bake_region_id_t();
}

uint64_t compute_size(oid_t oid, double ts)
{
    segment_key_t lb;
    lb.oid = oid;
    lb.timestamp = ts;
    uint64_t size = 0;
    auto it = segment_map.lower_bound(lb);
    for(; it != segment_map.end(); it++) {
        if(it->first.oid != oid) break;
            auto& seg = it->first;
            if(seg.type < seg_type_t::TOMBSTONE) {
                if(size < seg.end_index) size = seg.end_index;
            } else if(seg.type == seg_type_t::TOMBSTONE) {
                if(size < seg.end_index) size = seg.start_index;
                break;
            }
    }
    return size;
}
