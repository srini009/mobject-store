#include <map>
#include <string>
#include <iostream>
#include <algorithm>
#include <list>
#include <bake-bulk-client.h>
#include "src/server/core/core-read-op.h"
#include "src/server/visitor-args.h"
#include "src/io-chain/read-op-visitor.h"
#include "src/io-chain/read-resp-impl.h"
#include "src/omap-iter/omap-iter-impl.h"
#include "src/server/core/fake-kv.hpp"
#include "src/server/core/covermap.hpp"

static void read_op_exec_begin(void*);
static void read_op_exec_stat(void*, uint64_t*, time_t*, int*);
static void read_op_exec_read(void*, uint64_t, size_t, buffer_u, size_t*, int*);
static void read_op_exec_omap_get_keys(void*, const char*, uint64_t, mobject_store_omap_iter_t*, int*);
static void read_op_exec_omap_get_vals(void*, const char*, const char*, uint64_t, mobject_store_omap_iter_t*, int*);
static void read_op_exec_omap_get_vals_by_keys(void*, char const* const*, size_t, mobject_store_omap_iter_t*, int*);
static void read_op_exec_end(void*);

struct read_request_t {
    double timestamp;              // timestamp at which the segment was created
    uint64_t absolute_start_index; // start index within the object
    uint64_t absolute_end_index;   // end index within the object
    uint64_t region_start_index;   // where to start within the region
    uint64_t region_end_index;     // where to end within the region
    uint64_t client_offset;        // offset within the client's buffer
    bake_bulk_region_id_t region;  // region id
};

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
    bake_target_id_t bti = vargs->srv_ctx->bake_id;
    bake_bulk_region_id_t rid;
    hg_bulk_t remote_bulk = vargs->bulk_handle;
    const char* object_name = vargs->object_name;
    const char* remote_addr_str = vargs->client_addr_str;
    hg_addr_t   remote_addr     = vargs->client_addr;
    int ret;

    uint64_t client_start_index = offset;
    uint64_t client_end_index = offset+len;

    *prval = 0;

    // find oid
    if(name_map.count(object_name) == 0) {
        *prval = -1;
        return;
    }
    oid_t oid = name_map[object_name];

    segment_key_t lb;
    lb.oid = oid;
    lb.timestamp = std::numeric_limits<double>::max();
    auto it = segment_map.lower_bound(lb);
    covermap<uint64_t> coverage(offset, offset+len);

    while(!coverage.full() && it != segment_map.end() && it->first.oid == oid) {
        const segment_key_t& seg = it->first;
        const bake_bulk_region_id_t& region = it->second;
        
        switch(seg.type) {
        case seg_type_t::ZERO:
            coverage.set(seg.start_index, seg.end_index);
            if(*bytes_read < seg.end_index) *bytes_read = seg.end_index;
            break;
        case seg_type_t::TOMBSTONE:
            coverage.set(seg.start_index, seg.end_index);
            if(*bytes_read < seg.start_index) *bytes_read = seg.start_index;
            break;
        case seg_type_t::BAKE_REGION: {
            auto ranges = coverage.set(seg.start_index, seg.end_index);
            for(auto r : ranges) {
                uint64_t segment_size  = r.end - r.start;
                uint64_t region_offset = r.start - seg.start_index;
                uint64_t remote_offset = r.start - offset;
                bake_bulk_proxy_read(bti, region, region_offset, remote_bulk,
                        remote_offset, remote_addr_str, segment_size);
                if(*bytes_read < r.end) *bytes_read = r.end;
            }
            break;
          }
        case seg_type_t::SMALL_REGION: {
            auto ranges = coverage.set(seg.start_index, seg.end_index);
            const char* base = static_cast<const char*>((void*)(&region));
            margo_instance_id mid = vargs->srv_ctx->mid;
            for(auto r : ranges) {
                uint64_t segment_size  = r.end - r.start;
                uint64_t region_offset = r.start - seg.start_index;
                uint64_t remote_offset = r.start - offset;
                void* buf_ptrs[1] = { const_cast<char*>(base + region_offset) };
                hg_size_t buf_sizes[1] = { segment_size };
                hg_bulk_t handle;
                std::cout << "Reading from a small region" << std::endl;
                ret = margo_bulk_create(mid,1, buf_ptrs, buf_sizes, HG_BULK_READ_ONLY, &handle);
                ret = margo_bulk_transfer(mid, HG_BULK_PUSH, remote_addr, remote_bulk, buf.as_offset+remote_offset, handle, 0, segment_size);
                ret = margo_bulk_free(handle);
                if(*bytes_read < r.end) *bytes_read = r.end;
            }
          }
        }
        it++;
    }
}

void read_op_exec_omap_get_keys(void* u, const char* start_after, uint64_t max_return, 
				mobject_store_omap_iter_t* iter, int* prval)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
    const char* object_name = vargs->object_name;

    *prval = 0;

    // find oid
    if(name_map.count(object_name) == 0) {
        *prval = -1;
        return;
    }
    oid_t oid = name_map[object_name];

    omap_iter_create(iter);
    omap_key_t lb;
    lb.oid = oid;
    lb.key = std::string(start_after);

    auto it = omap_map.lower_bound(lb);
    if(it == omap_map.end()) return;
    if(it->first.key == lb.key) it++;

    for(uint64_t i=0; it != omap_map.end() && i < max_return; it++, i++) {
        omap_iter_append(*iter, it->first.key.c_str(), nullptr, 0);
    }
}

void read_op_exec_omap_get_vals(void* u, const char* start_after, const char* filter_prefix, uint64_t max_return, mobject_store_omap_iter_t* iter, int* prval)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
    const char* object_name = vargs->object_name;

    *prval = 0;

    // find oid
    if(name_map.count(object_name) == 0) {
        *prval = -1;
        return;
    }
    oid_t oid = name_map[object_name];

    omap_iter_create(iter);
    omap_key_t lb;
    lb.oid = oid;
    lb.key = std::string(start_after);

    auto it = omap_map.lower_bound(lb);
    if(it == omap_map.end()) return;
    if(it->first.key == lb.key) it++;

    std::string prefix(filter_prefix);
    for(uint64_t i=0; it != omap_map.end() && i < max_return; it++, i++) {
        const std::string& k = it->first.key;
        if(k.compare(0, prefix.size(), prefix) == 0) {
                omap_iter_append(*iter, k.c_str(), &(it->second[0]), it->second.size());
        }
    }
}

void read_op_exec_omap_get_vals_by_keys(void* u, char const* const* keys, size_t num_keys, mobject_store_omap_iter_t* iter, int* prval)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
    const char* object_name = vargs->object_name;

    *prval = 0;

    // find oid
    if(name_map.count(object_name) == 0) {
        *prval = -1;
        return;
    }
    oid_t oid = name_map[object_name];

    omap_iter_create(iter);

    omap_key_t key;
    key.oid = oid;

    for(size_t i=0; i<num_keys; i++) {
        key.key = keys[i];
        auto it = omap_map.find(key);
        if(it != omap_map.end()) {
             omap_iter_append(*iter, keys[i], &(it->second[0]), it->second.size());
        }
    }
}

void read_op_exec_end(void* u)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
}
