#include <map>
#include <string>
#include <iostream>
#include <algorithm>
#include <list>
#include <bake-client.h>
#include "src/server/core/core-read-op.h"
#include "src/server/visitor-args.h"
#include "src/io-chain/read-op-visitor.h"
#include "src/io-chain/read-resp-impl.h"
#include "src/omap-iter/omap-iter-impl.h"
#include "src/server/core/key-types.h"
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
    bake_region_id_t region;  // region id
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
    bake_provider_handle_t bph = vargs->srv_ctx->bake_ph;
    bake_target_id_t bti = vargs->srv_ctx->bake_tid;
    bake_region_id_t rid;
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
        const bake_region_id_t& region = it->second;
        
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
                bake_proxy_read(bph, region, region_offset, remote_bulk,
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
    sdskv_provider_handle_t sdskv_ph = vargs->srv_ctx->sdskv_ph;
    sdskv_database_id_t omap_db_id = vargs->srv_ctx->omap_db_id;
    int ret;
    *prval = 0;

    // find oid
    if(name_map.count(object_name) == 0) {
        *prval = -1;
        return;
    }
    oid_t oid = name_map[object_name];
    
    omap_iter_create(iter);
    size_t lb_size = sizeof(omap_key_t)+strlen(start_after);
    omap_key_t* lb = (omap_key_t*)calloc(1, lb_size);
    lb->oid = oid;
    strcpy(lb->key, start_after);

    hg_size_t max_keys = 10;
    hg_size_t key_len  = 128; 
    std::vector<void*> keys(max_keys);
    std::vector<hg_size_t> ksizes(max_keys, key_len);
    std::vector<std::vector<char>> buffers(max_keys, std::vector<char>(key_len));
    for(auto i=0; i < max_keys; i++) keys[i] = (void*)buffers[i].data();

    hg_size_t keys_retrieved = max_keys;
    hg_size_t count = 0;
    do {
        ret = sdskv_list_keys(sdskv_ph, omap_db_id,
                (const void*)lb, lb_size,
                keys.data(), ksizes.data(),
                &keys_retrieved);
        if(ret != SDSKV_SUCCESS) {
            *prval = -1;
            break;
        }
        const char* k;
        for(auto i = 0; i < keys_retrieved && count < max_return; i++, count++) {
            // extract the actual key part, without the oid
            k = ((omap_key_t*)keys[i])->key;
            /* this key is not part of the same object, we should leave the loop */
            if(((omap_key_t*)keys[i])->oid != oid) goto out; /* ugly way of leaving the loop, I know ... */

            omap_iter_append(*iter, k, nullptr, 0);
        }
        strcpy(lb->key, k);
        lb_size = strlen(k) + sizeof(omap_key_t);

    } while(keys_retrieved == max_keys && count < max_return);

out:
    free(lb);
}

void read_op_exec_omap_get_vals(void* u, const char* start_after, const char* filter_prefix, uint64_t max_return, mobject_store_omap_iter_t* iter, int* prval)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
    const char* object_name = vargs->object_name;
    sdskv_provider_handle_t sdskv_ph = vargs->srv_ctx->sdskv_ph;
    sdskv_database_id_t omap_db_id = vargs->srv_ctx->omap_db_id;
    int ret;
    *prval = 0;

    // find oid
    if(name_map.count(object_name) == 0) {
        *prval = -1;
        return;
    }
    oid_t oid = name_map[object_name];

    hg_size_t max_items = 10;
    // TODO make this changeable
    hg_size_t key_len  = 128 + sizeof(omap_key_t);
    hg_size_t val_len  = 256;

    omap_iter_create(iter);

    /* omap_key_t equivalent of start_key */
    hg_size_t lb_size = sizeof(omap_key_t)+strlen(start_after);
    omap_key_t* lb = (omap_key_t*)calloc(1, lb_size);
    lb->oid = oid;
    strcpy(lb->key, start_after);

    /* omap_key_t equivalent of the filter_prefix */
    hg_size_t prefix_size = sizeof(omap_key_t)+strlen(filter_prefix);
    omap_key_t* prefix = (omap_key_t*)calloc(1, prefix_size);
    prefix->oid = oid;
    strcpy(prefix->key, filter_prefix);

    /* key entry used to navigate the returned keys */
    omap_key_t* key = (omap_key_t*)calloc(1, key_len);
    key->oid = oid;
    
    /* initialize structures to pass to SDSKV functions */
    std::vector<void*> keys(max_items);
    std::vector<void*> vals(max_items);
    std::vector<hg_size_t> ksizes(max_items, key_len);
    std::vector<hg_size_t> vsizes(max_items, val_len);
    std::vector<std::vector<char>> key_buffers(max_items, std::vector<char>(key_len));
    std::vector<std::vector<char>> val_buffers(max_items, std::vector<char>(val_len));
    for(auto i=0; i < max_items; i++) {
        keys[i] = (void*)key_buffers[i].data();
        vals[i] = (void*)val_buffers[i].data();
    }

    hg_size_t items_retrieved = max_items;
    hg_size_t count = 0;
    do {
        ret = sdskv_list_keyvals_with_prefix(
                sdskv_ph, omap_db_id,
                (const void*)lb, lb_size,
                (const void*)prefix, prefix_size,
                keys.data(), ksizes.data(),
                vals.data(), vsizes.data(),
                &items_retrieved);
        if(ret != SDSKV_SUCCESS) {
            *prval = -1;
            break;
        }
        const char* k;
        for(auto i = 0; i < items_retrieved && count < max_return; i++, count++) {
            // extract the actual key part, without the oid
            k = ((omap_key_t*)keys[i])->key;
            /* this key is not part of the same object, we should leave the loop */
            if(((omap_key_t*)keys[i])->oid != oid) goto out; /* ugly way of leaving the loop, I know ... */

            omap_iter_append(*iter, k, (const char*)vals[i], vsizes[i]);
        }
        strcpy(lb->key, k);
        lb_size = strlen(k) + sizeof(omap_key_t);

    } while(items_retrieved == max_items && count < max_return);

out:

    free(lb);
    free(key);
}

void read_op_exec_omap_get_vals_by_keys(void* u, char const* const* keys, size_t num_keys, mobject_store_omap_iter_t* iter, int* prval)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
    const char* object_name = vargs->object_name;
    sdskv_provider_handle_t sdskv_ph = vargs->srv_ctx->sdskv_ph;
    sdskv_database_id_t omap_db_id = vargs->srv_ctx->omap_db_id;
    int ret;
    *prval = 0;

    // find oid
    if(name_map.count(object_name) == 0) {
        *prval = -1;
        return;
    }
    oid_t oid = name_map[object_name];

    omap_iter_create(iter);

    // figure out key sizes
    std::vector<hg_size_t> ksizes(num_keys);
    hg_size_t max_ksize = 0;
    for(auto i=0; i < num_keys; i++) {
        hg_size_t s = strlen(keys[i])+1;
        if(s > max_ksize) max_ksize = s;
        ksizes[i] = s;
    }

    omap_key_t* key = (omap_key_t*)calloc(1, sizeof(omap_key_t)+max_ksize);
    key->oid = oid;
    for(size_t i=0; i < num_keys; i++) {
        strcpy(key->key, keys[i]);
        // get length of the value
        hg_size_t vsize;
        ret = sdskv_length(sdskv_ph, omap_db_id, 
                (const void*)key, ksizes[i], &vsize);
        if(ret != SDSKV_SUCCESS) {
            *prval = -1;
            break;
        }
        std::vector<char> value(vsize);
        ret = sdskv_get(sdskv_ph, omap_db_id,
                (const void*)key, ksizes[i],
                (void*)value.data(), &vsize);
        if(ret != SDSKV_SUCCESS) {
            *prval = -1;
            break;
        }
        omap_iter_append(*iter, keys[i], value.data(), vsize);
    }
}

void read_op_exec_end(void* u)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
}
