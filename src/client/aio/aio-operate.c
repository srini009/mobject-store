/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/client/cluster.h"
#include "src/io-chain/prepare-write-op.h"
#include "src/io-chain/prepare-read-op.h"
#include "src/rpc-types/write-op.h"
#include "src/rpc-types/read-op.h"
#include "src/client/aio/completion.h"
#include "src/util/log.h"

// global variables for RPC ids, defined in client/cluster.c
extern hg_id_t mobject_write_op_rpc_id;
extern hg_id_t mobject_read_op_rpc_id;

int mobject_store_aio_write_op_operate(mobject_store_write_op_t write_op,
        mobject_store_ioctx_t io,
        mobject_store_completion_t completion,
        const char *oid,
        time_t *mtime,
        int flags)
{   
    hg_return_t ret;

    write_op_in_t in;
    in.object_name = oid;
    in.pool_name   = io->pool_name;
    in.write_op    = write_op;
    // TODO take mtime into account

    prepare_write_op(io->cluster->mid, write_op);

    hg_addr_t svr_addr = ssg_get_addr(io->cluster->gid, 0); // XXX pick other servers using ch-placement
    MOBJECT_ASSERT(svr_addr != HG_ADDR_NULL, "NULL server address");

    hg_handle_t h;
    ret = margo_create(io->cluster->mid, svr_addr, mobject_write_op_rpc_id, &h);
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not create RPC handle");

    margo_request req;

    ret = margo_iforward(h, &in, &req);
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not forward RPC");
    
    completion->request = req;
    completion->handle = h;
    completion->type = AIO_WRITE_COMPLETION;
    completion->op.write_op = write_op;

    return 0;
}

int mobject_store_aio_read_op_operate(mobject_store_read_op_t read_op,
        mobject_store_ioctx_t io,
        mobject_store_completion_t completion,
        const char *oid,
        int flags)
{   
    hg_return_t ret;

    read_op_in_t in; 
    in.object_name = oid;
    in.pool_name   = io->pool_name;
    in.read_op     = read_op;

    prepare_read_op(io->cluster->mid, read_op);

    hg_addr_t svr_addr = ssg_get_addr(io->cluster->gid, 0); // XXX pick other servers using ch-placement
    MOBJECT_ASSERT(svr_addr != HG_ADDR_NULL, "NULL server address");

    hg_handle_t h;
    ret = margo_create(io->cluster->mid, svr_addr, mobject_read_op_rpc_id, &h);
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not create RPC handle");
    margo_request req;
    ret = margo_iforward(h, &in, &req);
    MOBJECT_ASSERT(ret == HG_SUCCESS, "Could not forward RPC");

    completion->request = req;
    completion->handle = h;
    completion->type = AIO_READ_COMPLETION;
    completion->op.read_op = read_op;

    return 0;
}
