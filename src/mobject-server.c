#include <sds-keyval.h>
#include <bake-bulk.h>
#include <bake-bulk-server.h>
#include <margo.h>
#include <libpmemobj.h>

#include <mobject-server.h>

int mobject_server_register(const char *addr_str, const char *poolname)
{
    int ret=0;
    margo_instance_id mid;
    kv_context *metadata;
    struct bake_pool_info *pool_info;

    pool_info = bake_server_makepool(poolname);

    mid = margo_init(addr_str, MARGO_SERVER_MODE, 0, -1);

    bake_server_register(mid, pool_info);
    metadata = kv_server_register(mid);
    return ret;
}


int mobject_shutdown()
{
    margo_wait_for_finalize(NULL);
    pmemobj_close(NULL);


}
