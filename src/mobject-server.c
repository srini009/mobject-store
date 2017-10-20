#include <sds-keyval.h>
#include <bake-bulk.h>
#include <bake-bulk-server.h>
#include <margo.h>
#include <libpmemobj.h>

#include <mobject-server.h>

int mobject_server_register(margo_instance_id mid, const char *poolname)
{
    int ret=0;
    kv_context *metadata;
    struct bake_pool_info *pool_info;

    pool_info = bake_server_makepool(poolname);

    bake_server_register(mid, pool_info);
    metadata = kv_server_register(mid);
    return ret;
}


int mobject_shutdown(margo_instance_id mid)
{
    margo_wait_for_finalize(mid);
    pmemobj_close(NULL);
    return 0;

}
