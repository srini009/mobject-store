#include <margo.h>
#include <mobject-server.h>
/* the SDS-keyval and bake-bulk tests should still work, even if there is a higher-level protocol */
int main(int argc, char **argv)
{
    margo_instance_id mid;
    mid = margo_init(argv[1], MARGO_SERVER_MODE, 0, -1);
    mobject_server_register(argv[2], argv[3]);
    margo_wait_for_finalize(mid);
}
