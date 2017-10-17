/* the SDS-keyval and bake-bulk tests should still work, even if there is a higher-level protocol */
int main(int argc, char **argv)
{
    mobject_server_register(argc, argv);
    margo_wait_for_finalize();
    margo_server_deregister();
}
