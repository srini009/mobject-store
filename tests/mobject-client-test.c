#include <assert.h>
#include <stdio.h>
#include <margo.h>
#include <libmobject-store.h>

/* Main function. */
int main(int argc, char** argv)
{
    mobject_store_t cluster;
    mobject_store_create(&cluster, "admin");
    mobject_store_connect(cluster);
    mobject_store_ioctx_t ioctx;
    mobject_store_ioctx_create(cluster, "my-object-pool", &ioctx);
    char buffer[256];
    unsigned i;
    for(i=0; i<256; i++) buffer[i] = 'A'+(i % 26);

    { // WRITE OP TEST

        mobject_store_write_op_t write_op = mobject_store_create_write_op();

        // Add a "create" operation
        mobject_store_write_op_create(write_op, LIBMOBJECT_CREATE_EXCLUSIVE, NULL);
        // Add a "write" operation
        mobject_store_write_op_write(write_op, buffer, 128, 32);
        // Add a "write_full" operation
        mobject_store_write_op_write_full(write_op, buffer, 256);
        // Add a "writesame" operation
        mobject_store_write_op_writesame(write_op, buffer, 32, 64, 256);
        // Add a "append" operation
        mobject_store_write_op_append(write_op, buffer, 64);
        // Add a "remove" operation
        mobject_store_write_op_remove(write_op);
        // Add a "truncate" operation
        mobject_store_write_op_truncate(write_op, 32);
        // Add a "zero" operation
        mobject_store_write_op_zero(write_op, 16, 48);
        // Add a "omap_set" operation
        const char* keys[]   = { "matthieu", "rob", "shane", "phil", "robl" };
        const char* values[] = { "mdorier@anl.gov", "rross@anl.gov", "ssnyder@anl.gov", "carns@anl.gov", "robl@anl.gov" };
        size_t val_sizes[]   = { 16, 14, 16, 14, 13 };
        mobject_store_write_op_omap_set(write_op, keys, values, val_sizes, 5);
        // Add a omap_rm_keys" operation
        mobject_store_write_op_omap_rm_keys(write_op, keys, 5);
        fprintf(stderr,"FFF\n");
        mobject_store_write_op_operate(write_op, ioctx, "test-object", NULL, LIBMOBJECT_OPERATION_NOFLAG);
        fprintf(stderr,"GGG\n");
        mobject_store_release_write_op(write_op);

    }

    { // READ OP TEST

        mobject_store_read_op_t read_op = mobject_store_create_read_op();

        // Add "stat" operation
        uint64_t psize;
        time_t pmtime;
        int prval1;
        mobject_store_read_op_stat(read_op, &psize, &pmtime, &prval1);
        // Add "read" operation
        char read_buf[512];
        size_t bytes_read;
        int prval2;
        mobject_store_read_op_read(read_op, 2, 32, buffer, &bytes_read, &prval2);
        // Add "omap_get_keys" operation
        const char* start_after = "shane";
        mobject_store_omap_iter_t iter3;
        int prval3;
        mobject_store_read_op_omap_get_keys(read_op, start_after, 7, &iter3, &prval3);
        // Add "omap_get_vals" operation
        const char* filter_prefix = "p";
        mobject_store_omap_iter_t iter4;
        int prval4;
        mobject_store_read_op_omap_get_vals(read_op, start_after, filter_prefix, 3, &iter4, &prval4);
        // Add "omap_get_vals_by_keys" operation
        const char* keys[] = {"matthieu", "shane"};
        int prval5;
        mobject_store_read_op_omap_get_vals_by_keys(read_op, keys, 2, &iter4, &prval5);

        mobject_store_read_op_operate(read_op, ioctx, "test-object",LIBMOBJECT_OPERATION_NOFLAG);

        mobject_store_release_read_op(read_op);

        // print the results of the read operations
        printf("Client received the following results:\n");
        printf("stat: psize=%ld pmtime=%lld prval=%d\n", psize, (long long)pmtime, prval1);
        printf("read: bytes_read=%ld prval=%d\n", bytes_read, prval2);
        printf("omap_get_keys: prval=%d\n", prval3);
        printf("omap_get_vals: prval=%d\n", prval4);
        printf("omap_get_vals_by_keys: prval=%d\n", prval5);
    }

    mobject_store_ioctx_destroy(ioctx);

    mobject_store_shutdown(cluster);

    return 0;
}
