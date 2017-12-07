#include <assert.h>
#include <stdio.h>
#include <margo.h>
#include <libmobject-store.h>

const char* content = "AAAABBBBCCCCDDDDEEEEFFFF";

/* Main function. */
int main(int argc, char** argv)
{
    mobject_store_t cluster;
    mobject_store_create(&cluster, "admin");
    mobject_store_connect(cluster);
    mobject_store_ioctx_t ioctx;
    mobject_store_ioctx_create(cluster, "my-object-pool", &ioctx);

    { // WRITE OP TEST

        mobject_store_write_op_t write_op = mobject_store_create_write_op();

        // Add a "create" operation
        mobject_store_write_op_create(write_op, LIBMOBJECT_CREATE_EXCLUSIVE, NULL);
        // Add a "write_full" operation to write "AAAABBBB"
        mobject_store_write_op_write_full(write_op, content, 8);
        // Add a "write" operation to write "CCCC"
        mobject_store_write_op_write(write_op, content+8, 4, 8);
        // Add a "writesame" operation to write "DDDD" in two "DD"
        mobject_store_write_op_writesame(write_op, content+12, 2, 4, 12);
        // Add a "append" operation to append "EEEEFFFF"
        mobject_store_write_op_append(write_op, content+16, 8);
        // Add a "remove" operation
//        mobject_store_write_op_remove(write_op);
        // Add a "truncate" operation to remove the "FFFF" part
        mobject_store_write_op_truncate(write_op, 20);
        // Add a "zero" operation zero-ing the "BBBBCCCC"
        mobject_store_write_op_zero(write_op, 4, 8);
        // Add a "omap_set" operation
        const char* keys[]   = { "matthieu", "rob", "shane", "phil", "robl" };
        const char* values[] = { "mdorier@anl.gov", "rross@anl.gov", "ssnyder@anl.gov", "carns@anl.gov", "robl@anl.gov" };
        size_t val_sizes[]   = { 16, 14, 16, 14, 13 };
        mobject_store_write_op_omap_set(write_op, keys, values, val_sizes, 5);
        // Add a omap_rm_keys" operation
//        mobject_store_write_op_omap_rm_keys(write_op, keys, 5);

        mobject_store_write_op_operate(write_op, ioctx, "test-object", NULL, LIBMOBJECT_OPERATION_NOFLAG);

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
        mobject_store_read_op_read(read_op, 0, 512, read_buf, &bytes_read, &prval2);
        // Add "omap_get_keys" operation
        const char* start_after1 = "shane";
        mobject_store_omap_iter_t iter3;
        int prval3;
        mobject_store_read_op_omap_get_keys(read_op, start_after1, 7, &iter3, &prval3);
        // Add "omap_get_vals" operation
        const char* start_after2 = "matthieu";
        const char* filter_prefix2 = "p";
        mobject_store_omap_iter_t iter4;
        int prval4;
        mobject_store_read_op_omap_get_vals(read_op, start_after2, filter_prefix2, 3, &iter4, &prval4);
        // Add "omap_get_vals_by_keys" operation
        const char* keys[] = {"matthieu", "robl"};
        mobject_store_omap_iter_t iter5;
        int prval5;
        mobject_store_read_op_omap_get_vals_by_keys(read_op, keys, 2, &iter5, &prval5);

        mobject_store_read_op_operate(read_op, ioctx, "test-object",LIBMOBJECT_OPERATION_NOFLAG);

        mobject_store_release_read_op(read_op);

        // print the results of the read operations
        printf("Client received the following results:\n");
        printf("stat: psize=%ld pmtime=%lld prval=%d\n", psize, (long long)pmtime, prval1);
        {
            printf("read: bytes_read = %ld, prval=%d content: ", bytes_read, prval2);
            unsigned i;
            for(i=0; i<bytes_read; i++) printf("%c", read_buf[i] ? read_buf[i] : '*' );
            printf("\n");
        }
        printf("omap_get_keys: prval=%d\n", prval3);
        {
            char* key = NULL;
            char* val = NULL;
            size_t size;
            do {
                mobject_store_omap_get_next(iter3, &key, &val, &size);
                if(key) printf("===> key: \"%s\"\n", key);
            } while(key);
        }
        printf("omap_get_vals: prval=%d\n", prval4);
        {
            char* key = NULL;
            char* val = NULL;
            size_t size;
            do {
                mobject_store_omap_get_next(iter4, &key, &val, &size);
                if(key) printf("===> key: \"%s\" , val: %s \n", key, val);
            } while(key);
        }
        printf("omap_get_vals_by_keys: prval=%d\n", prval5);
        {
            char* key = NULL;
            char* val = NULL;
            size_t size;
            do {
                mobject_store_omap_get_next(iter5, &key, &val, &size);
                if(key) printf("===> key: \"%s\" , val: %s \n", key, val);
            } while(key);
        }
    }

    mobject_store_ioctx_destroy(ioctx);

    mobject_store_shutdown(cluster);

    return 0;
}
