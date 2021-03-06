#include <assert.h>
#include <stdio.h>
#include <margo.h>
#include <libmobject-store.h>
#include "src/rpc-types/write-op.h"
#include "src/rpc-types/read-op.h"
#include "src/io-chain/prepare-write-op.h"
#include "src/io-chain/prepare-read-op.h"

/* Main function. */
int main(int argc, char** argv)
{
	if(argc != 2) {
		fprintf(stderr,"Usage: %s <server address>\n", argv[0]);
		exit(0);
	}

	/* Start Margo */
	margo_instance_id mid = margo_init("bmi+tcp", MARGO_CLIENT_MODE, 0, 0);

	/* Register a RPC function */
	hg_id_t write_op_rpc_id = MARGO_REGISTER(mid, "mobject_write_op", write_op_in_t, write_op_out_t, NULL);
	hg_id_t read_op_rpc_id  = MARGO_REGISTER(mid, "mobject_read_op",  read_op_in_t,  read_op_out_t, NULL);

	/* Lookup the address of the server */
	hg_addr_t svr_addr;
	margo_addr_lookup(mid, argv[1], &svr_addr);

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

		// BEGIN this is what write_op_operate should contain

		write_op_in_t in;
                in.object_name = "test-object";
		in.write_op = write_op;

		prepare_write_op(mid, write_op);

		hg_handle_t h;
		margo_create(mid, svr_addr, write_op_rpc_id, &h);
		margo_forward(h, &in);
		
		write_op_out_t resp;
		margo_get_output(h, &resp);

		margo_free_output(h,&resp);
		margo_destroy(h);

		// END this is what write_op_operate should contain

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

		// BEGIN this is what read_op_operate should contain

		read_op_in_t in;
		in.object_name = "test-object";
		in.read_op = read_op;

		prepare_read_op(mid, read_op);

		hg_handle_t h;
		margo_create(mid, svr_addr, read_op_rpc_id, &h);
		margo_forward(h, &in);
	
		read_op_out_t resp;
		margo_get_output(h, &resp);

		feed_read_op_pointers_from_response(read_op, resp.responses);

		margo_free_output(h,&resp);
		margo_destroy(h);

		// END this is what read_op_operate should contain

		mobject_store_release_read_op(read_op);

		// print the results of the read operations
		printf("Client received the following results:\n");
		printf("stat: psize=%ld pmtime=%lld prval=%d\n", psize, (long long)pmtime, prval1);
		printf("read: bytes_read=%ld prval=%d\n", bytes_read, prval2);
		printf("omap_get_keys: prval=%d\n", prval3);
		printf("omap_get_vals: prval=%d\n", prval4);
		printf("omap_get_vals_by_keys: prval=%d\n", prval5);
	}

	/* free the address */
	margo_addr_free(mid, svr_addr);

	/* shut down Margo */
	margo_finalize(mid);

	return 0;
}
