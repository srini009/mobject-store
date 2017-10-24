#include <assert.h>
#include <stdio.h>
#include <margo.h>
#include <libmobject-store.h>
#include "types.h"
#include "src/prepare-write-op.h"

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
	
		write_op_in_t in;
		in.object_name = "test-write-object";

		mobject_store_write_op_t write_op = mobject_store_create_write_op();
		in.chain = write_op;

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
		// the operation chain should be prepare for sending before being serialized
		prepare_write_op(mid, write_op);

		hg_handle_t h;
		margo_create(mid, svr_addr, write_op_rpc_id, &h);
		margo_forward(h, &in);
		
		write_op_out_t resp;
		margo_get_output(h, &resp);

		// printf("Got response: %d+%d = %d\n", args.x, args.y, resp.ret);

		margo_free_output(h,&resp);
		margo_destroy(h);
	}

#if 0
	{ // READ OP TEST
		 
		read_op_in_t in;

		// TODO fill the read_op_in_t

		hg_handle_t h;
		margo_create(mid, svr_addr, read_op_rpc_id, &h);
		margo_forward(h, &in);
	
		read_op_out_t resp;
		margo_get_output(h, &resp);

		// print something

		margo_free_output(h,&resp);
		margo_destroy(h);
	}
#endif
	/* free the address */
	margo_addr_free(mid, svr_addr);

	/* shut down Margo */
	margo_finalize(mid);

	return 0;
}
