#include <assert.h>
#include <stdio.h>
#include <margo.h>
#include "types.h"

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
	hg_id_t read_op_rpc_id  = MARGO_REGISTER(mid, "mobject_read_op", read_op_in_t, read_op_out_t, NULL);

	/* Lookup the address of the server */
	hg_addr_t svr_addr;
	margo_addr_lookup(mid, argv[1], &svr_addr);

	{ // WRITE OP TEST
	
		write_op_in_t in;

		// TODO fill the write_op_in_t

		hg_handle_t h;
		margo_create(mid, svr_addr, write_op_rpc_id, &h);
		margo_forward(h, &args);
		
		write_op_out_t resp;
		margo_get_output(h, &resp);

		// printf("Got response: %d+%d = %d\n", args.x, args.y, resp.ret);

		margo_free_output(h,&resp);
		margo_destroy(h);
	}

	{ // READ OP TEST
		 
		read_op_in_t in;

		// TODO fill the read_op_in_t

		hg_handle_t h;
		margo_create(mid, svr_addr, read_op_rpc_id, &h);
		margo_forward(h, &args);
	
		read_op_out_t resp;
		margo_get_output(h, &resp);

		// print something

		margo_free_output(h,&resp);
		margo_destroy(h);
	}

	/* free the address */
	margo_addr_free(mid, svr_addr);

	/* shut down Margo */
    margo_finalize(mid);

	return 0;
}
