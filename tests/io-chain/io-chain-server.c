#include <assert.h>
#include <stdio.h>
#include <abt.h>
#include <abt-snoozer.h>
#include <margo.h>
#include <mercury.h>
#include "types.h"

/* after serving this number of rpcs, the server will shut down. */
static const int TOTAL_RPCS = 16;
/* number of RPCS already received. */
static int num_rpcs = 0;

/* 
 * hello_world function to expose as an RPC.
 * This function just prints "Hello World"
 * and increment the num_rpcs variable.
 *
 * All Mercury RPCs must have a signature
 *   hg_return_t f(hg_handle_t h)
 */
hg_return_t sum(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(sum)

/*
 * main function.
 */
int main(int argc, char** argv)
{
	/* Initialize Margo */
	margo_instance_id mid = margo_init("bmi+tcp", MARGO_SERVER_MODE, 0, 0);
    assert(mid);

	hg_addr_t my_address;
	margo_addr_self(mid, &my_address);
	char addr_str[128];
	size_t addr_str_size = 128;
	margo_addr_to_string(mid, addr_str, &addr_str_size, my_address);
	margo_addr_free(mid,my_address);
	printf("Server running at address %s\n", addr_str);

	/* Register the RPC by its name ("sum") */
	MARGO_REGISTER(mid, "sum", sum_in_t, sum_out_t, sum);

	/* NOTE: there isn't anything else for the server to do at this point
     * except wait for itself to be shut down.  The
     * margo_wait_for_finalize() call here yields to let Margo drive
     * progress until that happens.
	 */
	margo_wait_for_finalize(mid);

	return 0;
}

/* Implementation of the RPC. */
hg_return_t sum(hg_handle_t h)
{
	hg_return_t ret;
	num_rpcs += 1;

	sum_in_t in;
	sum_out_t out;

	margo_instance_id mid = margo_hg_handle_get_instance(h);

	/* Deserialize the input from the received handle. */
	ret = margo_get_input(h, &in);
	assert(ret == HG_SUCCESS);

	/* Compute the result. */
	out.ret = in.x + in.y;
	printf("Computed %d + %d = %d\n",in.x,in.y,out.ret);

	ret = margo_respond(h, &out);
	assert(ret == HG_SUCCESS);

	/* Free the input data. */
	ret = margo_free_input(h, &in);
	assert(ret == HG_SUCCESS);

	/* We are not going to use the handle anymore, so we should destroy it. */
	ret = margo_destroy(h);
	assert(ret == HG_SUCCESS);

	if(num_rpcs == TOTAL_RPCS) {
		/* NOTE: we assume that the server daemon is using
		 * margo_wait_for_finalize() to suspend until this RPC executes, so there
		 * is no need to send any extra signal to notify it.
		 */
		margo_finalize(mid);
	}

	return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(sum)
