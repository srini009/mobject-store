#include <assert.h>
#include <stdio.h>
#include <abt.h>
#include <abt-snoozer.h>
#include <margo.h>
#include <mercury.h>
#include "types.h"
#include "src/write-op-visitor.h"
#include "src/read-op-visitor.h"

/* after serving this number of rpcs, the server will shut down. */
static const int TOTAL_RPCS = 16;
/* number of RPCS already received. */
static int num_rpcs = 0;

hg_return_t mobject_write_op_rpc(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(mobject_write_op_rpc)

hg_return_t mobject_read_op_rpc(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(mobject_read_op_rpc)

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
	MARGO_REGISTER(mid, "mobject_write_op", write_op_in_t, write_op_out_t, mobject_write_op_rpc);
	MARGO_REGISTER(mid, "mobject_read_op", read_op_in_t, read_op_out_t, mobject_read_op_rpc)

	/* NOTE: there isn't anything else for the server to do at this point
	 * except wait for itself to be shut down.  The
	 * margo_wait_for_finalize() call here yields to let Margo drive
	 * progress until that happens.
	 */
	margo_wait_for_finalize(mid);

	return 0;
}

static void write_op_printer_begin(void*);
static void write_op_printer_end(void*);
static void write_op_printer_create(void*, int);
static void write_op_printer_write(void*, buffer_u, size_t, uint64_t);
static void write_op_printer_write_full(void*, buffer_u, size_t);
static void write_op_printer_writesame(void*, buffer_u, size_t, size_t, uint64_t);
static void write_op_printer_append(void*, buffer_u, size_t);
static void write_op_printer_remove(void*);
static void write_op_printer_truncate(void*, uint64_t);
static void write_op_printer_zero(void*, uint64_t, uint64_t);
static void write_op_printer_omap_set(void*, char const* const*, char const* const*, const size_t*, size_t);
static void write_op_printer_omap_rm_keys(void*, char const* const*, size_t);

struct write_op_visitor write_op_printer = {
	.visit_begin        = write_op_printer_begin,
	.visit_create       = write_op_printer_create,
	.visit_write        = write_op_printer_write,
	.visit_write_full   = write_op_printer_write_full,
	.visit_writesame    = write_op_printer_writesame,
	.visit_append       = write_op_printer_append,
	.visit_remove       = write_op_printer_remove,
	.visit_truncate     = write_op_printer_truncate,
	.visit_zero         = write_op_printer_zero,
	.visit_omap_set     = write_op_printer_omap_set,
	.visit_omap_rm_keys = write_op_printer_omap_rm_keys,
	.visit_end          = write_op_printer_end
};

/* Implementation of the RPC. */
hg_return_t mobject_write_op_rpc(hg_handle_t h)
{
	hg_return_t ret;
	num_rpcs += 1;

	write_op_in_t in;
	write_op_out_t out;

	margo_instance_id mid = margo_hg_handle_get_instance(h);

	/* Deserialize the input from the received handle. */
	ret = margo_get_input(h, &in);
	assert(ret == HG_SUCCESS);

	/* Execute the operation chain */
	execute_write_op_visitor(&write_op_printer, in.chain, in.object_name);

	// set the return value of the RPC
	out.ret = 0;

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
DEFINE_MARGO_RPC_HANDLER(mobject_write_op_rpc)


static void read_op_printer_begin(void*);
static void read_op_printer_stat(void*, uint64_t*, time_t*, int*);
static void read_op_printer_read(void*, uint64_t, size_t, buffer_u, size_t*, int*);
static void read_op_printer_omap_get_keys(void*, const char*, uint64_t, mobject_store_omap_iter_t*, int*);
static void read_op_printer_omap_get_vals(void*, const char*, const char*, uint64_t, mobject_store_omap_iter_t*, int*);
static void read_op_printer_omap_get_vals_by_keys(void*, char const* const*, size_t, mobject_store_omap_iter_t*, int*);
static void read_op_printer_end(void*);

struct read_op_visitor read_op_printer = {
	.visit_begin                   = read_op_printer_begin,
	.visit_end                     = read_op_printer_end,
	.visit_stat                    = read_op_printer_stat,
	.visit_read                    = read_op_printer_read,
	.visit_omap_get_keys           = read_op_printer_omap_get_keys,
	.visit_omap_get_vals           = read_op_printer_omap_get_vals,
	.visit_omap_get_vals_by_keys   = read_op_printer_omap_get_vals_by_keys
};

/* Implementation of the RPC. */
hg_return_t mobject_read_op_rpc(hg_handle_t h)
{
	hg_return_t ret;
	num_rpcs += 1;

	read_op_in_t in;
	read_op_out_t out;

	margo_instance_id mid = margo_hg_handle_get_instance(h);

	/* Deserialize the input from the received handle. */
	ret = margo_get_input(h, &in);
	assert(ret == HG_SUCCESS);

	/* Compute the result. */
	execute_read_op_visitor(&read_op_printer, in.chain, in.object_name);

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
DEFINE_MARGO_RPC_HANDLER(mobject_read_op_rpc)

void write_op_printer_begin(void* object_name)
{
	printf("<mobject_write_operation on=\"%s\">\n",(char*)object_name);
}

void write_op_printer_end(void* unused)
{
	printf("</mobject_write_operation>\n");
}

void write_op_printer_create(void* unused, int exclusive)
{
	printf("\t<create exclusive=%d />\n", exclusive);
}

void write_op_printer_write(void* u, buffer_u buf, size_t len, uint64_t offset)
{
	printf("\t<write from=%ld length=%ld offset=%ld />\n", buf.as_offset, len, offset);
}

void write_op_printer_write_full(void* u, buffer_u buf, size_t len)
{
	printf("\t<write_full from=%ld length=%ld />\n", buf.as_offset, len);
}

void write_op_printer_writesame(void* u, buffer_u buf, size_t data_len, size_t write_len, uint64_t offset)
{
	printf("\t<writesame from=%ld data_len=%ld write_len=%ld offset=%ld />\n",
		buf.as_offset, data_len, write_len, offset);
}

void write_op_printer_append(void* u, buffer_u buf, size_t len)
{
	printf("\t<append from=%ld length=%ld />\n", buf.as_offset, len);
}

void write_op_printer_remove(void* u)
{
	printf("\t<remove />\n");
}

void write_op_printer_truncate(void* u, uint64_t offset)
{
	printf("\t<truncate offset=%ld />\n", offset);
}

void write_op_printer_zero(void* u, uint64_t offset, uint64_t len)
{
	printf("\t<zero offset=%ld len=%ld />\n", offset, len);
}

void write_op_printer_omap_set(void* u,	char const* const* keys,
                                     char const* const* vals,
                                     const size_t *lens,
                                     size_t num)
{
	printf("\t<omap_set num=%ld>\n", num);
	unsigned i;
	for(i=0; i<num; i++) {
		printf("\t\t<record key=\"%s\"\t lens=%ld>%s</record>\n",
			keys[i], lens[i], vals[i]);
	}
	printf("\t</omap_set>\n");
}

void write_op_printer_omap_rm_keys(void* u, char const* const* keys, size_t num_keys)
{
	printf("\t<omap_rm_keys num=%ld>\n", num_keys);
	unsigned i;
	for(i=0; i<num_keys; i++) {
		printf("\t\t<record key=\"%s\" />\n", keys[i]);
	}
	printf("\t</omap_rm_keys>\n");
}

void read_op_printer_begin(void* u)
{
	printf("<mobject_read_operation object_name=\"%s\">\n", (char*)u);
}

void read_op_printer_stat(void* u, uint64_t* psize, time_t* pmtime, int* prval)
{
	printf("\t<stat/>\n");
}

void read_op_printer_read(void* u, uint64_t offset, size_t len, buffer_u buf, size_t* bytes_read, int* prval)
{
	printf("\t<read offset=%ld length=%ld to=%ld/>\n",offset, len, buf.as_offset);
}

void read_op_printer_omap_get_keys(void* u, const char* start_after, uint64_t max_return, 
				mobject_store_omap_iter_t* iter, int* prval)
{
	printf("\t<omap_get_keys start_after=\"%s\" max_return=%ld />\n", start_after, max_return);
}

void read_op_printer_omap_get_vals(void* u, const char* start_after, const char* filter_prefix, uint64_t max_return, mobject_store_omap_iter_t* iter, int* prval)
{
	printf("\t<omap_get_vals start_after=\"%s\" filter_prefix=\"%s\" max_return=%ld />\n",
		start_after, filter_prefix, max_return);
}

void read_op_printer_omap_get_vals_by_keys(void* u, char const* const* keys, size_t num_keys, mobject_store_omap_iter_t* iter, int* prval)
{
	printf("\t<omap_get_vals_by_keys num=%ld>\n", num_keys);
	unsigned i;
	for(i=0; i<num_keys; i++)
		printf("\t\t<record key=\"%s\" />\n",  keys[i]);
	printf("\t<omap_get_vals_by_keys/>\n");
}

void read_op_printer_end(void* u)
{
	printf("</mobject_read_operation>\n");
}
