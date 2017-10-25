/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "prepare-read-op.h"
#include "read-op-impl.h"
#include "utlist.h"
#include "log.h"

static void prepare_read(uint64_t* cur_offset,
                          rd_action_read_t action,
                          void** ptr,
                          size_t* len);

void prepare_read_op(margo_instance_id mid, mobject_store_read_op_t read_op) 
{
	if(read_op->ready == 1) return;
	if(read_op->num_actions == 0) {
		read_op->ready = 1;
		return;
	}

	rd_action_base_t action;

	void** pointers = (void**)calloc(read_op->num_actions, sizeof(void*));
	size_t* lengths = (size_t*)calloc(read_op->num_actions, sizeof(size_t));
	uint64_t current_offset = 0;
	size_t i = 0;

	DL_FOREACH(read_op->actions, action) {

		switch(action->type) {
		case READ_OPCODE_READ:
			prepare_read(&current_offset, 
				(rd_action_read_t)action, pointers+i, lengths+i);
			i += 1;
			break;
		}	
	}

	uint32_t count = i;
	if(count != 0) {
		hg_return_t ret = margo_bulk_create(mid, count,
    						pointers, lengths, HG_BULK_WRITE_ONLY, 
							&(read_op->bulk_handle));
		// TODO handle error

	}

	read_op->ready = 1;

	free(pointers);
	free(lengths);
}

////////////////////////////////////////////////////////////////////////////////
//                          STATIC FUNCTIONS BELOW                            //
////////////////////////////////////////////////////////////////////////////////

static void prepare_read(uint64_t* cur_offset,
                          rd_action_read_t action,
                          void** ptr,
                          size_t* len)
{
	uint64_t pos = *cur_offset;
	*cur_offset += action->len;
	*ptr         = (void*)action->buffer.as_pointer;
	*len         = action->len;
	action->buffer.as_offset = pos;
}
