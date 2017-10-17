/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_WRITE_OP_H
#define __MOBJECT_WRITE_OP_H

#include "mobject-store-config.h"
#include "write-actions.h"

struct mobject_store_write_op {
	size_t num_actions;
	wr_action_base_t actions;
};

#endif

