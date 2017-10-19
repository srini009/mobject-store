/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_READ_OP_H
#define __MOBJECT_READ_OP_H

#include <margo.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "read-actions.h"

struct mobject_store_read_op {
	int              use_local_pointers;
	hg_bulk_t        bulk_handle;
	size_t           num_actions;
	rd_action_base_t actions;
};

#endif
