/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_COMPLETION_H
#define __MOBJECT_COMPLETION_H

#include <abt.h>
#include "mobject-store-config.h"

struct mobject_store_completion {
	mobject_store_callback_t cb_complete;    // completion callback
	mobject_store_callback_t cb_safe;        // safe callback
	void*                    cb_arg;         // arguments for callbacks
	ABT_eventual             eventual;       // eventual used to notify completion
	int*                     ret_value_ptr;  // pointer to eventual's internal value
	ABT_rwlock               lock;           // lock protecting access to this structure
};

#endif

