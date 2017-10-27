/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_COMPLETION_H
#define __MOBJECT_COMPLETION_H

#include <abt.h>
#include "mobject-store-config.h"

typedef enum {
	COMPLETION_CREATED = 1,
	COMPLETION_IN_PROGRESS,
	COMPLETION_TERMINATED,
	COMPLETION_JOINED
} completion_state_t;

/**
 * The mobject_store_completion object is used for asynchronous
 * functions. It contains the callbacks to call when the data is
 * safe and when the operation has completed, as well as potential
 * user data and required mechanism to be able to block on the
 * completion object.
 * mobject_store_completion* is typedef-ed as mobject_store_completion_t
 * in libmobject-store.h.
 */
struct mobject_store_completion {
	completion_state_t       state;          // state of the completion
	mobject_store_callback_t cb_complete;    // completion callback
	mobject_store_callback_t cb_safe;        // safe callback
	void*                    cb_arg;         // arguments for callbacks
//	ABT_eventual             eventual;       // eventual used to notify completion
//	int*                     ret_value_ptr;  // pointer to eventual's internal value
	int                      ret_value;      // return value of the operation
	ABT_rwlock               lock;           // lock protecting access to this structure
	ABT_thread               ult;            // thread running the operation
};

#endif

