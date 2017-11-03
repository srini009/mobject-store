/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/client/aio/completion.h"
#include "src/util/log.h"

int mobject_store_aio_create_completion(void *cb_arg,
                                mobject_store_callback_t cb_complete,
                                mobject_store_callback_t cb_safe,
                                mobject_store_completion_t *pc)
{
	int r;
	mobject_store_completion_t completion = 
		(mobject_store_completion_t)calloc(1, sizeof(struct mobject_store_completion));
	MOBJECT_ASSERT(completion != 0, "Could not allocate mobject_store_completion_t object"); 
	completion->cb_complete   = cb_complete;
	completion->cb_safe       = cb_safe;
	completion->cb_arg        = cb_arg;
    completion->request       = MARGO_REQUEST_NULL;
	*pc = completion;
	return 0;
}

int mobject_store_aio_wait_for_complete(mobject_store_completion_t c)
{
	if(c == MOBJECT_COMPLETION_NULL) {
		MOBJECT_LOG("Warning: passing NULL to mobject_store_aio_wait_for_complete");
		return -1;
	}
    
    MOBJECT_ASSERT(c->request != MARGO_REQUEST_NULL, "Invalid completion handle");
    c->ret_value = margo_wait(c->request);
    if(c->ret_value != HG_SUCCESS) {
        MOBJECT_LOG("Warning: margo_wait returned something different from HG_SUCCESS");
    }
    c->request = MARGO_REQUEST_NULL;

    if(c->cb_safe)
        (c->cb_safe)(c, c->cb_arg);

    if(c->cb_complete)
        (c->cb_complete)(c, c->cb_arg);

	return 0;
}

int mobject_store_aio_is_complete(mobject_store_completion_t c)
{
    MOBJECT_ASSERT(0,"mobject_store_aio_is_complete is not yet implemented");
    return 0;
}

int mobject_store_aio_get_return_value(mobject_store_completion_t c)
{
	int r;
	if(c == MOBJECT_COMPLETION_NULL) {
		MOBJECT_LOG("Warning: passing NULL to mobject_store_aio_get_return_value");
		return 0;
	}
    MOBJECT_ASSERT((c->request == MARGO_REQUEST_NULL), 
        "calling mobject_store_aio_get_return_value on a non-terminated completion");
	return c->ret_value;
}

void mobject_store_aio_release(mobject_store_completion_t c)
{
    if(c == MOBJECT_COMPLETION_NULL) return;
    MOBJECT_ASSERT(c->request != MARGO_REQUEST_NULL,
        "Trying to release a completion handle before operation completed");
	free(c);
}
