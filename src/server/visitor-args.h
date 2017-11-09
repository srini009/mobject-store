#ifndef __SERVER_VISITOR_ARGS_H
#define __SERVER_VISITOR_ARGS_H

#include <margo.h>
#include "libmobject-store.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
	const char* object_name;
	const char* pool_name;
    margo_instance_id mid;
    hg_addr_t   client_addr;
	hg_bulk_t   bulk_handle;
} server_visitor_args;

typedef server_visitor_args* server_visitor_args_t;

#ifdef __cplusplus
}
#endif

#endif
