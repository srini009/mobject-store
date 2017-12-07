#ifndef __SERVER_VISITOR_ARGS_H
#define __SERVER_VISITOR_ARGS_H

#include <margo.h>
#include "libmobject-store.h"
#include "src/server/mobject-server-context.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    MOBJECT_ADDR_STRING,
    MOBJECT_ADDR_HANDLE
} addr_str_type_t ;

typedef struct {
	const char*                    object_name;
	const char*                    pool_name;
    struct mobject_server_context* srv_ctx;
    union {
        const char* as_string;
        hg_addr_t   as_handle;
    } client_addr;
	hg_bulk_t                      bulk_handle;
    addr_str_type_t                client_addr_type;
} server_visitor_args;

typedef server_visitor_args* server_visitor_args_t;

#ifdef __cplusplus
}
#endif

#endif
