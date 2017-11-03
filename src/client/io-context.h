/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_IOCTX_H
#define __MOBJECT_IOCTX_H

#include <margo.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"

typedef struct mobject_ioctx {
	mobject_store_t cluster;
	char* pool_name;
}* mobject_store_ioctx_t;

#endif
