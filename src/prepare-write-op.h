/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_PREPARE_WRITE_OP_H
#define __MOBJECT_PREPARE_WRITE_OP_H

#include <margo.h>
#include "libmobject-store.h"

void prepare_write_op(margo_instance_id mid, mobject_store_write_op_t write_op);

#endif
