/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_PREPARE_READ_OP_H
#define __MOBJECT_PREPARE_READ_OP_H

#include <margo.h>
#include "libmobject-store.h"

void prepare_read_op(margo_instance_id mid, mobject_store_read_op_t read_op);

#endif
