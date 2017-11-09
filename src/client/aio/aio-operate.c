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

int mobject_store_aio_write_op_operate(mobject_store_write_op_t write_op,
                                       mobject_store_ioctx_t io,
                                       mobject_store_completion_t completion,
                                       const char *oid,
                                       time_t *mtime,
                                       int flags)
{

}


int mobject_store_aio_read_op_operate(mobject_store_read_op_t read_op,
                                      mobject_store_ioctx_t io,
                                      mobject_store_completion_t completion,
                                      const char *oid,
                                      int flags)
{

}
