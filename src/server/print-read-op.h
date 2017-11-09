#ifndef __EXEC_READ_OP_H
#define __EXEC_READ_OP_H

#include <margo.h>
#include "libmobject-store.h"

void execute_read_op(mobject_store_read_op_t read_op, const char* object_name);

#endif
