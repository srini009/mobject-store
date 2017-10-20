/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_BUFFER_UNION_H
#define __MOBJECT_BUFFER_UNION_H

typedef union {
	const char* as_pointer;
	uint64_t    as_offset;
} buffer_u;

#endif

