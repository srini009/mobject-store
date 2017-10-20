/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_BUFFER_UNION_H
#define __MOBJECT_BUFFER_UNION_H

/**
 * This union is defined to be used in read and write actions
 * involving either a local pointer (const char*) or an offset
 * within a hg_bulk_t object (unt64_t).
 */
typedef union {
	const char* as_pointer;
	uint64_t    as_offset;
} buffer_u;

#endif

