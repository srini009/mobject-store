/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_LOG_H
#define __MOBJECT_LOG_H

#define STRINGIZE(x) STRINGIZE2(x)
#define STRINGIZE2(x) #x
#define LINE_STRING STRINGIZE(__LINE__)

#define MOBJECT_LOG(msg) do {\
	fprintf(stderr,__FILE__ ":" LINE_STRING " " msg "\n");\
	} while(0)

#define MOBJECT_ASSERT(cond, msg) if(!(cond)) {\
	MOBJECT_LOG("assert (" #cond ") failed. " msg);\
	exit(-1);\
	}

#endif

