AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

check_PROGRAMS = 
TESTS =
EXTRA_DIST = prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include
AM_CFLAGS =
AM_CXXFLAGS = $(AM_CFLAGS)

lib_LTLIBRARIES = libmobject-store.la
libmobject_store_la_SOURCES = src/libmobject-store.c \
			src/completion.c \
			src/write-op.c \
			src/completion.h \
			src/log.h \
			src/write-op.h \
			include/rados-mobject-store.h \
			include/mobject-store.h

include_HEADERS = include/mobject-store.h
libmobject_la_CPPFLAGS = -I${srcdir}/include -I${srcdir}/src

include Make.rules

include $(top_srcdir)/src/Makefile.subdir
include $(top_srcdir)/tests/Makefile.subdir
