AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

check_PROGRAMS = 
TESTS =
EXTRA_DIST = prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include
AM_CFLAGS =
AM_CXXFLAGS = $(AM_CFLAGS)

include_HEADERS = include/libmobject-store.h
libmobject_la_CPPFLAGS = -I${srcdir}/include

lib_LTLIBRARIES = src/libmobject-store.la
src_libmobject_store_la_SOURCES =

include Make.rules

include $(top_srcdir)/src/Makefile.subdir
include $(top_srcdir)/tests/Makefile.subdir
