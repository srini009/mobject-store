AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

check_PROGRAMS = 
TESTS =
EXTRA_DIST = prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include
AM_CFLAGS =
AM_CXXFLAGS = $(AM_CFLAGS)

lib_LTLIBRARIES = src/libmobject-store.la
lib_LTLIBRARIES += src/libmobject-store-server.la

include_HEADERS = include/libmobject-store.h \
		  include/librados-mobject-store.h \
		  include/mobject-server.h

# setting this to empty so subdir makefiles can use +=
noinst_HEADERS =

src_libmobject_la_CPPFLAGS = -I${srcdir}/include -I${srcdir}/src

include Make.rules

include $(top_srcdir)/src/Makefile.subdir
include $(top_srcdir)/tests/Makefile.subdir
