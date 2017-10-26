AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

bin_PROGRAMS =
check_PROGRAMS =
TESTS =
EXTRA_DIST = prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include
AM_CFLAGS   = 
AM_CXXFLAGS = $(AM_CFLAGS)

lib_LTLIBRARIES = \
  src/libmobject-store.la \
  src/libmobject-server.la

include_HEADERS = \
  include/libmobject-store.h \
  include/librados-mobject-store.h \
  include/mobject-server.h

# setting this to empty so subdir makefiles can use +=
noinst_HEADERS =
noinst_LTLIBRARIES =

include Make.rules

include $(top_srcdir)/src/Makefile.subdir
include $(top_srcdir)/tests/Makefile.subdir
include $(top_srcdir)/tests/io-chain/Makefile.subdir
