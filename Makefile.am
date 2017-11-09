AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

# setting this to empty so subdir makefiles can use +=
noinst_HEADERS =
noinst_LTLIBRARIES =
noinst_PROGRAMS =
bin_PROGRAMS =
check_PROGRAMS =
TESTS =
TESTS_ENVIRONMENT=
EXTRA_DIST = prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include
AM_CFLAGS   = 
AM_CXXFLAGS = -std=c++14 $(AM_CFLAGS)

SERVER_LIBS = @SERVER_LIBS@
CLIENT_LIBS = @CLIENT_LIBS@

lib_LTLIBRARIES = \
  src/client/libmobject-store.la \
  src/server/libmobject-server.la

include_HEADERS = \
  include/libmobject-store.h \
  include/librados-mobject-store.h \
  include/mobject-server.h

include Make.rules

include $(top_srcdir)/src/Makefile.subdir
include $(top_srcdir)/tests/Makefile.subdir
include $(top_srcdir)/tests/io-chain/Makefile.subdir
