AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

EXTRA_DIST = prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include
AM_CFLAGS =
AM_CXXFLAGS = $(AM_CFLAGS)

lib_LTLIBRARIES = libmobject-store.la
libmobject_store_la_SOURCES = src/libmobject-store.c \
			include/mobject-store.h

include_HEADERS = include/mobject-store.h
libmobject_la_CPPFLAGS = -I${srcdir}/include

include Make.rules

