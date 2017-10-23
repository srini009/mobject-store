AC_PREREQ([2.69])
AC_INIT([mobject], [0.1], [ssio-services@lists.mcs.anl.gov])
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects silent-rules])
AM_SILENT_RULES([yes])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([include/libmobject-store.h])
AC_CONFIG_HEADER([src/mobject-store-config.h])
LT_PREREQ([2.2])
AM_PROG_AR
LT_INIT([dlopen])

# checks for programs
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_INSTALL

# checks for libraires
#pkg-config and libraries supporting that
PKG_PROG_PKG_CONFIG
if test "x$PKG_CONFIG" == "x"; then
	AC_MSG_ERROR([Could not find pkg-config utility!])
fi

dnl seems like this should be a function but my first stab at m4 exploded
dnl mightily...
dnl also, we need to separate client libraries from server libraries.  Just jam
dnl it all into one set of flags for now

PKG_CHECK_MODULES([MARGO],[margo],[],
	AC_MSG_ERROR([Could not find working margo installation!]) )
LIBS="$MARGO_LIBS $LIBS"
CPPFLAGS="$MARGO_CFLAGS $CPPFLAGS"
CFLAGS="$MARGO_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([SSG],[ssg],[],
	AC_MSG_ERROR([Could not find working ssg installation!]) )
LIBS="$SSG_LIBS $LIBS"
CPPFLAGS="$SSG_CFLAGS $CPPFLAGS"
CFLAGS="$SSG_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([ARGOBOTS],[argobots],[],
	AC_MSG_ERROR([Could not find working argobots installation!]) )
LIBS="$ARGOBOTS_LIBS $LIBS"
CPPFLAGS="$ARGOBOTS_CFLAGS $CPPFLAGS"
CFLAGS="$ARGOBOTS_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([SDSKEYVALSERVER],[kv-server],[],
	AC_MSG_ERROR([Could not find working sds-keyval server installation!]) )
LIBS="$SDSKEYVALSERVER_LIBS $LIBS"
CPPFLAGS="$SDSKEYVALSERVER_CFLAGS $CPPFLAGS"
CFLAGS="$SDSKEYVALSERVER_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([SDSKEYVALCLIENT],[kv-client],[],
	AC_MSG_ERROR([Could not find working sds-keyval client installation!]) )
LIBS="$SDSKEYVALCLIENT_LIBS $LIBS"
CPPFLAGS="$SDSKEYVALCLIENT_CFLAGS $CPPFLAGS"
CFLAGS="$SDSKEYVALCLIENT_CFLAGS $CFLAGS"


PKG_CHECK_MODULES([PMEM],[libpmemobj],[],
	AC_MSG_ERROR([Could not find working libpmemobj installation!]) )
LIBS="$PMEM_LIBS $LIBS"
CPPFLAGS="$PMEM_CFLAGS $CPPFLAGS"
CFLAGS="$PMEM_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([BAKE],[bake-bulk-server],[],
	AC_MSG_ERROR([Could not find working BAKE installation!]) )
LIBS="$BAKE_LIBS $LIBS"
CPPFLAGS="$BAKE_CFLAGS $CPPFLAGS"
CFLAGS="$BAKE_CFLAGS $CFLAGS"



# check for RADOS
AC_ARG_WITH([rados],
    AS_HELP_STRING([--with_rados], [Additionally build tests against librados (default is no)]),
    [], [])
if test x"$with_rados" == "xyes" ; then
    # check for RADOS headers and libraries
    AC_CHECK_HEADERS([rados/librados.h rados/rados_types.h],
        [],
        [AC_MSG_ERROR([RADOS support requested but could not find RADOS headers!])],
        [])
    AC_CHECK_LIB([rados], [rados_create],
        [],
        [AC_MSG_ERROR([RADOS support requested but could not link against librados!])])

fi

# both found, set output to additionally build tests against RADOS
AM_CONDITIONAL(HAVE_RADOS, test x"$with_rados" == "xyes")

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
