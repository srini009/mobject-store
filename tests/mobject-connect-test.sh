#!/bin/bash -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi
if [ -z "$MKTEMP" ] ; then
    echo expected MKTEMP variable defined to its respective command
    exit 1
fi
source $srcdir/tests/mobject-test-util.sh

TEST_DIR=`$MKTEMP -d /tmp/mobject-connect-test-XXXXXX`
CLUSTER_GID_FILE=$TEST_DIR/cluster.gid

# start 1 server with 2 second wait, 20s timeout
mobject_test_start_servers 1 2 20 $CLUSTER_GID_FILE

wait

# cleanup
rm -rf $TEST_DIR
