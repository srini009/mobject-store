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
MOBJECT_CLUSTER_FILE=$TEST_DIR/cluster.gid

##############

# start 1 server with 2 second wait, 20s timeout
mobject_test_start_servers 1 2 20 $MOBJECT_CLUSTER_FILE

##############

# run 1 connect test client
export MOBJECT_CLUSTER_FILE
run_to 10 tests/mobject-connect-test
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

##############

wait

# cleanup
rm -rf $TEST_DIR
exit 0
