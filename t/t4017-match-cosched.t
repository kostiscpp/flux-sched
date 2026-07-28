#!/bin/sh
# set -x 

test_description='Test cosched Aware Scheduler On Small System.'

. $(dirname $0)/sharness.sh

cmd_dir="${SHARNESS_TEST_SRCDIR}/data/resource/commands/cosched"
exp_dir="${SHARNESS_TEST_SRCDIR}/data/resource/expected/cosched"
grugs="${SHARNESS_TEST_SRCDIR}/data/resource/grugs/small.graphml"
query="../../resource/utilities/resource-query"

cmds001="${cmd_dir}/cmds01.in"
test001_desc="cosched policy works on small graph"
test_expect_success "${test001_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${cmds001} > cmds001 &&
    ${query} -L ${grugs} -S CA -P cosched -F rv1_nosched -t 001.R.out < cmds001 &&
    test_cmp 001.R.out ${exp_dir}/001.R.out
'

test_done