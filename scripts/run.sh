#! /bin/bash

set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source $SCRIPT_DIR/utilities.sh

function print_usage {
        echo "Usage: ./scripts/run.sh MODE [HOST_NUM]"
        echo "  COMPILE                  Build locally"
        echo "  COMPILE_SYNC HOST_NUM    Build and upload binaries/config"
        echo "  sync HOST_NUM            Upload binaries/config"
        echo "  init HOST_NUM            Initialize the shared CXL region"
        echo "  KILL HOST_NUM            Stop the previous experiment"
        echo "  COLLECT_OUTPUTS HOST_NUM Print VM output logs"
}

function kill_prev_exps {
        typeset HOST_NUM=$1
        typeset i=0

        echo "killing previous experiments..."
        for (( i=0; i < $HOST_NUM; ++i ))
        do
                ssh_command "pkill sequencer" $i
                # ssh_command "pkill bench_smallbank" $i
                # ssh_command "pkill bench_tatp" $i
        done
}

function delete_log_files {
        typeset MAX_HOST_NUM=8
        typeset LOG_FILE_NAME=output.txt
        typeset i=0

        echo "deleting log files..."
        for (( i=0; i < $MAX_HOST_NUM; ++i ))
        do
                ssh_command "[ -e $LOG_FILE_NAME ] && rm $LOG_FILE_NAME" $i
                ssh_command "echo 1 > /proc/sys/vm/drop_caches" $i
                ssh_command "sync" $i
        done
}

function gather_other_output {
        typeset HOST_NUM=$1

        typeset i=0

        for (( i=0; i < $HOST_NUM; ++i ))
        do
                ssh_command "cat /root/rhodes/output.txt" $i
        done
}

function sync_binaries {
        typeset HOST_NUM=$1

        cd $SCRIPT_DIR
        echo "Syncing executables and configs..."
        for (( i=0; i < $HOST_NUM; ++i ))
        do
                ssh_command "mkdir -p rhodes" $i
        done
        sync_files $SCRIPT_DIR/../build/server /root/rhodes/ $HOST_NUM
        sync_files $SCRIPT_DIR/../build/sequencer /root/rhodes/ $HOST_NUM
        sync_files $SCRIPT_DIR/../src/common/config.json /root/rhodes/ $HOST_NUM
        sync_files $SCRIPT_DIR/../build/client /root/rhodes/ $HOST_NUM



        # sync_files $SCRIPT_DIR/../build/client /root/rhodes/ $HOST_NUM
        # sync_files $SCRIPT_DIR/../build/server /root/rhodes/ $HOST_NUM

        # sync_files $SCRIPT_DIR/../build/bench_smallbank /root/pasha/ $HOST_NUM
        # sync_files $SCRIPT_DIR/../build/bench_tatp /root/pasha/ $HOST_NUM
        return 0
}

# process arguments
if [ $# -lt 1 ]; then
        print_usage
        exit -1
fi

typeset RUN_TYPE=$1
typeset HOST_NUM=${2:-0}

create_dir_for_vms "/root/rhodes" $HOST_NUM

if [ $RUN_TYPE = "init" ]; then
        kill_prev_exps $HOST_NUM
        delete_log_files
        init_cxl_for_vms $HOST_NUM
        exit 0
elif [ $RUN_TYPE = "KILL" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2

        kill_prev_exps $HOST_NUM

        exit 0
elif [ $RUN_TYPE = "COMPILE" ]; then
        if [ $# != 1 ]; then
                print_usage
                exit -1
        fi

        # compile
        cd $SCRIPT_DIR/../
        mkdir -p build
        cd build
        cmake ..
        make -j

        exit 0
elif [ $RUN_TYPE = "COMPILE_SYNC" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2

        # compile
        cd $SCRIPT_DIR/../
        mkdir -p build
        cd build
        cmake ..
        make -j

        # sync
        sync_binaries $HOST_NUM

        exit 0
elif [ $RUN_TYPE = "sync" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2

        # compile
        # cd $SCRIPT_DIR/../
        # mkdir -p build
        # cd build
        # cmake ..
        # make -j

        # sync
        sync_binaries $HOST_NUM

        exit 0
elif [ $RUN_TYPE = "CI" ]; then
        if [ $# != 3 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2
        typeset WORKER_NUM=$3

        echo "CI under construction!"

        exit -1
elif [ $RUN_TYPE = "COLLECT_OUTPUTS" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2

        gather_other_output $HOST_NUM

        exit 0
else
        print_usage
        exit -1
fi
