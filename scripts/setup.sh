#! /bin/bash

# set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

function print_usage {
        echo "[usage] ./setup.sh [HOST/VMS] EXP-SPECIFIC"
        echo "HOST: None"
        echo "VMS: HOST_NUM"
}

if [ $# -lt 1 ]; then
        print_usage
        exit -1
fi

typeset TASK_TYPE=$1

source $SCRIPT_DIR/utilities.sh

if [ $TASK_TYPE = "HOST" ]; then
        if [ $# != 1 ]; then
                print_usage
                exit -1
        fi

        # tool chains
        sudo apt-get install -y cmake gcc-12 g++-12 clang-15 clang++-15 lld-15 cargo

        # libraries - install boost packages individually to avoid UCX conflict
        sudo apt-get install -y libjemalloc-dev libgoogle-glog-dev libgtest-dev
        
        # Install specific boost libraries instead of libboost-all-dev to avoid UCX conflict
        echo "Installing boost libraries individually..."
        for pkg in libboost-system-dev libboost-thread-dev libboost-filesystem-dev libboost-program-options-dev libboost-regex-dev libboost-serialization-dev libboost-iostreams-dev; do
            sudo apt-get install -y $pkg || echo "Failed to install $pkg, continuing..."
        done

        # required by VM-based emulation
        sudo apt-get install -y python3 python3-pip mkosi ovmf numactl python3-pyroute2

        # required by parsing and plotting - use system packages instead of pip
        sudo apt-get install -y python3-pandas python3-matplotlib msttcorefonts -qq
        rm ~/.cache/matplotlib -rf           # remove cache

        # setup ssh key
        [ -f $HOME/.ssh/id_rsa ] || ssh-keygen -t rsa -N "" -f $HOME/.ssh/id_rsa
        cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys

        exit 0
elif [ $TASK_TYPE = "VMS" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi
        typeset HOST_NUM=$2
        echo "Setting up VMs..."

        # sync kernel module
        echo "Sync kernel module..."
        sync_files $SCRIPT_DIR/../dependencies/kernel_module/cxl_init /root/cxl_init $HOST_NUM
        sync_files $SCRIPT_DIR/../dependencies/kernel_module/cxl_recover_meta /root/cxl_recover_meta $HOST_NUM
        sync_files $SCRIPT_DIR/../dependencies/kernel_module/cxl_ivpci.ko /root/cxl_ivpci.ko $HOST_NUM

        # sync dependencies
        echo "Sync dependencies..."
        sync_files /lib/x86_64-linux-gnu/libjemalloc.so.2 /lib/x86_64-linux-gnu/libjemalloc.so.2 $HOST_NUM
        sync_files /lib/x86_64-linux-gnu/libjemalloc.so.2 /root/libjemalloc.so.2 $HOST_NUM
        sync_files $SCRIPT_DIR/../dependencies/kernel_module/libglog.so.0 /lib/x86_64-linux-gnu/libglog.so.0 $HOST_NUM
        sync_files $SCRIPT_DIR/../dependencies/kernel_module/libgflags.so.2.2 /lib/x86_64-linux-gnu/libgflags.so.2.2 $HOST_NUM
        sync_files /lib/x86_64-linux-gnu/libspdlog.so.1.9.2 /lib/x86_64-linux-gnu/libspdlog.so.1.9.2 $HOST_NUM
        sync_files /lib/x86_64-linux-gnu/libfmt.so.8.1.1 /lib/x86_64-linux-gnu/libfmt.so.8.1.1 $HOST_NUM
        sync_files $SCRIPT_DIR/../dependencies/kernel_module/lib /root $HOST_NUM        
        sync_files $SCRIPT_DIR/../dependencies/kernel_module/ld-linux-x86-64.so.2 /root/ $HOST_NUM        

        # setup the VM(s)
        echo "Loading kernel module..."
        for (( i=0; i < $HOST_NUM; ++i ))
        do
                ssh_command "ln -sf libspdlog.so.1.9.2 /lib/x86_64-linux-gnu/libspdlog.so.1" $i
                ssh_command "ln -sf libfmt.so.8.1.1 /lib/x86_64-linux-gnu/libfmt.so.8" $i
                ssh_command "ldconfig" $i
                ssh_command "rmmod cxl_ivpci 2>/dev/null" $i
                ssh_command "insmod ./cxl_ivpci.ko 2>/dev/null" $i
        done

        echo "Finished"
        exit 0
else
        print_usage
        exit -1
fi
