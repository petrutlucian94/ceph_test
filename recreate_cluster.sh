set -ex

CEPH_BUILD_DIR=${CEPH_BUILD_DIR:-`realpath /mnt/sdb1/ceph.linux/build`}

cd "$CEPH_BUILD_DIR"
export PATH=$PATH:$CEPH_BUILD_DIR

../src/stop.sh

DEBUG=${DEBUG:-}
CLEAN=${CLEAN:-}
SKIP_DEFAULT_IMAGES=${SKIP_DEFAULT_IMAGES:-}
IP=${IP:-192.168.122.1}
VSTART_DIR=${VSTART_DIR:-"/mnt/sdb1/vstart"}
MEMSTORE_BYTES=${MEMSTORE_BYTES:-16106127360}
BIND_IPV6=${BIND_IPV6:-"false"}
USE_MEMSTORE=${USE_MEMSTORE:-}

if [[ -n $DEBUG ]]; then
    dbg_flag="-d"
fi

if [[ -n $CLEAN ]]; then
    clean_flag="-n"

    rm -rf $VSTART_DIR/out/*
fi

mkdir -p $VSTART_DIR/out

../src/stop.sh

memstore_args=""
if [[ -n $USE_MEMSTORE ]]; then
    memstore_args="--memstore -o \"memstore_device_bytes=$MEMSTORE_BYTES\""
fi

OSD_POOL_DEFAULT_SIZE=1 VSTART_DEST=$VSTART_DIR ../src/vstart.sh \
    $dbg_flag $clean_flag $memstore_args \
    -o "ms bind ipv6 = $BIND_IPV6" \
    -i $IP \
    2>&1 | tee $VSTART_DIR/vstart.log

export CEPH_CONF=$VSTART_DIR/ceph.conf
export CEPH_KEYRING=$VSTART_DIR/keyring

sudo mkdir -p /etc/ceph
sudo cp $CEPH_CONF /etc/ceph/
sudo cp $CEPH_KEYRING /etc/ceph
# It's just a test env, let's make this easy to transfer.
sudo chmod a+rw /etc/ceph/keyring

ceph osd pool create rbd

ceph osd pool set cephfs.a.data size 1 --yes-i-really-mean-it
ceph osd pool set cephfs.a.meta size 1 --yes-i-really-mean-it
ceph osd pool set rbd size 1 --yes-i-really-mean-it

ceph tell mon.0 config set debug_mon 0
ceph tell mon.0 config set debug_ms 0
ceph tell mon.1 config set debug_mon 0
ceph tell mon.1 config set debug_ms 0
ceph tell mon.2 config set debug_mon 0
ceph tell mon.2 config set debug_ms 0


if [[ -n $CLEAN && -z $SKIP_DEFAULT_IMAGES ]]; then
    rbd create rbd_win_10g --size 10GB
    rbd create rbd_linux_10g --size 10GB
    rbd create iscsi_win_10g --size 10GB
    rbd create iscsi_linux_10g --size 10GB
fi

# sudo systemctl restart *rbd*
