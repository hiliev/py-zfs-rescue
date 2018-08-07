#!/bin/bash
if [ ! -f disk0.bin ]; then
    echo "run 'make d' to generate data[0-2].bin first"
    exit 1;
fi

if ! losetup -l | grep /dev/loop0; then
    echo "run 'make lo' to setup /dev/loop[0-2]"
    exit 1;
fi

d=$(pwd)

cat <<EOF > /tmp/_server_start.sh
pkill -9 -f server.py
$(readlink -f ../block_server/server.py) --config=$(readlink -f ${d}/../block_server/disks_lo.tab) &
echo -n $! > /tmp/_server_pid.txt
EOF
chmod a+rwx /tmp/_server_start.sh
sudo bash /tmp/_server_start.sh

sleep 1
python3 ../zfs_rescue.py -v -C --label=/dev/loop0

pkill -9 -f server.py
