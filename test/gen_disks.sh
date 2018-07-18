#!/bin/bash

dd if=/dev/zero of=disk0.bin bs=1024 count=$((1024*100))
dd if=/dev/zero of=disk1.bin bs=1024 count=$((1024*100))
dd if=/dev/zero of=disk2.bin bs=1024 count=$((1024*100))

losetup /dev/loop0 disk0.bin
losetup /dev/loop1 disk1.bin
losetup /dev/loop2 disk2.bin

zpool create datapool0 -f -o ashift=12 \
      -O atime=off -O canmount=off -O compression=lz4 -O normalization=formD \
      raidz /dev/loop0 /dev/loop1 /dev/loop2

zfs create datapool0/datadir

echo "data0" > /datapool0/datadir/f0.txt
echo "data1" > /datapool0/datadir/f1.txt

sync

zdb -ddddddd datapool0 > datapool0.txt

zpool export datapool0

losetup -d /dev/loop0
losetup -d /dev/loop1
losetup -d /dev/loop2

cat datatab.txt.templ | sed "s@--sed--@$(pwd)@g" > datatab.txt

cat <<EOF > ../block_server/disks_lo.tab
# Original path		Translated path
/dev/loop0 /dev/loop0
/dev/loop1 /dev/loop1
/dev/loop2 /dev/loop2
EOF

chmod a+rw \
      datatab.txt \
      disk0.bin \
      disk1.bin \
      disk2.bin \
      datapool0.txt \
      ../block_server/disks_lo.tab
