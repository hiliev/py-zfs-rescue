#!/bin/bash
if [ ! -f disk0.bin ]; then
    echo "run 'make d' to generate data[0-2].bin first"
    exit 1;
fi

d=$(pwd)
cd ..;
python3 zfs_rescue.py --files=${d}/datatab.txt --label=/dev/loop0
