#!/usr/bin/env python3

# Copyright (c) 2017 Hristo Iliev <github@hiliev.eu>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from block_proxy.proxy import BlockProxy
from zfs.label import Label
from zfs.zap import zap_factory
from zfs.dataset import Dataset
from zfs.objectset import ObjectSet
from zfs.zio import RaidzDevice             # or MirrorDevice
from zfs.blocktree import BlockTree

import argparse

from os import path

parser = argparse.ArgumentParser(description='zfs_rescue')
parser.add_argument('--verbose', '-v', dest='verbose', action='count', default=0)
parser.add_argument('--files', '-f', dest='files', type=str, default=None,
                    help='Read blocks from files, specify disks.tab location')
parser.add_argument('--label', '-l', dest='label', type=str, default='/dev/dsk/c3t0d0s7',
                    help='Device where to read the initial label from')
args = parser.parse_args()

if args.verbose > 0:
    BlockTree.VERBOSE_TRAVERSE = 1

BLK_PROXY_ADDR = ("localhost", 24892)       # network block server
if not args.files is None:
    BLK_PROXY_ADDR = ("files:", args.files)  # local device nodes
BLK_INITIAL_DISK = args.label      # device to read the label from

TXG = -1                                    # select specific transaction or -1 for the active one

TEMP_DIR = "/tmp"
OUTPUT_DIR = "rescued"
DS_TO_ARCHIVE = []
DS_OBJECTS = []                             # objects to export
DS_OBJECTS_SKIP = []                        # objects to skip
DS_SKIP_TRAVERSE = []                       # datasets to skip while exporting file lists
FAST_ANALYSIS = True

print("[+] zfs_rescue v0.3183")

lnum = 0
print("[+] Reading label {} on disk {}".format(lnum, BLK_INITIAL_DISK))
bp = BlockProxy(BLK_PROXY_ADDR)
id_l = Label(bp, BLK_INITIAL_DISK)
id_l.read(0)
id_l.debug()
all_disks = id_l.get_vdev_disks()

pool_dev = RaidzDevice(all_disks, 1, BLK_PROXY_ADDR, bad=[3], ashift=id_l._ashift, repair=True, dump_dir=OUTPUT_DIR)
# pool_dev = MirrorDevice(all_disks, BLK_PROXY_ADDR, dump_dir=OUTPUT_DIR)

print("[+] Loading uberblocks from child vdevs")
uberblocks = {}
for disk in all_disks:
    bp = BlockProxy(BLK_PROXY_ADDR)
    l0 = Label(bp, disk)
    l0.read(0)
    l1 = Label(bp, disk)
    l1.read(1)
    ub = l0.find_active_ub()
    ub_found = " (active UB txg {})".format(ub.txg) if ub is not None else ""
    print("[+]  Disk {}: L0 txg {}{}, L1 txg {}".format(disk, l0.get_txg(), ub_found, l1.get_txg()))
    uberblocks[disk] = ub

# print("\n[+] Active uberblocks:")
# for disk in uberblocks.keys():
#     print(disk)
#     uberblocks[disk].debug()

ub = id_l.find_ub_txg(TXG)
if ub:
    root_blkptr = ub.rootbp
    print("[+] Selected uberblock with txg", TXG)
else:
    root_blkptr = uberblocks[BLK_INITIAL_DISK].rootbp
    print("[+] Selected active uberblock from initial disk")

print("[+] Reading MOS: {}".format(root_blkptr))

datasets = {}

# Try all copies of the MOS
for dva in range(3):
    mos = ObjectSet(pool_dev, root_blkptr, dvas=(dva,))
    for n in range(len(mos)):
        d = mos[n]
        # print("[+]  dnode[{:>3}]={}".format(n, d))
        if d and d.type == 16:
            datasets[n] = d

print("[+] add one level of child datasets")
try:
    rds_z = mos[1]
    rds_zap = zap_factory(pool_dev, rds_z)
    rds_id = rds_zap['root_dataset']
    rdir = mos[rds_id]
    cdzap_id = rdir.bonus.dd_child_dir_zapobj
    cdzap_z = mos[cdzap_id]
    cdzap_zap = zap_factory(pool_dev, cdzap_z)
    for k,v in cdzap_zap._entries.items():
        if not k[0:1] == '$': 
            child = mos[v]
            cds = child.bonus.dd_head_dataset_obj
            print("[+] child %s with dataset %d" %(k,cds))
            # mos[cds] points to a 'zap' with "bonus  DSL dataset"
            datasets[cds] = mos[cds]
except:
    pass
    
print("[+] {} datasets found".format(len(datasets)))

for dsid in datasets:
    print("[+] Dataset", dsid)
    ds_dnode = datasets[dsid]
    print("[+]  dnode {}".format(ds_dnode))
    print("[+]  creation timestamp {}".format(ds_dnode.bonus.ds_creation_time))
    print("[+]  creation txg {}".format(ds_dnode.bonus.ds_creation_txg))
    print("[+]  {} uncompressed bytes".format(ds_dnode.bonus.ds_uncompressed_bytes))
    if FAST_ANALYSIS:
        continue
    ddss = Dataset(pool_dev, ds_dnode)
    ddss.analyse()
    if dsid not in DS_SKIP_TRAVERSE:
        ddss.export_file_list(path.join(OUTPUT_DIR, "ds_{}_filelist.csv".format(dsid)))

for dsid in DS_TO_ARCHIVE:
    ddss = Dataset(pool_dev, datasets[dsid], dvas=(0,1))
    ddss.analyse()
    # ddss.prefetch_object_set()
    if len(DS_OBJECTS) > 0:
        for dnid, objname in DS_OBJECTS:
            ddss.archive(path.join(OUTPUT_DIR, "ds_{}_{}.tar".format(dsid, objname)),
                         dir_node_id=dnid, skip_objs=DS_OBJECTS_SKIP, temp_dir=TEMP_DIR)
    else:
        ddss.archive(path.join(OUTPUT_DIR, "ds_{}.tar".format(dsid)), skip_objs=DS_OBJECTS_SKIP, temp_dir=TEMP_DIR)
