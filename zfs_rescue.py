from block_proxy.proxy import BlockProxy
from zfs.label import Label
from zfs.dataset import Dataset
from zfs.objectset import ObjectSet
from zfs.zio import RaidzDevice  # or MirrorDevice

from os import path

BLK_PROXY_ADDR = ("localhost", 24892)       # network block server
# BLK_PROXY_ADDR = ("files:", "disks.tab")  # local device nodes

BLK_INITIAL_DISK = "/dev/dsk/c3t0d0s7"      # device to read the label from
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

pool_dev = RaidzDevice(all_disks, 1, BLK_PROXY_ADDR, bad=[3], repair=True, dump_dir=OUTPUT_DIR)
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
    mos = ObjectSet(pool_dev, root_blkptr, dva=dva)
    for n in range(len(mos)):
        d = mos[n]
        # print("[+]  dnode[{:>3}]={}".format(n, d))
        if d and d.type == 16:
            datasets[n] = d

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
    ddss = Dataset(pool_dev, datasets[dsid], dva=1)
    ddss.analyse()
    # ddss.prefetch_object_set()
    if len(DS_OBJECTS) > 0:
        for dnid, objname in DS_OBJECTS:
            ddss.archive(path.join(OUTPUT_DIR, "ds_{}_{}.tar".format(dsid, objname)),
                         dir_node_id=dnid, skip_objs=DS_OBJECTS_SKIP, temp_dir=TEMP_DIR)
    else:
        ddss.archive(path.join(OUTPUT_DIR, "ds_{}.tar".format(dsid)), skip_objs=DS_OBJECTS_SKIP, temp_dir=TEMP_DIR)
