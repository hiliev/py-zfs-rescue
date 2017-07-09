import struct

from zfs.blockptr import BlockPtr
from zfs.obj_desc import DMU_TYPE_DESC

BLKPTR_OFFSET = 64


class BonusDataset:

    def __init__(self, data):
        (self.ds_dir_obj, self.ds_prev_snap_obj, self.ds_prev_snap_txg, self.ds_prev_next_obj, self.ds_snapnames_zapobj,
         self.ds_num_children, self.ds_creation_time, self.ds_creation_txg, self.ds_deadlist_obj, self.ds_used_bytes,
         self.ds_compressed_bytes, self.ds_uncompressed_bytes, self.ds_unique_bytes, self.ds_fsid_guid, self.ds_guid,
         self.ds_restoring) = struct.unpack("=16Q", data[:16*8])
        self.bptr = BlockPtr()
        self.bptr.parse(data[16*8:16*8+128])

    def __str__(self):
        fields = [
            'ds_dir_obj',
            'ds_prev_snap_obj',
            'ds_prev_snap_txg',
            'ds_prev_next_obj',
            'ds_snapnames_zapobj',
            'ds_num_children',
            'ds_creation_time',
            'ds_creation_txg',
            'ds_deadlist_obj',
            'ds_used_bytes',
            'ds_compressed_bytes',
            'ds_uncompressed_bytes',
            'ds_unique_bytes',
            'ds_fsid_guid',
            'ds_guid',
            'ds_restoring',
            'ds_bp'
        ]
        fmt = ' '.join([f + '={}' for f in fields])
        return fmt.format(
            self.ds_dir_obj,
            self.ds_prev_snap_obj,
            self.ds_prev_snap_txg,
            self.ds_prev_next_obj,
            self.ds_snapnames_zapobj,
            self.ds_num_children,
            self.ds_creation_time,
            self.ds_creation_txg,
            self.ds_deadlist_obj,
            self.ds_used_bytes,
            self.ds_compressed_bytes,
            self.ds_uncompressed_bytes,
            self.ds_unique_bytes,
            self.ds_fsid_guid,
            self.ds_guid,
            self.ds_restoring,
            self.bptr
        )


class BonusDirectory:

    def __init__(self, data):
        (
            self.dd_creation_time,
            self.dd_head_dataset_obj,
            self.dd_parent_obj,
            self.dd_clone_parent_obj,
            self.dd_child_dir_zapobj,
            self.dd_used_bytes,
            self.dd_compressed_bytes,
            self.dd_uncompressed_bytes,
            self.dd_quota,
            self.dd_reserved,
            self.dd_props_zapobj
        ) = struct.unpack("=11Q", data[:11*8])

    def __str__(self):
        fields = [
            'dd_creation_time',
            'dd_head_dataset_obj',
            'dd_parent_obj',
            'dd_clone_parent_obj',
            'dd_child_dir_zapobj',
            'dd_used_bytes',
            'dd_compressed_bytes',
            'dd_uncompressed_bytes',
            'dd_quota',
            'dd_reserved',
            'dd_props_zapobj',
        ]
        fmt = ' '.join([f+'={}' for f in fields])
        return fmt.format(
            self.dd_creation_time,
            self.dd_head_dataset_obj,
            self.dd_parent_obj,
            self.dd_clone_parent_obj,
            self.dd_child_dir_zapobj,
            self.dd_used_bytes,
            self.dd_compressed_bytes,
            self.dd_uncompressed_bytes,
            self.dd_quota,
            self.dd_reserved,
            self.dd_props_zapobj
        )


class BonusZnode:

    def __init__(self, data):
        (
            self.zp_atime, self.zp_atime_ns,
            self.zp_mtime, self.zp_mtime_ns,
            self.zp_ctime, self.zp_ctime_ns,
            self.zp_crtime, self.zp_crtime_ns,
            self.zp_gen,
            self.zp_mode,
            self.zp_size,
            self.zp_parent,
            self.zp_links,
            self.zp_xattr,
            self.zp_rdev,
            self.zp_flags,
            self.zp_uid, self.zp_gid
        ) = struct.unpack("=18Q", data[:18*8])
        self.zp_inline_content = data[264:]

    def __str__(self):
        fields = [
            'zp_atime', 'zp_atime_ns',
            'zp_mtime', 'zp_mtime_ns',
            'zp_ctime', 'zp_ctime_ns',
            'zp_crtime', 'zp_crtime_ns',
            'zp_gen',
            'zp_mode',
            'zp_size',
            'zp_parent',
            'zp_links',
            'zp_xattr',
            'zp_rdev',
            'zp_flags',
            'zp_uid', 'zp_gid'
        ]
        fmt = ' '.join([f+'={}' for f in fields])
        return fmt.format(
            self.zp_atime, self.zp_atime_ns,
            self.zp_mtime, self.zp_mtime_ns,
            self.zp_ctime, self.zp_ctime_ns,
            self.zp_crtime, self.zp_crtime_ns,
            self.zp_gen,
            self.zp_mode,
            self.zp_size,
            self.zp_parent,
            self.zp_links,
            self.zp_xattr,
            self.zp_rdev,
            self.zp_flags,
            self.zp_uid, self.zp_gid
        )


class DNode:

    def __init__(self, data=None):
        self._data = None
        self._type = None  # uint8_t 1
        self._indblkshift = None  # uint8_t 1
        self._nlevels = None  # uint8_t 1
        self._nblkptr = None  # uint8_t 1
        self._bonustype = None  # uint8_t 1
        self._checksum = None  # uint8_t 1
        self._compress = None  # uint8_t 1
        self._pad = None  # uint8_t 1
        self._datablkszsec = None  # uint16_t 2
        self._bonuslen = None  # uint16_t 2
        self._pad2 = None  # uint8_t[4] 4
        self._maxblkid = None  # uint64_t 8
        self._secphys = None  # uint64_t 8
        self._pad3 = None  # uint64_t[4] 32
        self._blkptr = None  # blkptr_t[N] @64
        self._bonus = None  # uint8_t[BONUSLEN]
        self._datablksize = None
        if data is not None:
            self.parse(data)

    def parse(self, data):
        if len(data) < 512:
            raise ValueError("Data is too small")
        # Save data for dumping purposes
        self._data = data[:]
        (self._type, self._indblkshift, self._nlevels, self._nblkptr,
         self._bonustype, self._checksum, self._compress,
         self._datablkszsec, self._bonuslen, self._maxblkid,
         self._secphys) = struct.unpack("=7Bx2H4xQQ32x", data[:BLKPTR_OFFSET])
        if self._type == 0:
            return
        # Object type > 100 (or even 53) is probably due to data error
        elif self._type > 100:
            self._invalidate()
            return
        self._blkptr = []
        if self._nblkptr > 3:
            # More than three block pointers is a sign of data error
            self._invalidate()
            return
        self._datablksize = self._datablkszsec << 9
        ptr = BLKPTR_OFFSET
        for bn in range(self._nblkptr):
            b = BlockPtr(data=data[ptr:ptr+128])
            self._blkptr.append(b)
            ptr += 128
        bonus_data = data[ptr:ptr+self._bonuslen]
        if self._bonuslen and self._bonustype == 12:
            self._bonus = BonusDirectory(bonus_data)
        elif self._bonuslen and self._bonustype == 16:
            self._bonus = BonusDataset(bonus_data)
        elif self._bonuslen and self._bonustype == 17:
            self._bonus = BonusZnode(bonus_data)
        else:
            self._bonus = bonus_data

    @property
    def blkptrs(self):
        return self._blkptr

    @property
    def maxblkid(self):
        return self._maxblkid

    @property
    def bonus(self):
        return self._bonus

    @property
    def type(self):
        return self._type

    @property
    def levels(self):
        return self._nlevels

    @property
    def datablksize(self):
        return self._datablksize

    @property
    def indblkshift(self):
        return self._indblkshift

    def dump_data(self, file_path):
        with open(file_path, 'wb') as f:
            f.write(self._data)

    def _invalidate(self):
        self._type = None

    def __str__(self):
        if self._type is None:
            return "<invalid dnode>"
        elif self._type == 0:
            return "<unallocated dnode>"
        try:
            dmu_type = DMU_TYPE_DESC[self._type]
        except IndexError:
            dmu_type = "unk_{}".format(self._type)
        bptrs = " ".join(["blkptr[{}]={}".format(i, v) for i, v in enumerate(self._blkptr)])
        bonus = " bonus[{}]".format(self._bonuslen) if self._bonuslen else ""
        if self._bonustype in [12, 16]:
            bonus += "=[{}]".format(self._bonus)
        return "[{}] {}B {}L/{} {}{}".format(dmu_type, self._maxblkid+1,
                                             self._nlevels, 1 << self._indblkshift, bptrs, bonus)

    @staticmethod
    def from_bptr(vdev, bptr, dvas=(0, 1)):
        data = None
        for dva in dvas:
            data = vdev.read_block(bptr, dva=dva)
            if data:
                break
        if data is None:
            return None
        dn = DNode()
        dn.parse(data)
        return dn
