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


from zfs.blockptr import BlockPtrArray
from zfs.zio import dumppacket
from zfs.dnode import BLKPTR_OFFSET
import struct

MZAP_ENT_LEN = 64
MZAP_NAME_LEN = (MZAP_ENT_LEN - 8 - 4 - 2)

ZBT_MICRO = (1 << 63) + 3
ZBT_HEADER = (1 << 63) + 1
ZBT_LEAD = (1 << 63) + 0

ZAP_LEAF_ARRAY = 251
ZAP_LEAF_ENTRY = 252
ZAP_LEAF_FREE = 253

# /* 0 */ "not specified",
# /* 1 */ "FIFO",
# /* 2 */ "Character Device",
# /* 3 */ "3 (invalid)",
# /* 4 */ "Directory",
# /* 5 */ "5 (invalid)",
# /* 6 */ "Block Device",
# /* 7 */ "7 (invalid)",
# /* 8 */ "Regular File",
# /* 9 */ "9 (invalid)",
# /* 10 */ "Symbolic Link",
# /* 11 */ "11 (invalid)",
# /* 12 */ "Socket",
# /* 13 */ "Door",
# /* 14 */ "Event Port",
# /* 15 */ "15 (invalid)",
TYPECODES = "-pc-d-b-f-l-soe-"

ZAP_CHNK_ARR_BEGIN = 0x430
ZAP_CHNK_SIZE = 24


def safe_decode_string(val):
    try:
        return val.decode("utf-8")
    except UnicodeDecodeError:
        # If not UTF-8, probably CP1251 from Windows clients
        # Use Latin1 as last resort
        try:
            return val.decode("cp1251")
        except UnicodeDecodeError:
            return val.decode("latin1")


class MicroZap:

    def __init__(self):
        self._type = None
        self._salt = None
        self._entries = {}

    def parse(self, data):
        if len(data) < 128:
            print("[-]  Not enough data to fill a Micro Zap")
            return
        (self._type, self._salt) = struct.unpack("=QQ", data[:16])
        if self._type != ZBT_MICRO:
            print("[-]  Not a Micro Zap: type={}".format(hex(self._type)))
            return
        ptr = 64
        nents = (len(data) - ptr) // 64
        for n in range(nents):
            e = self._parse_entry(data[ptr:ptr+64])
            if e[0]:
                self._entries[e[0]] = e[1]
            ptr += 64

    def debug(self, as_dir=False):
        print("[=]  Micro Zap (type {}) content".format(hex(self._type)))
        if self._type != ZBT_MICRO:
            return
        if as_dir:
            for name in self._entries:
                val = self._entries[name]
                t = val >> 60
                v = val & ~(15 << 60)
                kind = TYPECODES[t]
                print('{} {} @ {}'.format(kind, name, v))
        else:
            for name in self._entries:
                print('[=]  {}={}'.format(name, self._entries[name]))

    def keys(self):
        return self._entries.keys()

    @staticmethod
    def _parse_entry(data):
        (v, cd, name) = struct.unpack("=QL2x50s", data[:64])
        return safe_decode_string(name.rstrip(b'\0')), v, cd

    def __getitem__(self, item):
        return self._entries.get(item)


class FatZap:

    def __init__(self, dbsize):
        self._type = None
        self._dbsize = dbsize
        self._entries = {}

    def parse(self, data):
        # Parse the zap_phys_t structure
        zpt = struct.unpack("=11Q", data[:11*8])
        fields = [
            'zap_block_type',
            'zap_magic',
            'zap_ptrtbl.zt_blk',
            'zap_ptrtbl.zt_numblks',
            'zap_ptrtbl.zt_shift',
            'zap_ptrtbl.zt_nextblk',
            'zap_ptrtbl.zt_blk_copied',
            'zap_freeblk',
            'zap_num_leafs',
            'zap_num_entries',
            'zap_salt',
        ]
        fmt = ' '.join(f+"={}" for f in fields)
        print("[+]  Fat Zap header:", fmt.format(*zpt))
        if (zpt[3] == 0): # embedded zap ptrtable
            self._embedptrs= data[self._dbsize//2:self._dbsize]; # not used but extract anyway
        block_size = self._dbsize
        nblocks = (len(data) - block_size) // block_size
        for n in range(1, nblocks+1):
            self._parse_zap_block(data[n*block_size:(n+1)*block_size])

    def debug(self, as_dir=False):
        pass

    def keys(self):
        return self._entries.keys()

    def _parse_zap_block(self, data):
        header = struct.unpack("=QQQLHHHH12x", data[:0x30])
        fields = [
            'lhr_block_type',
            'lhr_next',
            'lhr_prefix',
            'lhr_magic',
            'lhr_nfree',
            'lhr_nentries',
            'lhr_prefix_len',
            'lh_freelist',
        ]
        fmt = ' '.join([f+"={}" for f in fields])
        print("[+]  Fat Zap block:", fmt.format(*header))
        chunk_table = struct.unpack("=512H", data[0x30:ZAP_CHNK_ARR_BEGIN])
        for c in chunk_table:
            if c == 0xffff:
                continue
            self._follow_collision_chain(data[ZAP_CHNK_ARR_BEGIN:], c)

    def _follow_collision_chain(self, chunk_arr, idx):
        chunk_begin = idx*ZAP_CHNK_SIZE
        chunk_type = chunk_arr[chunk_begin]
        if chunk_type != ZAP_LEAF_ENTRY:
            print("[-]  Expected ZAP leaf entry, got {}".format(chunk_type))
        else:
            (int_size, next_chunk, name_chunk, name_length, value_chunk, value_length, cd, _hash) = struct.unpack(
                "=B6H2xQ", chunk_arr[chunk_begin+1:chunk_begin+ZAP_CHNK_SIZE])
            name_data = self._follow_chunk_list(chunk_arr, name_chunk)[:name_length-1]
            value_data = self._follow_chunk_list(chunk_arr, value_chunk)[:value_length * int_size]
            # TODO: Implement extraction of arrays
            # For now assume values are single qwords
            name = safe_decode_string(name_data)
            if not len(value_data) == 8:
                value = value_data
            else:
                value = struct.unpack(">Q", value_data)[0]
            self._entries[name] = value
            if next_chunk != 0xffff:
                self._follow_collision_chain(chunk_arr, next_chunk)

    def _follow_chunk_list(self, chunk_arr, idx):
        chunk = chunk_arr[idx*ZAP_CHNK_SIZE:(idx+1)*ZAP_CHNK_SIZE]
        chunk_type = chunk[0]
        if chunk_type != ZAP_LEAF_ARRAY:
            print("[-]  Expected ZAP array entry, got {}".format(chunk_type))
            return None
        chunk_data = chunk[1:22]
        (next_chunk,) = struct.unpack("=H", chunk[-2:])
        if next_chunk == 0xffff:
            return chunk_data
        else:
            return chunk_data + self._follow_chunk_list(chunk_arr, next_chunk)

    def __getitem__(self, item):
        return self._entries.get(item)

def _choose_zap_factory(data, dbsize):
    (block_type,) = struct.unpack("=Q", data[:8])
    if block_type == ZBT_MICRO:
        zap = MicroZap()
    elif block_type == ZBT_HEADER:
        zap = FatZap(dbsize)
    else:
        print("[-]  Data is not a ZAP object: type={}".format(hex(block_type)))
        return None
    zap.parse(data)
    return zap

def _blockptrar_zap_factory(vdev, bpa, dbsize, nblocks):
    data = bytearray()
    for i in range(nblocks):
        d = vdev.read_block(bpa[i])
        if not (d is None):
            data += d
    return _choose_zap_factory(data, dbsize)

def _indirect_zap_factory(vdev, bptr, dbsize, nblocks):
    data = vdev.read_block(bptr)
    if data is None:
        return None
    # Data contains first indirection block
    bpa = BlockPtrArray(data)
    return _blockptrar_zap_factory(vdev, bpa, dbsize, nblocks)
    
def zap_factory(vdev, dnode):
    bptr = dnode.blkptrs[0]
    dbsize = dnode.datablksize
    if dnode.levels == 1:
        nblocks = dnode._nblkptr
        bpa = BlockPtrArray(dnode._data[BLKPTR_OFFSET:BLKPTR_OFFSET+nblocks*128])
        return _blockptrar_zap_factory(vdev, bpa, dbsize, nblocks)
    elif dnode.levels == 2:
        return _indirect_zap_factory(vdev, bptr, dbsize, dnode.maxblkid+1)
    # TODO: Implement Fat Zap with BlockTree
    raise NotImplementedError("Deeper ZAPs not supported yet")
