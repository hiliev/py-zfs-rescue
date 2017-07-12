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
from zfs.lzjb import lzjb_decompress

from os import path

LOG_QUIET = 0
LOG_VERBOSE = 1
LOG_NOISY = 5


def roundup(x, y):
    return ((x + y - 1) // y) * y


class GenericDevice:

    def __init__(self, child_devs, block_provider_addr, dump_dir="/tmp"):
        self._devs = child_devs
        self._bp = BlockProxy(block_provider_addr)
        self._dump_dir = dump_dir
        self._verbose = LOG_QUIET

    def set_verbosity_level(self, level):
        self._verbose = level

    def read_block(self, bptr, dva=0, debug_dump=False, debug_prefix="block"):
        if bptr.get_dva(dva).gang:
            # TODO: Implement gang blocks
            raise NotImplementedError("Gang blocks are still not supported")
        offset = bptr.get_dva(dva).offset
        psize = bptr.psize
        if offset == 0 and psize == 0:
            return None
        lsize = bptr.lsize
        if self._verbose >= LOG_VERBOSE:
            print("[+] Reading block at {}:{}".format(hex(offset)[2:], hex(psize)[2:]))
        data = self._read_physical(offset, psize, debug_dump, debug_prefix)
        if bptr.compressed:
            if bptr.comp_alg in [1, 3]:
                if self._verbose >= LOG_VERBOSE:
                    print("[+]  Decompressing with LZJB")
                try:
                    data = lzjb_decompress(data, lsize)
                except:
                    data = None
                if data is None:
                    if self._verbose >= LOG_VERBOSE:
                        print("[-]   Decompression failed")
                    return None
            else:
                if self._verbose >= LOG_VERBOSE:
                    print("[-]  Unsupported compression algorithm")
                return None
                # data = lz4.frame.decompress(data)
            if len(data) < lsize:
                data += b'\0' * (lsize - len(data))
        if debug_dump:
            f = open(path.join(self._dump_dir, "{}.raw".format(debug_prefix)), "wb")
            f.write(data)
            f.close()
        return data

    def _read_physical(self, offset, psize, debug_dump, debug_prefix):
        raise RuntimeError("Attempted read from generic device!")


class MirrorDevice(GenericDevice):

    def __init__(self, child_vdevs, proxy_addr, bad=None, dump_dir="/tmp"):
        super().__init__(child_vdevs, proxy_addr, dump_dir=dump_dir)
        self._bad = bad
        if self._bad and len(self._bad) > len(self._devs):
            print("[-] Mirror created with more bad disks than copies!")

    def _read_physical(self, offset, psize, debug_dump, debug_prefix):
        if self._verbose >= LOG_NOISY:
            print("[+]  Reading from {}:{}:{}".format(self._devs[0], offset, psize))
        data = self._bp.read(self._devs[0], offset + 0x400000, psize)
        if debug_dump:
            f = open(path.join(self._dump_dir, "{}-{}:{}.raw".format(debug_prefix, 0, offset)), "wb")
            f.write(data)
            f.close()
        return data


class RaidzDevice(GenericDevice):

    def __init__(self, child_vdevs, nparity, proxy_addr, bad=None, repair=False, dump_dir="/tmp"):
        super().__init__(child_vdevs, proxy_addr, dump_dir=dump_dir)
        self._nparity = nparity
        self._bad = bad
        self._repair = repair
        if self._nparity != 1:
            print("[-] Raidz with parity != 1 is not supported!")
        if self._bad and len(self._bad) > self._nparity:
            print("[-] Raidz created with more bad disks than parity allows!")

    def _read_physical(self, offset, psize, debug_dump, debug_prefix):
        (cols, firstdatacol, skipstart) = self._map_alloc(offset, psize, 9)
        col_data = []
        blockv = []
        for c in range(len(cols)):
            col = cols[c]
            devidx = col["rc_devidx"]
            offset = col["rc_offset"]
            size = col["rc_size"]
            if debug_dump:
                p = "" if c >= firstdatacol else " (parity)"
                bad = " BAD" if devidx in self._bad else ""
                if self._verbose >= LOG_NOISY:
                    print("[+]  Reading from {} at {}:{}{}{}".format(self._devs[devidx], offset, size, p, bad))
            blockv.append((self._devs[devidx], offset + 0x400000, size))
        data = self._bp.readv(blockv)
        ptr = 0
        for c in range(len(cols)):
            col = cols[c]
            size = col["rc_size"]
            piece = data[ptr:ptr+size]
            col_data.append(piece)
            ptr += size
            if debug_dump:
                devidx = col["rc_devidx"]
                offset = col["rc_offset"]
                f = open(path.join(self._dump_dir, "{}-{}.{}:{}.raw".format(debug_prefix, c, devidx, offset)), "wb")
                f.write(piece)
                f.close()
        if self._repair and len(self._bad) > 0:
            # Only support a single bad drive
            devs = [c["rc_devidx"] for c in cols]
            # Drop parity if stored on the bad disk
            if self._bad[0] in devs[1:]:
                parity = col_data[0]
                bad = devs.index(self._bad[0])
                bad_size = cols[bad]["rc_size"]
                if self._verbose >= LOG_NOISY:
                    print("[+]  Repairing {} bad bytes".format(bad_size))
                for b in range(1, len(col_data)):
                    if b != bad:
                        self._xor(parity, col_data[b])
                col_data[bad] = parity[:bad_size]
        data = bytearray()
        for c in col_data[firstdatacol:]:
            data += c
        return data

    @staticmethod
    def _xor(p, d):
        for i in range(len(d)):
            p[i] ^= d[i]

    def _map_alloc(self, io_offset, io_size, unit_shift):
        dcols = len(self._devs)
        # The starting RAIDZ (parent) vdev sector of the block.
        b = io_offset >> unit_shift
        # The zio's size in units of the vdev's minimum sector size.
        s = io_size >> unit_shift
        # The first column for this stripe.
        f = b % dcols
        # The starting byte offset on each child vdev.
        o = (b // dcols) << unit_shift

        # "Quotient": The number of data sectors for this stripe on all but
        # the "big column" child vdevs that also contain "remainder" data.
        q = s // (dcols - self._nparity)

        # "Remainder": The number of partial stripe data sectors in this I/O.
        # This will add a sector to some, but not all, child vdevs.
        r = s - q * (dcols - self._nparity)

        # The number of "big columns" - those which contain remainder data.
        bc = (r + self._nparity) if r else 0

        # The total number of data and parity sectors associated with
        # this I/O.
        tot = s + self._nparity * (q + (1 if r else 0))

        # acols: The columns that will be accessed.
        # scols: The columns that will be accessed or skipped.
        if q == 0:
            # Our I/O request doesn't span all child vdevs.
            acols = bc
            scols = min(dcols, roundup(bc, self._nparity + 1))
        else:
            acols = dcols
            scols = dcols

        rm_skipstart = bc
        rm_firstdatacol = self._nparity
        rm_cols = []

        for c in range(scols):
            col = f + c
            coff = o
            if col >= dcols:
                col -= dcols
                coff += (1 << unit_shift)
            rm_col = {"rc_devidx": col, "rc_offset": coff}
            if c >= acols:
                rm_col["rc_size"] = 0
            elif c < bc:
                rm_col["rc_size"] = (q + 1) << unit_shift
            else:
                rm_col["rc_size"] = q << unit_shift
            if rm_col["rc_size"] > 0:
                rm_cols.append(rm_col)

        if (rm_firstdatacol == 1) and (io_offset & (1 << 20)):
            devidx = rm_cols[0]["rc_devidx"]
            o = rm_cols[0]["rc_offset"]
            rm_cols[0]["rc_devidx"] = rm_cols[1]["rc_devidx"]
            rm_cols[0]["rc_offset"] = rm_cols[1]["rc_offset"]
            rm_cols[1]["rc_devidx"] = devidx
            rm_cols[1]["rc_offset"] = o
            if rm_skipstart == 0:
                rm_skipstart = 1

        return rm_cols, rm_firstdatacol, rm_skipstart
