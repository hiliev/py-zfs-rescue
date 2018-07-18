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


from zfs.blocktree import BlockTree


class FileObj:

    def __init__(self, vdev, dnode, bad_as_zeros=False):
        self._vdev = vdev
        self._bt = BlockTree(dnode.levels, self._vdev, dnode.blkptrs[0])
        self._next_blkid = 0
        self._max_blkid = dnode.maxblkid
        self._datablksize = dnode.datablksize
        self._size = dnode.bonus.zp_size
        self._filepos = 0
        self._blkpos = 0
        self._buf = bytearray()
        self._corrupted = False
        self._bad_as_zeros = bad_as_zeros

    def read(self, n):
        # Consume the rest of the block
        nn = min(n, len(self._buf) - self._blkpos)
        data = self._buf[self._blkpos:self._blkpos+nn]
        self._blkpos += nn
        l = len(data)
        # Keep reading new blocks until desired size is reached
        while l < n:
            bad_block = False
            if self._next_blkid > self._max_blkid:
                print("[-]  Reading past last file block")
                bad_block = True
            if not bad_block:
                bptr = self._bt[self._next_blkid]
                self._next_blkid += 1
                if bptr is None:
                    print("[-]  Broken block tree")
                    bad_block = True
                else:
                    self._buf,c = self._vdev.read_block(bptr, dva=0)
                    if (not c) or self._buf is None:
                        print("[-]  Unreadable block")
                        bad_block = True
            if bad_block:
                self._corrupted = True
                if self._bad_as_zeros:
                    self._buf = b'\x00' * self._datablksize
                else:
                    break
            if self._next_blkid % 16 == 0:
                print("[+]  Block {}/{}".format(self._next_blkid, self._max_blkid+1))
            self._blkpos = 0
            nn = min(n - len(data), len(self._buf))
            data += self._buf[:nn]
            self._blkpos += nn
            l += nn
        self._filepos += l
        return data

    def tell(self):
        return self._filepos

    @property
    def corrupted(self):
        return self._corrupted
