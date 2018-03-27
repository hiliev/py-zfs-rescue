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


from zfs.blockptr import BlockPtr

import struct
from io import BytesIO

UBLOCK_CKSUM_SIZE = 32
UBLOCK_MAGIC = 0x00bab10c


class Uberblock:

    def __init__(self, data=None):
        self._magic = None
        self._version = None
        self._txg = None
        self._guid_sum = None
        self._timestamp = None
        self._rootbp = None
        self._valid = False
        self._data = None
        if data is not None:
            self.parse(data)

    def parse(self, data):
        self._data = bytearray(data)
        (self._magic, self._version, self._txg, self._guid_sum, self._timestamp) = struct.unpack("=QQQQQ", data[:5*8])
        self._rootbp = BlockPtr(data=data[40:40+128])
        self._valid = (self._magic == UBLOCK_MAGIC)

    @property
    def magic(self):
        return self._magic

    @property
    def version(self):
        return self._version

    @property
    def txg(self):
        return self._txg

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def valid(self):
        return self._valid

    @property
    def rootbp(self):
        return self._rootbp

    def debug(self):
        print("Uberblock: valid={} version={} txg={} timestamp={} blkptr={}".format(
            self._valid, self._version, self._txg, self._timestamp, self._rootbp
        ))


class UBArray:

    def __init__(self):
        self._blocks = []

    def parse(self, data, ashift=9):
        stream = BytesIO(data)
        ushift = max(ashift, 10) # min 1024
        num_blocks = len(data) >> ushift
        self._blocks = []
        for i in range(num_blocks):
            block_data = stream.read(1 << ushift)
            ublock = Uberblock()
            ublock.parse(block_data)
            self._blocks.append(ublock)

    def __len__(self):
        return len(self._blocks)

    def __getitem__(self, item):
        return self._blocks[item]

    def find_block_by_txg(self, txg):
        for b in self._blocks:
            if b.valid and b.txg == txg:
                return b
        return None
