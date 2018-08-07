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


import struct
from zfs.obj_desc import *

VERBOSE_EMBED=False

class DVA:

    def __init__(self, qword0, qword1):
        self._asize = (qword0 & 0xffffff) << 9
        self._grid = (qword0 >> 24) & 0xff
        self._vdev = (qword0 >> 32)
        self._offset = (qword1 & 0x7fffffffffffffff) << 9
        self._gang = (qword1 & (1 << 63)) != 0

    def __str__(self):
        gang = "G:" if self._gang else ""
        grid = "/grid={}".format(self._grid) if self._grid else ""
        return "<{}{}:{}:{}{}>".format(
            gang, self._vdev, hex(self._offset)[2:], hex(self._asize)[2:], grid
        )

    @property
    def null(self):
        return (self._vdev == 0) and (self._offset == 0) and (self._asize == 0)

    @property
    def gang(self):
        return self._gang == 1

    @property
    def offset(self):
        return self._offset


class BlockPtr:

    def __init__(self, data=None):
        self._dva0 = None
        self._dva1 = None
        self._dva2 = None
        self._lsize = None
        self._psize = None
        self._comp = None
        self._embeded = None
        self._cksum = None
        self._type = None
        self._lvl = None
        self._E = None
        self._birth_txg = None
        self._fill_count = None
        self._checksum = None
        self._encrypted = None
        if data is not None:
            self.parse(data)

    def parse(self, data):
        qwords = struct.unpack('=QQQQQQQQQQQQQQQQ', data)
        self._dva0 = DVA(qwords[0], qwords[1])
        self._dva1 = DVA(qwords[2], qwords[3])
        self._dva2 = DVA(qwords[4], qwords[5])
        self._lsize = (1 + (qwords[6] & 0xffff)) << 9
        self._psize = (1 + ((qwords[6] >> 16) & 0xffff)) << 9
        self._comp = (qwords[6] >> 32) & 0x7f
        self._embeded = (qwords[6] >> 39) & 0x1
        self._cksum = (qwords[6] >> 40) & 0xff
        self._type = (qwords[6] >> 48) & 0xff
        self._lvl = (qwords[6] >> 56) & 0x7f
        self._encrypted = (qwords[6] >> 61) & 0x1;
        self._E = qwords[6] >> 63
        self._birth_txg = qwords[10]
        self._fill_count = qwords[11]
        self._checksum = qwords[12:16]
        if self._embeded:
            self._dva0 = self._dva0 = DVA(0, 0)
            self._dva1 = self._dva0 = DVA(0, 0)
            self._dva2 = self._dva0 = DVA(0, 0)
            self._embeded_lsize = (qwords[6]) & 0x1ffffff
            self._embeded_data = data[0:(6*8)] + data[(7*8):(10*8)] + data[(11*8):(16*8)]
            self._etype = (qwords[6] >> 40) & 0xff
            self._psize = (qwords[6] >> 25) & 0x7f
            self._lsize = self._embeded_lsize
            if VERBOSE_EMBED:
                print("[E]: zdb -E %x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x | xxd" %(
                    qwords[0], qwords[1], qwords[2], qwords[3],
                    qwords[4], qwords[5], qwords[6], qwords[7],
                    qwords[8], qwords[9], qwords[10], qwords[11],
                    qwords[12], qwords[13], qwords[14], qwords[15]));


    def get_dva(self, dvanum):
        if dvanum == 0:
            return self._dva0
        elif dvanum == 1:
            return self._dva1
        elif dvanum == 2:
            return self._dva2
        else:
            return self._dva0

    @property
    def psize(self):
        return self._psize

    @property
    def lsize(self):
        return self._lsize

    @property
    def comp_alg(self):
        return self._comp

    @property
    def compressed(self):
        return self._comp != 2

    @property
    def empty(self):
        return self._dva0.null and self._dva1.null and self._dva2.null and not self._embeded

    def __str__(self):
        if self.empty:
            return "empty"
        gang = "gang" if self._dva0.gang else "contiguous"
        try:
            dmu_type = DMU_TYPE_DESC[self._type]
        except IndexError:
            dmu_type = "unk_{}".format(self._type)
        try:
            cksum = CHKSUM_DESC[self._cksum]
        except IndexError:
            cksum = "unk_{}".format(self._cksum)
        try:
            comp = COMP_DESC[self._comp]
        except IndexError:
            comp = "unk_{}".format(self._comp)
        if self._embeded:
            return "<[L{} {}] {}L/{}P embedded={} birth={} {} {} {} {} fill={}>".format(
            self._lvl, dmu_type, hex(self._lsize)[2:], hex(self._psize)[2:],
            self._embeded_lsize, self._birth_txg, cksum, comp, ENDIAN_DESC[self._E], gang,
            self._fill_count)
        return "<[L{} {}] {}L/{}P DVA[0]={} DVA[1]={} DVA[2]={} birth={} {} {} {} {} fill={}>".format(
            self._lvl, dmu_type, hex(self._lsize)[2:], hex(self._psize)[2:],
            self._dva0, self._dva1, self._dva2,
            self._birth_txg, cksum, comp, ENDIAN_DESC[self._E], gang,
            self._fill_count)


class BlockPtrArray:

    def __init__(self, data):
        self._bptrs = []
        for i in range(len(data) // 128):
            bp = BlockPtr(data=data[i*128:(i+1)*128])
            self._bptrs.append(bp)

    def __len__(self):
        return len(self._bptrs)

    def __getitem__(self, item):
        return self._bptrs[item]

def fletcher4(data):
    l = len(data)
    e = ( 4 - (l % 4) ) % 4;
    if (e > 0):
        data = data + ([0]*e);
        l += e
    a = 0
    b = 0
    c = 0
    d = 0
    for i in range(l//4):
        v, = struct.unpack("<I",data[i*4:(i+1)*4])
        a += v
        b += a
        c += b
        d += c
    return (a&0xffffffffffffffff,b&0xffffffffffffffff,c&0xffffffffffffffff,d&0xffffffffffffffff)
