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


from zfs.nvpair import NVPairParser
from zfs.uberblock import UBArray

K = 1 << 10


class Label:

    BLANK_OFFSET = 0
    BLANK_SIZE = 8*K

    BOOT_OFFSET = 8*K
    BOOT_SIZE = 8*K

    NVLIST_OFFSET = 16*K
    NVLIST_SIZE = (128-16)*K

    UBARRAY_OFFSET = 128 * K
    UBARRAY_SIZE = 128 * K

    def __init__(self, block_proxy, vdev):
        self._bp = block_proxy
        self._data = None
        self._which = None
        self._nvlist = None
        self._ubarray = None
        self._vdev = vdev

    def read(self, which=0):
        if which < 2:
            self._data = self._bp.read(self._vdev, which * 256*K, 256*K)
        else:
            self._data = self._bp.read(self._vdev, 0, 256*K)
        nvparser = NVPairParser()
        self._nvlist = nvparser.parse(self._data[self.NVLIST_OFFSET:self.NVLIST_OFFSET+self.NVLIST_SIZE][4:])
        self._ashift = self._nvlist['vdev_tree']['ashift'] if 'vdev_tree' in self._nvlist and 'ashift' in self._nvlist['vdev_tree'] else 9;
        self._ubarray = UBArray()
        self._ubarray.parse(self._data[self.UBARRAY_OFFSET:self.UBARRAY_OFFSET + self.UBARRAY_SIZE],ashift=self._ashift)
        self._which = which

    def debug(self, show_uberblocks=False):
        print("Label", self._which)
        print("=" * 20)
        self._print_nvlist(self._nvlist)
        if show_uberblocks:
            print("Uberblock array")
            print("-" * 20)
            self._print_ubarray()

    def find_active_ub(self):
        highest_txg = self._nvlist['txg']
        active_ub_id = None
        for i in range(len(self._ubarray)):
            ub = self._ubarray[i]
            if ub.valid and ub.txg >= highest_txg:
                active_ub_id = i
                highest_txg = ub.txg
        if active_ub_id is not None:
            return self._ubarray[active_ub_id]
        return None

    def find_ub_txg(self, txg):
        return self._ubarray.find_block_by_txg(txg)

    def find_uncompressed_ub(self):
        for i in range(len(self._ubarray)):
            ub = self._ubarray[i]
            if ub.valid and ub.rootbp.comp_alg == 2:
                return ub
        return None

    def get_vdev_disks(self):
        return list(map(lambda x: x['path'], self._nvlist['vdev_tree']['children']))

    def get_txg(self):
        return self._nvlist['txg']

    def _print_nvlist(self, nvlist, indent=0):
        for k in nvlist.keys():
            v = nvlist[k]
            if isinstance(v, dict):
                print("{}{}:".format(" " * indent, k))
                self._print_nvlist(v, indent + 4)
            elif isinstance(v, list):
                for i in range(len(v)):
                    if isinstance(v[i], dict):
                        print("{}{}[{}]:".format(" " * indent, k, i))
                        self._print_nvlist(v[i], indent + 4)
                    else:
                        print("{}{}[{}]: {}".format(" " * indent, k, i, v[i]))
            else:
                print("{}{}: {}".format(" "*indent, k, v))

    def _print_ubarray(self, how_many=16):
        for i in range(min(how_many, len(self._ubarray))):
            uberblock = self._ubarray[i]
            uberblock.debug()
