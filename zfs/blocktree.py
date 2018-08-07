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
from zfs.col import color

class BlockTree:
    VERBOSE_TRAVERSE=False

    def __init__(self, levels, vdev, root_bptr):
        self._levels = levels
        self._vdev = vdev
        print("[+] Creating block tree from", root_bptr)
        self._cache = {}
        if levels == 1:
            self._root = root_bptr
        else:
            self._root = self._load_from_bptr(root_bptr)
            self._blocks_per_level = len(self._root)
            print("[+]  {} blocks per level".format(self._blocks_per_level))

    def _load_from_bptr(self, bptr):
        block_data = None
        for dva in range(3):
            block_data,c = self._vdev.read_block(bptr, dva=dva)
            if block_data and c:
                break
        if block_data is None:
            return None
        return BlockPtrArray(block_data)

    def _get_level_indices(self, blockid):
        indices = []
        for i in range(self._levels-1):
            index = blockid % self._blocks_per_level
            blockid = blockid // self._blocks_per_level
            indices.append(index)
        indices.reverse()
        return indices

    def __getitem__(self, item):
        if item < 0:
            return None
        if self._levels == 1:
            return self._root if item == 0 else None
        indices = self._get_level_indices(item)
        bpa = self._root
        _cache = self._cache
        for (l, i) in enumerate(indices[:-1]):
            if not i in _cache:
                _cache[i] = { 'b' : None, 'n' : {} } # generate a graph
            if not _cache[i]['b'] is None:
                next_bpa = _cache[i]['b']
            else:
                b = bpa[i]
                bpa_data = None
                for dva in range(3):
                    bpa_data,c = self._vdev.read_block(b, dva=dva)
                    if bpa_data and c:
                        break
                next_bpa = None
                if bpa_data is not None: 
                    next_bpa = BlockPtrArray(bpa_data)
                else:
                    print("[-] Block tree is broken at", b)
                _cache[i]['b'] = next_bpa
            bpa = next_bpa
            if BlockTree.VERBOSE_TRAVERSE:
                   self.printidx(indices, l, str(bpa))
            if bpa is None:
                return None
            _cache = _cache[i]['n']
        b = bpa[indices[-1]]
        if BlockTree.VERBOSE_TRAVERSE:
               print(("[t-%d] " %(item))+color.GREEN+str(indices)+color.END+" : "+str(b)+" : "+str(bpa))
        return b
    
    def printidx(self, indices, i, postfix):
        b = [str(j) for j in ([(" %02d" %(j)) for j in indices[0:i]] + [">%02d" %(indices[i])] + [(" %02d" %(j)) for j in indices[i+1:]]) ]
        a = ",".join(b) + "  " + postfix
        print (a)
