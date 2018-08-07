# Copyright (c) 2017 Hristo Iliev <github@hiliev.eu>
# Copyright (c) 2018 Konrad Eisele <eiselekd@gmail.com>
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
from zfs.zap import zap_factory

class SystemAttr:
    
    def __init__(self, vdev, dataset, idx):
        self._sa = None
        self._sa_attrs = idx
        self._sa_attrs_dnode = dataset[idx]
        sa_attrs_zap = zap_factory(vdev, self._sa_attrs_dnode)
        self._sa_layout_id = sa_attrs_zap['LAYOUTS']
        self._sa_registry_id = sa_attrs_zap['REGISTRY']
        registry = dataset[self._sa_registry_id]
        layout = dataset[self._sa_layout_id]
        print("[+] SA registry: %s" %(str(registry)))
        print("[+] SA layout  : %s" %(str(layout)))
        self._r_zap = zap_factory(vdev, registry)
        self._l_zap = zap_factory(vdev, layout)
        self._reg = {}
        for k in self._r_zap.keys():
            v = self._r_zap[k]
            # 64      56      48      40      32      24      16      8       0                                                                                                    
            # +-------+-------+-------+-------+-------+-------+-------+-------+                                                                                                    
            # |        unused         |      len      | bswap |   attr num    |                                                                                                    
            # +-------+-------+-------+-------+-------+-------+-------+-------+                                                                                                    
            n = v & 0xffff
            l = v >> 24 & 0xffff
            self._reg[n] = {'len': l, 'name': k.lower()}
        self._lay = {}
        for k in self._l_zap.keys():
            b = self._l_zap[k]
            self._lay[k] = [] 
            for i in range(len(b)//2):
                idx, = struct.unpack(">H",b[i*2:(i+1)*2])
                self._lay[k].append(self._reg[idx])
            
    def parse(self,zap):
        pass
    
