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
import socket, json, os

SECTOR_SIZE = 512
VERBOSE=0
VERBOSEERR=1

class BlockProxy:

    def __init__(self, host_port):
        self.host = host_port[0]
        self.port = host_port[1]
        self._use_files = False
        self._use_1tb = False
        if self.host == 'files:':
            self._init_files()

    def _init_files(self):
        self._device_files = {}
        self._trans_table = {}
        self._use_files = True
        try:
            with open(self.port, 'r') as f:
                self._trans_table = json.load(f)
        except Exception as e:
            print("Cannot open %s (%s)" %(self.port,str(e)))
        for k,v in self._trans_table.items():
            if isinstance(v,list):
                self._use_1tb = True
                for i in v[:-1]:
                    b = os.path.getsize(i)
                    if not b == 1024*1024*1024*1024:
                        raise Exception("Expecting parts to be 1TB")
                b = os.path.getsize(v[-1])
                if not b <= 1024*1024*1024*1024:
                    raise Exception("Expecting parts to be <= 1TB")

    def read(self, dev_path, offset, count):
        if self._use_files:
            return self._read_files(dev_path, offset, count)
        else:
            return self._read_network(dev_path, offset, count)

    def readv(self, blockv):
        if self._use_files:
            return self._readv_files(blockv)
        else:
            return self._readv_network(blockv)

    def _read_network(self, dev_path, offset, count):
        buf = bytearray(count)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.host, self.port))
            request = struct.pack('=BB', ord('r'), 1)
            sock.sendall(request)
            self._read_network_buf(sock, buf, 0, dev_path, offset, count)
        finally:
            sock.close()
        return buf
        
    def _readv_network(self, blockv):
        
        count = 0
        for block in blockv:
            count += block[2]
        buf = bytearray(count)
        VERBOSE
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.host, self.port))
            request = struct.pack('=BB', ord('v'), len(blockv))
            sock.sendall(request)
            boff = 0;
            for block in blockv:
                self._read_network_buf(sock, buf, boff, block[0], block[1], block[2])
                boff += block[2]
        finally:
            sock.close()
        return buf

    def _read_network_buf(self, sock, buf, boff, dev_path, offset, count):

        enc_name = dev_path.encode('utf8')
        request = struct.pack('=QQB', offset, count, len(enc_name)) + enc_name;
        sock.sendall(request)
        
        view = memoryview(buf)
        while True:
            answer = bytearray()
            while len(answer) < 17:
                answer += sock.recv(1)
            a, off, l = struct.unpack('=BQQ', answer)
            if a == ord('n'):
                if VERBOSE:
                    print("[>] 'n' received");
                if (count <= 0):
                    break;
                count -= l;
                while l > 0:
                    nbytes = sock.recv_into(view[boff+(off-offset):], l)
                    off += nbytes
                    l -= nbytes
            elif a == ord('e'):
                if VERBOSEERR:
                    print("[>] 'e' received");
                break;
            elif a == ord('l'):
                if VERBOSE:
                    print("[>] 'l' received");
                break;
            else:
                print("Unknown code '%d'" %(a))
                break
            
        # print("[+] BlockProxy: received {} bytes".format(len(buf)))
        
    def _read_files(self, dev_path, offset, count):
        idx = offset // (1024*1024*1024*1024)
        if self._use_1tb:
            dev_path_r = dev_path + (".%d" %(idx))                    
            offset = offset % (1024*1024*1024*1024)
        else:
            dev_path_r = dev_path
        if dev_path_r not in self._device_files:
            trans_path = dev_path
            if trans_path in self._trans_table:
                trans_path = self._trans_table[trans_path]
                if self._use_1tb:
                    trans_path = trans_path[idx]
            self._device_files[dev_path_r] = open(trans_path, 'rb')
        f = self._device_files[dev_path_r]
        f.seek(offset)
        data = f.read(count)
        return data

    def _readv_files(self, blockv):
        data = bytearray()
        for b in blockv:
            data += self._read_files(b[0], b[1], b[2])
        return data

    def read_sectors(self, dev_path, sector, nsect):
        offset = sector * SECTOR_SIZE
        count = nsect * SECTOR_SIZE
        return self.read(dev_path, offset, count)
