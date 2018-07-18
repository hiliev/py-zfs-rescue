#!/usr/bin/env python3

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


"""
B{Block server}

Provides remote clients with access to the local disks.
"""

from socketserver import TCPServer, BaseRequestHandler
import struct, socket
import argparse

SERVER_ADDRESS = "localhost"
SERVER_PORT = 24892

trans_table = {}
verbose = 0

CHUNKSIZE=(4096*64)

class BlockTCPHandler(BaseRequestHandler):

    def read_len(self,l):
        buf = bytearray()
        while (len(buf) < l):
            buf += self.request.recv(1)
        return buf
    
    """
    Request handler for the block server
    """
    def handle(self):
        cmd = self.read_len(1)
        op = cmd[0]
        if op == ord('r'):
            self.handle_read_block(cmd)
        elif op == ord('v'):
            self.handle_read_blockv(cmd)
        else:
            print("[-] Block Server: invalid request %s" %(str(op)) )

    def handle_read_block(self, cmd):
        cmd = self.read_len(1+8+8+1)
        pad, offset, count, pathlen = struct.unpack('=BQQB', cmd)
        path = self.read_len(pathlen).decode('utf8')
        self._do_read(path, offset, count)

    def handle_read_blockv(self, cmd):
        cmd = self.read_len(1)
        nreqs, = struct.unpack('=B', cmd)
        for n in range(nreqs):
            cmd = self.read_len(8+8+1)
            (offset, count, pathlen) = struct.unpack('=QQB', cmd)
            path = self.read_len(pathlen).decode('utf8')
            self._do_read(path, offset, count)

    def _do_read(self, path, offset, count):
        if path in trans_table.keys():
            path = trans_table[path]
        if verbose:
            print("[+] Block server: {} -- {}/{}".format(path, offset, count))
        try:
            f = open(path, 'rb')

            f.seek(0,2)
            sz = f.tell();
            if verbose:
                print(("disk size: %d" % (sz)))
            
            i = 0;
            while i < count:
                l = min(count-i, CHUNKSIZE)
                f.seek(offset)
                data = f.read(l)
                self.request.sendall(struct.pack('=BQQ', ord('n'), offset, len(data))) # 'n' next packet
                self.request.sendall(data)
                i += l
                offset += l
            f.close()
        except Exception as e:
            if verbose:
                print(str(e))
            self.request.sendall(struct.pack('=BQQ', ord('e'), offset, 0 )) # 'e' error
        self.request.sendall(struct.pack('=BQQ', ord('l'),offset, count)) # 'l' last 
        if verbose:
            print("[+]  Read {} bytes".format(i))

def populate_trans_table(args):
    global trans_table
    try:
        f = open(args.config, 'r')
        entries = [l.strip() for l in f.readlines() if l[0] != '#']
        f.close()
        for entry in entries:
            (path, sub) = entry.split('\t')
            trans_table[path] = sub
    except:
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='block server')
    parser.add_argument('--verbose', '-v', dest='verbose', action='count', default=0)
    parser.add_argument('--config', '-c', dest='config', type=str, default="disks.tab",
                        help='Configuration file in json format, default disks.tab')
    args = parser.parse_args()
    verbose = args.verbose
    
    populate_trans_table(args)
    TCPServer.allow_reuse_address = True
    server = TCPServer((SERVER_ADDRESS, SERVER_PORT), BlockTCPHandler)
    server.serve_forever()
