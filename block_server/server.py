#!/usr/bin/env python3

# Block server
# provides remote clients with access to the local disks

from socketserver import TCPServer, BaseRequestHandler
import struct

SERVER_ADDRESS = "localhost"
SERVER_PORT = 24892

trans_table = {}
verbose = False


class BlockTCPHandler(BaseRequestHandler):
    """
    Request handler for the block server
    """
    def handle(self):
        cmd = self.request.recv(1024)
        if len(cmd) < 18:
            print("[-] Block Server: invalid request")
            return
        op = cmd[0]
        if op == 1:
            self.handle_read_block(cmd)
        elif op == 2:
            self.handle_read_blockv(cmd)

    def handle_read_block(self, cmd):
        (op, offset, count) = struct.unpack('=BQQ', cmd[:17])
        path = cmd[17:].decode('utf8')
        data = self._do_read(path, offset, count)
        self.request.sendall(data)

    def handle_read_blockv(self, cmd):
        (op, nreqs) = struct.unpack('=BB', cmd[:2])
        ptr = 2
        data = bytearray()
        for n in range(nreqs):
            (offset, count, pathlen) = struct.unpack('=QQB', cmd[ptr:ptr+17])
            ptr += 17
            path = cmd[ptr:ptr+pathlen].decode('utf8')
            ptr += pathlen
            data += self._do_read(path, offset, count)
        self.request.sendall(data)

    @staticmethod
    def _do_read(path, offset, count):
        if path in trans_table.keys():
            path = trans_table[path]
        if verbose:
            print("[+] Block server: {} -- {}/{}".format(path, offset, count))
        f = open(path, 'rb')
        f.seek(offset)
        data = f.read(count)
        f.close()
        if verbose:
            print("[+]  Read {} bytes".format(len(data)))
        return data


def populate_trans_table():
    global trans_table
    try:
        f = open('disks.tab', 'r')
        entries = [l.strip() for l in f.readlines() if l[0] != '#']
        f.close()
        for entry in entries:
            (path, sub) = entry.split('\t')
            trans_table[path] = sub
    except:
        pass


if __name__ == "__main__":
    populate_trans_table()
    server = TCPServer((SERVER_ADDRESS, SERVER_PORT), BlockTCPHandler)
    server.serve_forever()
