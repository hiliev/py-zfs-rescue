#!/usr/bin/env python

# Block server
# provides remote clients with access to the local disks

from SocketServer import TCPServer, BaseRequestHandler
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
            print "[-] Block Server: invalid request"
            return
        (op, offset, count) = struct.unpack('=BQQ', cmd[:17])
        path = cmd[17:].decode('utf8')
        if path in trans_table.keys():
            path = trans_table[path]
        if verbose:
            print "[+] Block server: %s -- %d:%d" % (path, offset, count)
        f = open(path, 'rb')
        f.seek(offset)
        data = f.read(count)
        f.close()
        if verbose:
            print "[+]  read %d bytes" % (len(data),)
        self.request.sendall(data)


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
