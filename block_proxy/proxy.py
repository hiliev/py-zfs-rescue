import struct
import socket

SECTOR_SIZE = 512


class BlockProxy:

    def __init__(self, host_port):
        self.host = host_port[0]
        self.port = host_port[1]
        self._use_files = False
        if self.host == 'files:':
            self._init_files()

    def _init_files(self):
        self._device_files = {}
        self._trans_table = {}
        self._use_files = True
        try:
            f = open(self.port, 'r')
            entries = [l.strip() for l in f.readlines() if l[0] != '#']
            f.close()
            for entry in entries:
                (path, sub) = entry.split('\t')
                self._trans_table[path] = sub
        except:
            pass

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
        request_header = struct.pack('=BQQ', 1, offset, count)
        request = request_header + dev_path.encode('utf8')

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            sock.connect((self.host, self.port))
            sock.sendall(request)

            buf = bytearray(count)
            view = memoryview(buf)
            while count:
                nbytes = sock.recv_into(view, count)
                view = view[nbytes:]
                count -= nbytes

            # print("[+] BlockProxy: received {} bytes".format(len(buf)))
            return buf
        finally:
            sock.close()

    def _read_files(self, dev_path, offset, count):
        if dev_path not in self._device_files:
            trans_path = dev_path
            if trans_path in self._trans_table:
                trans_path = self._trans_table[trans_path]
            self._device_files[dev_path] = open(trans_path, 'rb')
        f = self._device_files[dev_path]
        f.seek(offset)
        data = f.read(count)
        return data

    def _readv_network(self, blockv):
        request = struct.pack('=BB', 2, len(blockv))
        count = 0
        for block in blockv:
            enc_name = block[0].encode('utf8')
            subreq = struct.pack('=QQB', block[1], block[2], len(enc_name))
            request += subreq + enc_name
            count += block[2]

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            sock.connect((self.host, self.port))
            sock.sendall(request)

            buf = bytearray(count)
            view = memoryview(buf)
            while count:
                nbytes = sock.recv_into(view, count)
                view = view[nbytes:]
                count -= nbytes

            # print("[+] BlockProxy: received {} bytes".format(len(buf)))
            return buf
        finally:
            sock.close()

    def _readv_files(self, blockv):
        data = bytearray()
        for b in blockv:
            data += self._read_files(b[0], b[1], b[2])
        return data

    def read_sectors(self, dev_path, sector, nsect):
        offset = sector * SECTOR_SIZE
        count = nsect * SECTOR_SIZE
        return self.read(dev_path, offset, count)
