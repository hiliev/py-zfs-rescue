from zfs.blocktree import BlockTree


class FileObj:

    def __init__(self, vdev, dnode, bad_as_zeros=False):
        self._vdev = vdev
        self._bt = BlockTree(dnode.levels, self._vdev, dnode.blkptrs[0])
        self._next_blkid = 0
        self._max_blkid = dnode.maxblkid
        self._datablksize = dnode.datablksize
        self._size = dnode.bonus.zp_size
        self._filepos = 0
        self._blkpos = 0
        self._buf = bytearray()
        self._corrupted = False
        self._bad_as_zeros = bad_as_zeros

    def read(self, n):
        # Consume the rest of the block
        nn = min(n, len(self._buf) - self._blkpos)
        data = self._buf[self._blkpos:self._blkpos+nn]
        self._blkpos += nn
        l = len(data)
        # Keep reading new blocks until desired size is reached
        while l < n:
            bad_block = False
            if self._next_blkid > self._max_blkid:
                print("[-]  Reading past last file block")
                bad_block = True
            if not bad_block:
                bptr = self._bt[self._next_blkid]
                self._next_blkid += 1
                if bptr is None:
                    print("[-]  Broken block tree")
                    bad_block = True
                else:
                    self._buf = self._vdev.read_block(bptr, dva=0)
                    if self._buf is None:
                        print("[-]  Unreadable block")
                        bad_block = True
            if bad_block:
                self._corrupted = True
                if self._bad_as_zeros:
                    self._buf = b'\x00' * self._datablksize
                else:
                    break
            if self._next_blkid % 16 == 0:
                print("[+]  Block {}/{}".format(self._next_blkid, self._max_blkid+1))
            self._blkpos = 0
            nn = min(n - len(data), len(self._buf))
            data += self._buf[:nn]
            self._blkpos += nn
            l += nn
        self._filepos += l
        return data

    def tell(self):
        return self._filepos

    @property
    def corrupted(self):
        return self._corrupted
