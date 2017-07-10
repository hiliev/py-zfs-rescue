from zfs.dnode import DNode
from zfs.blocktree import BlockTree


class ObjectSet:

    def __init__(self, vdev, os_bptr, dva=0):
        self._vdev = vdev
        # Load the object set dnode
        self._dnode = self._load_os_dnode(os_bptr, dva)
        if self._dnode is None:
            print("[-] Object set dnode is unreachable")
            self._broken = True
            return
        # Compute intermediate properties
        self._indblksize = 1 << self._dnode.indblkshift
        self._datablksize = self._dnode.datablksize
        self._dnodes_per_block = self._datablksize // 512
        self._maxdnodeid = (self._dnode.maxblkid+1)*self._dnodes_per_block - 1
        print("[+] Object set information:")
        print("[+]  dnode", self._dnode)
        print("[+]  block size: indirect {} / data {}".format(self._indblksize, self._datablksize))
        print("[+]  {} blocks / {} dnodes per block".format(self._dnode.maxblkid+1, self._dnodes_per_block))
        self._blocktree = BlockTree(self._dnode.levels, self._vdev, self._dnode.blkptrs[0])
        if self._blocktree is None:
            print("[-]  Object set block tree is broken")
            self._broken = True
            return
        # print("[+] Block pointer 0 is", self._blocktree[0])
        # print("[+] Block pointer {} is {}".format(self._dnode.maxblkid, self._blocktree[self._dnode.maxblkid]))
        self._block_cache = {}
        self._broken = False

    def prefetch(self):
        print("[+] Prefetching the object set")
        for blkid in range(self._dnode.maxblkid+1):
            dn = self.__getitem__(blkid * self._dnodes_per_block)
            if dn is None:
                print("[-]  Corrupt object set block", blkid)
            else:
                print("[+]  dnode[{}]={}".format(blkid * self._dnodes_per_block, dn))

    @property
    def broken(self):
        return self._broken

    @property
    def dnodes_per_block(self):
        return self._dnodes_per_block

    def _load_os_dnode(self, os_bptr, dva):
        print("[+] Loading object set dnode from", os_bptr)
        return DNode.from_bptr(self._vdev, os_bptr, dvas=(dva,))

    def __getitem__(self, item):
        return self._get_dnode(item)
    
    def _get_dnode(self, dnode_id):
        if self._broken:
            print("[-] Accessing a broken object set!")
            return None
        blockid = dnode_id // self._dnodes_per_block
        if blockid in self._block_cache:
            block_data = self._block_cache[blockid]
        else:
            block_data = None
            bp = self._blocktree[blockid]
            if bp is not None:
                for dva in range(3):
                    block_data = self._vdev.read_block(bp, dva=dva)
                    if block_data:
                        break
            self._block_cache[blockid] = block_data
        if block_data is None:
            return None
        dnid = dnode_id % self._dnodes_per_block
        dnode = DNode(data=block_data[dnid*512:(dnid+1)*512])
        return dnode

    def __len__(self):
        return self._maxdnodeid+1