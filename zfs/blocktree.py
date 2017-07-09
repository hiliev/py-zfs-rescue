from zfs.blockptr import BlockPtrArray


class BlockTree:

    def __init__(self, levels, vdev, root_bptr):
        self._levels = levels
        self._vdev = vdev
        print("[+] Creating block tree from", root_bptr)
        if levels == 1:
            self._root = root_bptr
        else:
            self._root = self._load_from_bptr(root_bptr)
            self._blocks_per_level = len(self._root)
            print("[+]  {} blocks per level".format(self._blocks_per_level))
            self._cache = {}

    def _load_from_bptr(self, bptr):
        block_data = None
        for dva in range(3):
            block_data = self._vdev.read_block(bptr, dva=dva)
            if block_data:
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
        for (l, i) in enumerate(indices[:-1]):
            level_cache = self._cache.get(l)
            if level_cache is None:
                level_cache = {}
                self._cache[l] = level_cache
            if i in level_cache:
                next_bpa = level_cache[i]
            else:
                b = bpa[i]
                bpa_data = None
                for dva in range(3):
                    bpa_data = self._vdev.read_block(b, dva=dva)
                    if bpa_data:
                        break
                next_bpa = None
                if bpa_data is not None:
                    next_bpa = BlockPtrArray(bpa_data)
                else:
                    print("[-] Block tree is broken at", b)
                level_cache[i] = next_bpa
            bpa = next_bpa
            if bpa is None:
                return None
        return bpa[indices[-1]]
