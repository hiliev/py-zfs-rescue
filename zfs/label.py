from zfs.nvpair import NVPairParser
from zfs.uberblock import UBArray

K = 1 << 10


class Label:

    BLANK_OFFSET = 0
    BLANK_SIZE = 8*K

    BOOT_OFFSET = 8*K
    BOOT_SIZE = 8*K

    NVLIST_OFFSET = 16*K
    NVLIST_SIZE = (128-16)*K

    UBARRAY_OFFSET = 128 * K
    UBARRAY_SIZE = 128 * K

    def __init__(self, block_proxy, vdev):
        self._bp = block_proxy
        self._data = None
        self._which = None
        self._nvlist = None
        self._ubarray = None
        self._vdev = vdev

    def read(self, which=0):
        if which < 2:
            self._data = self._bp.read(self._vdev, which * 256*K, 256*K)
        else:
            self._data = self._bp.read(self._vdev, 0, 256*K)
        nvparser = NVPairParser()
        self._nvlist = nvparser.parse(self._data[self.NVLIST_OFFSET:self.NVLIST_OFFSET+self.NVLIST_SIZE][4:])
        self._ubarray = UBArray()
        self._ubarray.parse(self._data[self.UBARRAY_OFFSET:self.UBARRAY_OFFSET + self.UBARRAY_SIZE])
        self._which = which

    def debug(self, show_uberblocks=False):
        print("Label", self._which)
        print("=" * 20)
        self._print_nvlist(self._nvlist)
        if show_uberblocks:
            print("Uberblock array")
            print("-" * 20)
            self._print_ubarray()

    def find_active_ub(self):
        highest_txg = self._nvlist['txg']
        active_ub_id = None
        for i in range(len(self._ubarray)):
            ub = self._ubarray[i]
            if ub.valid and ub.txg >= highest_txg:
                active_ub_id = i
                highest_txg = ub.txg
        if active_ub_id is not None:
            return self._ubarray[active_ub_id]
        return None

    def find_ub_txg(self, txg):
        return self._ubarray.find_block_by_txg(txg)

    def find_uncompressed_ub(self):
        for i in range(len(self._ubarray)):
            ub = self._ubarray[i]
            if ub.valid and ub.rootbp.comp_alg == 2:
                return ub
        return None

    def get_vdev_disks(self):
        return list(map(lambda x: x['path'], self._nvlist['vdev_tree']['children']))

    def get_txg(self):
        return self._nvlist['txg']

    def _print_nvlist(self, nvlist, indent=0):
        for k in nvlist.keys():
            v = nvlist[k]
            if isinstance(v, dict):
                print("{}{}:".format(" " * indent, k))
                self._print_nvlist(v, indent + 4)
            elif isinstance(v, list):
                for i in range(len(v)):
                    if isinstance(v[i], dict):
                        print("{}{}[{}]:".format(" " * indent, k, i))
                        self._print_nvlist(v[i], indent + 4)
                    else:
                        print("{}{}[{}]: {}".format(" " * indent, k, i, v[i]))
            else:
                print("{}{}: {}".format(" "*indent, k, v))

    def _print_ubarray(self, how_many=16):
        for i in range(min(how_many, len(self._ubarray))):
            uberblock = self._ubarray[i]
            uberblock.debug()
