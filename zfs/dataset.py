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


from zfs.objectset import ObjectSet
from zfs.zap import zap_factory, TYPECODES, safe_decode_string
from zfs.blocktree import BlockTree
from zfs.sa import SystemAttr
from zfs.fileobj import FileObj

import csv
import time
import tarfile
import os

MODE_UR = 0o400
MODE_UW = 0o200
MODE_UX = 0o100
MODE_GR = 0o040
MODE_GW = 0o020
MODE_GX = 0o010
MODE_OR = 0o004
MODE_OW = 0o002
MODE_OX = 0o001


class Dataset(ObjectSet):

    def __init__(self, vdev, os_dnode, dva=0):
        super().__init__(vdev, os_dnode.bonus.bptr, dva=dva)
        self._rootdir_id = None

    def analyse(self):
        if self.broken:
            print("[-]  Dataset is broken")
            return
        # Read the master node
        master_dnode = self[1]
        if master_dnode is None:
            print("[-]  Master node missing/unreachable")
            return
        print("[+]  Master node", master_dnode)
        if master_dnode.type != 21:
            print("[-]  Master node object is of wrong type")
            return
        z = zap_factory(self._vdev, master_dnode)
        if z:
            self._rootdir_id = z["ROOT"]
            if self._rootdir_id is None:
                z.debug()
            
            # try load System Attribute Layout and registry:
            try:
                self._sa = SystemAttr(self._vdev, self, z["SA_ATTRS"]);
            except Exception as e:
                print("[-] Unable to parse System Attribute tables: %s" %(str(e)))   

        if self._rootdir_id is None:
            print("[-]  Root directory ID is not in master node")
            return
        rootdir_dnode = self[self._rootdir_id]
        if rootdir_dnode is None:
            print("[-]  Root directory dnode missing/unreachable")
            return
        if rootdir_dnode.type != 20:
            print("[-]  Root directory object is of wrong type")
        num_dnodes = min(self.dnodes_per_block, self.max_obj_id+1)
        print("[+]  First block of the object set:")
        for n in range(num_dnodes):
            d = self[n]
            if d is None:
                # Bad - very likely the block tree is broken
                print("[-]  Object set (partially) unreachable")
                break
            print("[+]  dnode[{:>2}]={}".format(n, d))

    def prefetch_object_set(self):
        self.prefetch()

    def traverse_dir(self, dir_dnode_id, depth=1, dir_prefix='/'):
        dir_dnode = self[dir_dnode_id]
        if dir_dnode is None:
            print("[-]  Directory dnode {} unreachable".format(dir_dnode_id))
            return
        zap = zap_factory(self._vdev, dir_dnode)
        if zap is None:
            print("[-]  Unable to create ZAP object")
            return
        keys = sorted(zap.keys())
        for name in keys:
            value = zap[name]
            t = value >> 60
            v = value & ~(15 << 60)
            k = TYPECODES[t]
            entry_dnode = self[v]
            if entry_dnode is None:
                mode = "?????????"
                size = "?"
            else:
                mode = entry_dnode.bonus.zp_mode
                size = entry_dnode.bonus.zp_size
                modes = [
                    'r' if (mode & MODE_UR) else '-',
                    'w' if (mode & MODE_UW) else '-',
                    'x' if (mode & MODE_UX) else '-',
                    'r' if (mode & MODE_GR) else '-',
                    'w' if (mode & MODE_GW) else '-',
                    'x' if (mode & MODE_GX) else '-',
                    'r' if (mode & MODE_OR) else '-',
                    'w' if (mode & MODE_OW) else '-',
                    'x' if (mode & MODE_OX) else '-'
                ]
                mode = "".join(modes)
            print("{}{} {:>8} {:>14} {}{}".format(k, mode, v, size, dir_prefix, name))
            if k == 'd' and depth > 0:
                self.traverse_dir(v, depth=depth-1, dir_prefix=dir_prefix+name+'/')

    def export_file_list(self, fname, root_dir_id=None):
        print("[+]  Exporting file list")
        if root_dir_id is None:
            root_dir_id = self._rootdir_id
        with open(fname, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, dialect="excel-tab")
            self._export_dir(csvwriter, root_dir_id)

    def extract_file(self, file_node_id, target_path):
        print("[+]  Extracting object {} to {}".format(file_node_id, target_path))
        file_dnode = self[file_node_id]
        bt = BlockTree(file_dnode.levels, self._vdev, file_dnode.blkptrs[0])
        num_blocks = file_dnode.maxblkid + 1
        f = open(target_path, "wb")
        total_len = 0
        corrupted = False
        tt = -time.time()
        if file_dnode.bonus.zp_size > 0:
            for n in range(num_blocks):
                bp = bt[n]
                bad_block = False
                if bp is None:
                    print("[-]  Broken block tree")
                    bad_block = True
                else:
                    block_data = self._vdev.read_block(bp, dva=0)
                    if block_data is None:
                        print("[-]  Unreadable block")
                        bad_block = True
                if bad_block:
                    block_data = b'\x00' * file_dnode.datablksize
                    corrupted = True
                f.write(block_data)
                total_len += len(block_data)
                if n % 16 == 0:
                    print("[+]  Block {:>3}/{} total {:>7} bytes".format(n, num_blocks, total_len))
        tt += time.time()
        if tt == 0.0:
            tt = 1.0  # Prevent division by zero for 0-length files
        data_size = min(total_len, file_dnode.bonus.zp_size)
        f.truncate(data_size)
        f.close()
        print("[+]  {} bytes in {:.3f} s ({:.1f} KiB/s)".format(total_len, tt, total_len / (1024 * tt)))
        return not corrupted

    def archive(self, archive_path, dir_node_id=None, skip_objs=None, temp_dir='/tmp'):
        if dir_node_id is None:
            dir_node_id = self._rootdir_id
        if skip_objs is None:
            skip_objs = []
        with tarfile.open(archive_path, 'w:') as tar:
            self._archive(tar, dir_node_id, temp_dir, skip_objs)

    def _archive(self, tar, dir_node_id, temp_dir, skip_objs, dir_prefix=''):
        print("[+]  Archiving directory object {}".format(dir_node_id))
        dir_dnode = self[dir_node_id]
        if dir_dnode is None:
            print("[-]  Archiving failed")
            return
        zap = zap_factory(self._vdev, dir_dnode)
        if zap is None:
            print("[-]  Archiving failed")
            return
        tmp_name = os.path.join(temp_dir, "extract.tmp")
        keys = sorted(zap.keys())
        for name in keys:
            value = zap[name]
            t = value >> 60
            v = value & ~(15 << 60)
            k = TYPECODES[t]
            if v in skip_objs:
                print("[+]  Skipping {} ({}) per request".format(name, v))
                continue
            if k in ['d', 'f', 'l']:
                entry_dnode = self[v]
                if entry_dnode is None:
                    print("[-]  Skipping unreadable object")
                    continue
                file_info = entry_dnode.bonus
                full_name = dir_prefix + name
                print("[+]  Archiving {} ({} bytes)".format(name, file_info.zp_size))
                if k == 'f':
                    success = self.extract_file(v, tmp_name)
                    if not success:
                        full_name += "._corrupted"
                    tar_info = tar.gettarinfo(name=tmp_name, arcname=full_name)
                    tar_info.uname = ""
                    tar_info.gname = ""
                elif k == 'd':
                    tar_info = tarfile.TarInfo()
                    tar_info.type = tarfile.DIRTYPE
                    tar_info.size = 0
                    tar_info.name = full_name
                else:
                    tar_info = tarfile.TarInfo()
                    tar_info.type = tarfile.SYMTYPE
                    tar_info.size = 0
                    tar_info.name = full_name
                    if file_info.zp_size > len(file_info.zp_inline_content):
                        # Link target is in the file content
                        linkf = FileObj(self._vdev, entry_dnode)
                        link_target = linkf.read(file_info.zp_size)
                        if link_target is None or len(link_target) < file_info.zp_size:
                            print("[-]  Insufficient content for symlink target")
                            # entry_dnode.dump_data('{}/dnode_{}.raw'.format(temp_dir, v))
                            # raise Exception("Insufficient link target content")
                            continue
                        tar_info.linkname = safe_decode_string(link_target)
                    else:
                        # Link target is inline in the bonus data
                        tar_info.linkname = safe_decode_string(file_info.zp_inline_content[:file_info.zp_size])
                tar_info.mtime = file_info.zp_mtime
                tar_info.mode = file_info.zp_mode  # & 0x1ff
                tar_info.uid = file_info.zp_uid
                tar_info.gid = file_info.zp_gid
                # print("[+]  Archiving {} bytes from {}".format(tar_info.size, tar_info.name))
                # f = FileObj(self._vdev, entry_dnode) if k == 'f' else None
                try:
                    if k == 'f':
                        if os.path.isfile(tmp_name):
                            with open(tmp_name, 'rb') as f:
                                tar.addfile(tar_info, f)
                            os.unlink(tmp_name)
                    else:
                        tar.addfile(tar_info)
                except:
                    print("[-]  Archiving {} failed".format(tar_info.name))
                if k == 'd':
                    self._archive(tar, v, temp_dir, skip_objs, dir_prefix=full_name+'/')

    def _export_dir(self, csv_obj, dir_node_id, dir_prefix='/'):
        print("[+]  Exporting directory object {}".format(dir_node_id))
        dir_dnode = self[dir_node_id]
        if dir_dnode is None:
            csv_obj.writerow([dir_node_id, -1, dir_prefix])
            return
        zap = zap_factory(self._vdev, dir_dnode)
        if zap is None:
            return
        keys = sorted(zap.keys())
        for name in keys:
            value = zap[name]
            t = value >> 60
            v = value & ~(15 << 60)
            k = TYPECODES[t]
            entry_dnode = self[v]
            size = entry_dnode.bonus.zp_size if entry_dnode is not None else -1
            full_name = dir_prefix + name
            print("{} {}".format(v, full_name))
            if k == 'f':
                csv_obj.writerow([v, size, full_name])
            if k == 'l':
                csv_obj.writerow([v, size, full_name + " -> ..."])
            if k == 'd':
                csv_obj.writerow([v, 0, full_name + '/'])
                self._export_dir(csv_obj, v, dir_prefix=full_name + '/')

    @property
    def max_obj_id(self):
        return self._maxdnodeid
