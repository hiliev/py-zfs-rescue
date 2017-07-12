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


DMU_TYPE_DESC = [
    "unallocated",              # 0
    "object directory",         # 1
    "object array",             # 2
    "packed nvlist",            # 3
    "packed nvlist size",       # 4
    "bpobj",                    # 5
    "bpobj header",             # 6
    "SPA space map header",     # 7
    "SPA space map",            # 8
    "ZIL intent log",           # 9
    "DMU dnode",                # 10
    "DMU objset",               # 11
    "DSL directory",            # 12
    "DSL directory child map",  # 13
    "DSL dataset snap map",     # 14
    "DSL props",                # 15
    "DSL dataset",              # 16
    "ZFS znode",                # 17
    "ZFS V0 ACL",               # 18
    "ZFS plain file",           # 19
    "ZFS directory",            # 20
    "ZFS master node",          # 21
    "ZFS delete queue",         # 22
    "zvol object",              # 23
    "zvol prop",                # 24
    "other uint8[]",            # 25
    "other uint64[]",           # 26
    "other ZAP",                # 27
    "persistent error log",     # 28
    "SPA history",              # 29
    "SPA history offsets",      # 30
    "Pool properties",          # 31
    "DSL permissions",          # 32
    "ZFS ACL",                  # 33
    "ZFS SYSACL",               # 34
    "FUID table",               # 35
    "FUID table size",          # 36
    "DSL dataset next clones",  # 37
    "scan work queue",          # 38
    "ZFS user/group used",      # 39
    "ZFS user/group quota",     # 40
    "snapshot refcount tags",   # 41
    "DDT ZAP algorithm",        # 42
    "DDT statistics",           # 43
    "System attributes",        # 44
    "SA master node",           # 45
    "SA attr registration",     # 46
    "SA attr layouts",          # 47
    "scan translations",        # 48
    "deduplicated block",       # 49
    "DSL deadlist map",         # 50
    "DSL deadlist map hdr",     # 51
    "DSL dir clones",           # 52
    "bpobj subobj"              # 53
]

COMP_DESC = [
    "invalid",
    "lzjb",
    "off",
    "lzjb",
    "empty",
    "gzip1",
    "gzip2",
    "gzip3",
    "gzip4",
    "gzip5",
    "gzip6",
    "gzip7",
    "gzip8",
    "gzip9",
    "zle",
    "lz4"
]

CHKSUM_DESC = ["invalid", "fletcher2", "none", "SHA-256", "SHA-256", "fletcher2", "fletcher2", "fletcher4", "SHA-256"]

ENDIAN_DESC = ["BE", "LE"]
