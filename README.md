# py-zfs-rescue
A very minimal implementation in Python 3 of ZFS in user-space for pool recovery purposes.

## Background
This project evolved from a set of Python scripts for reading and displaying on-disk structures that the ZFS debugger `zdb` would not show. It is the culmination of the effort to salvage the data from a severly broken raidz1 array. More background information is available in [this blog post](https://hiliev.eu/blog/recovering-datasets-from-broken-zfs-raidz-pools.html).

## What it is?
`zfs_rescue` is a Python 3 script that is able to read the structure of a ZFS pool provided an initial device that belongs to the pool and to extract various types of information from the pool:

* list the accessible datasets with their sizes
* recursively list the files in all or some of the datasets found
* archive the content of all or some regular files in a given dataset

The code was developed specifically against a broken ZFS raidz1 pool created by an old Solaris 10 x86 system and thus handles:

* ZFS version 10 on little-endian systems
* pools that consist of a single mirror or raidz1 vdev (the mirror code should be able to handle single devices too)
* for raidz1 the parity information is used to recreate the data from the failed device, if any
* directories with small to moderately large number of elements
* access to remote disks via a simple TCP/IP protocol

## What it is not?
This is not a generic rescue tool or a filesystem debugger *per se*. It provides no command-line interface and all configuration is done by altering the source code. The output is quite technical and requires some understanding of the ZFS internals and on-disk structure.

The ZFS implementation is minimal and incomplete. It is basically in a "works for me" state. Notably the following features are missing:

* support for really large directories (it could be implemented relatively easily)
* validation of the block checksums -- currently the tool relies on all metadata being compressed and the LZJB decompressor failing with garbled input data
* LZ4 and GZIP decompression
* support for pools created on big-endian systmes

There is minimal to no error recovery and encountering an unsupported object will abort the program. This is intentional as it helps easily spot unimplemented features and deviations from the specification.

## How to use it?
See the wiki.
