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

Thanks to the work of @eiselekd, additional support was added for:

* LZ4 compression
* Fletcher checksum validation
* modern ZFS attributes

Additional recovery fixes now cover:

* correct embedded block-pointer physical and logical sizes
* the ZFS LZ4 payload boundary and final literal-only sequence
* sparse-file holes as logical zero-filled blocks
* RAIDZ reads whose physical size is not aligned to `ashift`
* vdev offsets beyond the former hardcoded 8 TiB limit

## What it is not?
This is not a generic rescue tool or a filesystem debugger *per se*. The main
explorer still requires some source-level configuration. The standalone tools
described below provide command-line interfaces for specific forensic recovery
workflows. Their output is technical and requires an understanding of ZFS
internals and on-disk structures.

The ZFS implementation is minimal and incomplete. It is basically in a "works for me" state. Notably the following features are missing:

* support for really large directories (it could be implemented relatively easily)
* ~validation of the block checksums -- currently the tool relies on all metadata being compressed and the LZJB decompressor failing with garbled input data~
* ~LZ4 and GZIP decompression~
* support for pools created on big-endian systems

There is minimal to no error recovery and encountering an unsupported object will abort the program. This is intentional as it helps easily spot unimplemented features and deviations from the specification.

## How to use it?
Documentation is currently a WiP.

## Forensic recovery tools

The scripts in [`tools/`](tools/) generalize a read-only workflow for locating
and recovering a deleted dataset. They require Python 3.10 or newer. They do
not import the pool; all pool reads go through the existing files block
provider. Put every source block device in operating-system read-only mode
before starting, and verify device identities rather than relying on volatile
`/dev/sdX` names.

### Locate a historical dataset dnode

`scan_mos_dnode.py` searches log-spacemap FREE ranges from a selected TXG for a
strongly validated DSL dataset dnode. The range file is expected to contain
lines such as `TXG ... F range: START-END`, with hexadecimal byte offsets.

```bash
python3 tools/scan_mos_dnode.py \
  --disks /path/to/disks.json \
  --label /dev/disk/by-partuuid/LABEL-PARTUUID \
  --ranges /path/to/free-ranges.txt \
  --target-txg 123456 \
  --dataset-object 789 \
  --creation-txg 45678 \
  --directory-object 780 \
  --output-dir /safe/output/candidates
```

The default checks range starts, which is fast and often sufficient for a
recent deletion. Add `--scan-all-offsets` for the exhaustive,
`ashift`-aligned search.

### Inspect a candidate and export its file list

```bash
python3 tools/inspect_dataset.py \
  --disks /path/to/disks.json \
  --label /dev/disk/by-partuuid/LABEL-PARTUUID \
  --candidate /safe/output/candidates/mos-dnode-object-789-....bin \
  --dataset-object 789 \
  --creation-txg 45678 \
  --directory-object 780 \
  --depth 2 \
  --file-list /safe/output/dataset-file-list.tsv
```

The creation TXG and directory object are optional at this stage, but supplying
known values protects against opening an unrelated historical dnode that used
the same slot.

### Targeted checksum-guided recovery

`recover_file.py` accepts paths from the exported file list. For each block it
tries every DVA, reconstructing one failed RAIDZ1 data column, and then bounded
per-stripe-row recovery. It writes a file only when every block is verified by
the Fletcher4 checksum stored in its block pointer.

```bash
python3 tools/recover_file.py \
  --disks /path/to/disks.json \
  --label /dev/disk/by-partuuid/LABEL-PARTUUID \
  --file-list /safe/output/dataset-file-list.tsv \
  --candidate /safe/output/candidate.bin,789,45678,780 \
  --path path/inside/dataset/file.bin \
  --output-dir /safe/output/recovered
```

Repeat `--candidate` to try another saved dnode block from a snapshot of the
same dataset. Object IDs must still refer to the same files. `--manifest` reads
multiple paths, and the JSONL report records every attempted block method.
Non-Fletcher4 blocks are reported as unsupported rather than accepted without
verification.

### Resumable recovery to another host

Copy `tools/remote_receiver.py` and `tools/remote_protocol.py` to the target
host, then run the sender:

```bash
python3 tools/recover_dataset_remote.py \
  --disks /path/to/disks.json \
  --label /dev/disk/by-partuuid/LABEL-PARTUUID \
  --candidate /safe/output/candidate.bin,789,45678,780 \
  --remote recovery-user@backup-host \
  --identity /path/to/id_ed25519 \
  --receiver /opt/zfs-recovery/remote_receiver.py \
  --destination /recovery/dataset \
  --state-dir /recovery/state
```

The sender uses one persistent SSH connection and retries interrupted files.
The receiver stages each file on the destination filesystem, independently
calculates SHA-256, calls `fsync()`, atomically installs the file, and commits a
SQLite ledger with `synchronous=FULL`. A disconnect can leave a staging file,
but cannot promote an incomplete file to the recovered tree. The destination
and state directory must be on the same filesystem.

If a source block cannot be verified, the sender zero-fills only that block and
the receiver records the file as `corrupt` with a `._corrupted` suffix. After
improving the source reader, stop the receiver and use the default dry-run of:

```bash
python3 tools/retry_corrupt_remote_files.py \
  --root /recovery/dataset \
  --state-dir /recovery/state
```

Review the output, repeat with `--apply`, and restart the sender. The helper
backs up the SQLite database and moves old corrupt outputs into a timestamped
quarantine instead of deleting them.

## Tests

The self-contained regression and protocol tests do not require a ZFS pool:

```bash
python3 -m unittest discover -s test -p 'test_*.py' -v
```
