"""Shared helpers for the standalone recovery tools.

The tools are intentionally kept outside the core parser. They read pool
devices through the existing block provider and never import or modify a pool.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from block_proxy.proxy import BlockProxy  # noqa: E402
from zfs.dataset import Dataset  # noqa: E402
from zfs.dnode import BonusDataset, DNode  # noqa: E402
from zfs.label import Label  # noqa: E402
from zfs.zio import RaidzDevice  # noqa: E402

DNODE_SIZE = 512
DNODES_PER_BLOCK = 32


class RecoveryError(RuntimeError):
    """Raised when recovery input fails a required validation."""


@dataclass(frozen=True)
class CandidateSpec:
    path: Path
    dataset_object: int
    creation_txg: int | None = None
    directory_object: int | None = None


def parse_candidate_spec(value: str) -> CandidateSpec:
    """Parse PATH,OBJECT[,CREATION_TXG[,DIRECTORY_OBJECT]]."""

    fields = value.rsplit(",", 3)
    if len(fields) not in (2, 3, 4):
        raise ValueError(
            "candidate must be PATH,OBJECT[,CREATION_TXG[,DIRECTORY_OBJECT]]"
        )
    path = Path(fields[0])
    try:
        dataset_object = int(fields[1], 0)
        creation_txg = int(fields[2], 0) if len(fields) >= 3 and fields[2] else None
        directory_object = int(fields[3], 0) if len(fields) >= 4 and fields[3] else None
    except ValueError as error:
        raise ValueError(f"invalid candidate number: {error}") from error
    return CandidateSpec(path, dataset_object, creation_txg, directory_object)


def open_raidz(
    disks_file: str | Path,
    label_path: str | Path,
    *,
    parity: int = 1,
    bad_disks: list[int] | None = None,
) -> tuple[RaidzDevice, Label]:
    """Open a RAIDZ vdev through a local files block-provider definition."""

    if parity != 1:
        raise RecoveryError("the current RAIDZ implementation supports RAIDZ1 only")
    disks_file = Path(disks_file)
    label_path = Path(label_path)
    if not disks_file.is_file():
        raise FileNotFoundError(disks_file)
    if not label_path.exists():
        raise FileNotFoundError(label_path)

    provider = ("files:", str(disks_file))
    proxy = BlockProxy(provider)
    label = Label(proxy, str(label_path))
    label.read(0)
    bad = list(bad_disks or [])
    if len(bad) > parity:
        raise RecoveryError("more failed children were specified than RAIDZ parity")
    vdev = RaidzDevice(
        label.get_vdev_disks(),
        parity,
        provider,
        bad=bad,
        ashift=label._ashift,
        repair=bool(bad),
    )
    return vdev, label


def read_dataset_dnode(spec: CandidateSpec) -> DNode:
    """Read and validate a dataset dnode from a saved MOS dnode block."""

    if not spec.path.is_file():
        raise FileNotFoundError(spec.path)
    data = spec.path.read_bytes()
    slot = spec.dataset_object % DNODES_PER_BLOCK
    start = slot * DNODE_SIZE
    if len(data) < start + DNODE_SIZE:
        raise RecoveryError(f"candidate {spec.path} is too short for dnode slot {slot}")
    dnode = DNode(data[start : start + DNODE_SIZE])
    if not isinstance(dnode.bonus, BonusDataset):
        raise RecoveryError(f"dnode slot {slot} in {spec.path} is not a DSL dataset")
    if (
        spec.creation_txg is not None
        and dnode.bonus.ds_creation_txg != spec.creation_txg
    ):
        raise RecoveryError(
            f"candidate creation TXG is {dnode.bonus.ds_creation_txg}, "
            f"expected {spec.creation_txg}"
        )
    if (
        spec.directory_object is not None
        and dnode.bonus.ds_dir_obj != spec.directory_object
    ):
        raise RecoveryError(
            f"candidate directory object is {dnode.bonus.ds_dir_obj}, "
            f"expected {spec.directory_object}"
        )
    return dnode


def open_dataset(
    spec: CandidateSpec,
    disks_file: str | Path,
    label_path: str | Path,
    *,
    bad_disks: list[int] | None = None,
) -> tuple[Dataset, RaidzDevice, DNode]:
    """Open and analyse a recovered dataset candidate."""

    vdev, _ = open_raidz(disks_file, label_path, bad_disks=bad_disks)
    dnode = read_dataset_dnode(spec)
    dataset = Dataset(vdev, dnode)
    if dataset.broken:
        raise RecoveryError(f"cannot open dataset from {spec.path}")
    dataset.analyse()
    if dataset._rootdir_id is None:
        raise RecoveryError(f"dataset root is unavailable in {spec.path}")
    return dataset, vdev, dnode
