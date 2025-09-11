
from synctool.core.enums import DataStatus
from typing import Dict, List, Any


def calculate_row_status(src_hashes: List[Dict[str, Any]], dest_hashes: List[Dict[str, Any]], unique_columns: List[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """Calculate row status based on hashes"""
    statuses = []
    result: list[dict[str, Any]] = []
    all_keys = {tuple(x[y] for y in unique_columns) for x in src_hashes} | {tuple(x[y] for y in unique_columns) for x in dest_hashes}
    src_map = {tuple(x[y] for y in unique_columns): x for x in src_hashes}
    dest_map = {tuple(x[y] for y in unique_columns): x for x in dest_hashes}
    for key in all_keys:
        src_hash: dict[str, Any] | None = src_map.get(key)
        dest_hash: dict[str, Any] | None = dest_map.get(key)
        sel: dict[str, Any] = src_hash or dest_hash
        if src_hash and dest_hash:
            if src_hash['hash__'] == dest_hash['hash__']:
                statuses.append(DataStatus.UNCHANGED)
            else:
                statuses.append(DataStatus.MODIFIED)
        elif src_hash is None:
            statuses.append(DataStatus.DELETED)
        elif dest_hash is None:
            statuses.append(DataStatus.ADDED)
        result.append(sel)
    return result, statuses
