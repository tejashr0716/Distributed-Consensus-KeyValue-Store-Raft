import ctypes
import os
from typing import Dict, Any, Optional

class CRaftLogEntry(ctypes.Structure):
    _layout_ = "ms"
    _pack_ = 8
    _fields_ = [
        ("log_index", ctypes.c_int64),
        ("term", ctypes.c_int64),
        ("command_type", ctypes.c_int32),
        ("key", ctypes.c_char * 64),
        ("value", ctypes.c_char * 128),
        ("client_id", ctypes.c_char * 64),
        ("crc32", ctypes.c_uint32)
    ]

class CRaftClusterState(ctypes.Structure):
    _layout_ = "ms"
    _pack_ = 8
    _fields_ = [
        ("current_term", ctypes.c_int64),
        ("commit_index", ctypes.c_int64),
        ("last_applied", ctypes.c_int64),
        ("cluster_size", ctypes.c_int32),
        ("quorum_size", ctypes.c_int32),
        ("current_role", ctypes.c_int32),
        ("next_fencing_token", ctypes.c_int64)
    ]

class CLockResult(ctypes.Structure):
    _layout_ = "ms"
    _pack_ = 8
    _fields_ = [
        ("success", ctypes.c_int32),
        ("fencing_token", ctypes.c_int64),
        ("lease_duration_ms", ctypes.c_int64),
        ("error_message", ctypes.c_char * 64)
    ]

class ChandyStoreEngine:
    def __init__(self, dylib_path: Optional[str] = None):
        if dylib_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            dylib_path = os.path.join(base_dir, "libchandystore.dylib")

        if not os.path.exists(dylib_path):
            raise FileNotFoundError(f"Shared library not found at: {dylib_path}")

        self.lib = ctypes.CDLL(dylib_path)

        # uint32_t compute_entry_crc32(const RaftLogEntry* entry);
        self.lib.compute_entry_crc32.argtypes = [ctypes.POINTER(CRaftLogEntry)]
        self.lib.compute_entry_crc32.restype = ctypes.c_uint32

        # int32_t verify_and_append_log(RaftClusterState* state, const RaftLogEntry* new_entry, int32_t match_count);
        self.lib.verify_and_append_log.argtypes = [
            ctypes.POINTER(CRaftClusterState),
            ctypes.POINTER(CRaftLogEntry),
            ctypes.c_int32
        ]
        self.lib.verify_and_append_log.restype = ctypes.c_int32

        # void acquire_distributed_lock(...)
        self.lib.acquire_distributed_lock.argtypes = [
            ctypes.POINTER(CRaftClusterState),
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_int64,
            ctypes.POINTER(CLockResult)
        ]
        self.lib.acquire_distributed_lock.restype = None

        # int32_t release_distributed_lock(...)
        self.lib.release_distributed_lock.argtypes = [
            ctypes.POINTER(CRaftClusterState),
            ctypes.c_char_p,
            ctypes.c_int64
        ]
        self.lib.release_distributed_lock.restype = ctypes.c_int32

        # int32_t evaluate_election_quorum(...)
        self.lib.evaluate_election_quorum.argtypes = [
            ctypes.c_int64, ctypes.c_int64, ctypes.c_int64, ctypes.c_int32, ctypes.c_int32
        ]
        self.lib.evaluate_election_quorum.restype = ctypes.c_int32

    def compute_crc(self, log_index: int, term: int, cmd_type: int, key: str, val: str, client: str) -> int:
        entry = CRaftLogEntry(
            log_index=log_index,
            term=term,
            command_type=cmd_type,
            key=key.encode("utf-8"),
            value=val.encode("utf-8"),
            client_id=client.encode("utf-8"),
            crc32=0
        )
        return self.lib.compute_entry_crc32(ctypes.byref(entry))

    def verify_and_commit(self, state: Dict[str, Any], entry_data: Dict[str, Any], match_count: int) -> Dict[str, Any]:
        c_state = CRaftClusterState(
            current_term=state.get("current_term", 1),
            commit_index=state.get("commit_index", 0),
            last_applied=state.get("last_applied", 0),
            cluster_size=state.get("cluster_size", 3),
            quorum_size=state.get("quorum_size", 2),
            current_role=state.get("current_role", 0),
            next_fencing_token=state.get("next_fencing_token", 1000)
        )

        entry = CRaftLogEntry(
            log_index=entry_data["log_index"],
            term=entry_data["term"],
            command_type=entry_data["command_type"],
            key=entry_data["key"].encode("utf-8"),
            value=entry_data["value"].encode("utf-8"),
            client_id=entry_data["client_id"].encode("utf-8"),
            crc32=0
        )
        entry.crc32 = self.lib.compute_entry_crc32(ctypes.byref(entry))

        res = self.lib.verify_and_append_log(ctypes.byref(c_state), ctypes.byref(entry), match_count)

        return {
            "committed": bool(res),
            "commit_index": c_state.commit_index,
            "last_applied": c_state.last_applied,
            "crc32": entry.crc32
        }

    def acquire_lock(self, lock_name: str, client_id: str, lease_ms: int = 5000, current_fencing_token: int = 1000) -> Dict[str, Any]:
        c_state = CRaftClusterState(
            current_term=1,
            commit_index=0,
            last_applied=0,
            cluster_size=3,
            quorum_size=2,
            current_role=0,
            next_fencing_token=current_fencing_token
        )
        out_res = CLockResult()
        self.lib.acquire_distributed_lock(
            ctypes.byref(c_state),
            lock_name.encode("utf-8"),
            client_id.encode("utf-8"),
            lease_ms,
            ctypes.byref(out_res)
        )
        return {
            "success": bool(out_res.success),
            "fencing_token": out_res.fencing_token,
            "lease_duration_ms": out_res.lease_duration_ms,
            "message": out_res.error_message.decode("utf-8")
        }

    def release_lock(self, lock_name: str, fencing_token: int) -> bool:
        c_state = CRaftClusterState(next_fencing_token=fencing_token)
        res = self.lib.release_distributed_lock(ctypes.byref(c_state), lock_name.encode("utf-8"), fencing_token)
        return bool(res)

    def check_election_quorum(self, term: int, last_idx: int, last_term: int, votes: int, total_nodes: int = 3) -> bool:
        res = self.lib.evaluate_election_quorum(term, last_idx, last_term, votes, total_nodes)
        return bool(res)
