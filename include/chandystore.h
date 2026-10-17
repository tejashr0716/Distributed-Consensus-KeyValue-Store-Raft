#ifndef CHANDYSTORE_H
#define CHANDYSTORE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#pragma pack(push, 8)
typedef struct {
    int64_t log_index;
    int64_t term;
    int32_t command_type; // 0: NOOP, 1: SET, 2: DELETE, 3: LOCK_ACQUIRE, 4: LOCK_RELEASE
    char key[64];
    char value[128];
    char client_id[64];
    uint32_t crc32;
} RaftLogEntry;

typedef struct {
    int64_t current_term;
    int64_t commit_index;
    int64_t last_applied;
    int32_t cluster_size;
    int32_t quorum_size;
    int32_t current_role; // 0: LEADER, 1: FOLLOWER, 2: CANDIDATE
    int64_t next_fencing_token;
} RaftClusterState;

typedef struct {
    int32_t success;
    int64_t fencing_token;
    int64_t lease_duration_ms;
    char error_message[64];
} LockResult;
#pragma pack(pop)

// CRC32 calculation for log payload integrity
uint32_t compute_entry_crc32(const RaftLogEntry* entry);

// Quorum log verification and commit advancement
int32_t verify_and_append_log(RaftClusterState* state, const RaftLogEntry* new_entry, int32_t match_count);

// Distributed lock acquisition with monotonic 64-bit fencing token
void acquire_distributed_lock(RaftClusterState* state, const char* lock_name, const char* client_id,
                             int64_t lease_ms, LockResult* out_result);

// Fencing token validation on lock release
int32_t release_distributed_lock(RaftClusterState* state, const char* lock_name, int64_t provided_fencing_token);

// Raft candidate election quorum evaluation
int32_t evaluate_election_quorum(int64_t candidate_term, int64_t candidate_last_log_index,
                                int64_t candidate_last_log_term, int32_t votes_granted, int32_t total_nodes);

#ifdef __cplusplus
}
#endif

#endif // CHANDYSTORE_H
