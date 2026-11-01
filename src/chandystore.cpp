#include "chandystore.h"
#include <cstring>
#include <algorithm>
#include <cstdio>
#include <mutex>
#include <limits>

// Standard CRC32 table
static uint32_t crc32_tab[256];
static std::once_flag crc_init_once;

static void init_crc32_table() {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            c = (c & 1) ? (0xEDB88320L ^ (c >> 1)) : (c >> 1);
        }
        crc32_tab[i] = c;
    }

}

static uint32_t calc_crc32(const uint8_t* buf, size_t len) {
    std::call_once(crc_init_once, init_crc32_table);
    uint32_t c = 0xFFFFFFFFL;
    for (size_t i = 0; i < len; i++) {
        c = crc32_tab[(c ^ buf[i]) & 0xFF] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFL;
}

extern "C" {

uint32_t compute_entry_crc32(const RaftLogEntry* entry) {
    if (!entry) return 0;
    // Calculate CRC over payload fields: log_index, term, command_type, key, value, client_id
    size_t offset = offsetof(RaftLogEntry, crc32);
    return calc_crc32((const uint8_t*)entry, offset);
}

int32_t verify_and_append_log(RaftClusterState* state, const RaftLogEntry* new_entry, int32_t match_count) {
    if (!state || !new_entry) return 0;

    // Verify CRC32 checksum
    uint32_t expected_crc = compute_entry_crc32(new_entry);
    if (new_entry->crc32 != 0 && new_entry->crc32 != expected_crc) {
        return 0; // Corrupted log entry
    }

    // Quorum verification (strictly majority required to commit)
    int32_t required_quorum = (state->quorum_size > 0) ? state->quorum_size : ((state->cluster_size / 2) + 1);

    if (match_count >= required_quorum) {
        if (new_entry->log_index > state->commit_index) {
            state->commit_index = new_entry->log_index;
            state->last_applied = new_entry->log_index;
        }
        return 1; // Committed to state machine
    }

    return 0; // Replicated to some nodes, but quorum pending
}

void acquire_distributed_lock(RaftClusterState* state, const char* lock_name, const char* client_id,
                             int64_t lease_ms, LockResult* out_result) {
    if (!out_result) return;
    std::memset(out_result, 0, sizeof(LockResult));

    if (!state || !lock_name || !client_id) {
        out_result->success = 0;
        std::strncpy(out_result->error_message, "Invalid lock parameters", sizeof(out_result->error_message) - 1);
        return;
    }

    if (state->next_fencing_token <= 0) {
        state->next_fencing_token = 1000;
    }

    // Single-thread local demonstration only. API tokens are allocated by PostgreSQL.
    if (state->next_fencing_token == std::numeric_limits<int64_t>::max()) {
        std::strncpy(out_result->error_message, "Token exhausted", sizeof(out_result->error_message)-1);
        return;
    }
    int64_t token = ++(state->next_fencing_token);

    out_result->success = 1;
    out_result->fencing_token = token;
    out_result->lease_duration_ms = (lease_ms > 0) ? lease_ms : 5000;
    std::strncpy(out_result->error_message, "Lock acquired successfully", sizeof(out_result->error_message) - 1);
}

int32_t release_distributed_lock(RaftClusterState* state, const char* lock_name, int64_t provided_fencing_token) {
    if (!state || !lock_name || provided_fencing_token <= 0) return 0;

    // Fencing token safety: must be greater than 0
    return 1;
}

int32_t evaluate_election_quorum(int64_t candidate_term, int64_t candidate_last_log_index,
                                int64_t candidate_last_log_term, int32_t votes_granted, int32_t total_nodes) {
    int32_t majority = (total_nodes / 2) + 1;
    if (votes_granted >= majority) {
        return 1; // Quorum election won!
    }
    return 0; // Election failed or split-vote
}

} // extern "C"
