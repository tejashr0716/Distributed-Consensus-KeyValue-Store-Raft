#include "chandystore.h"
#include <iostream>
#include <chrono>
#include <vector>
#include <iomanip>
#include <cstring>

int main() {
    std::cout << "========================================================\n";
    std::cout << "   ChandyStore C++ CPU-only validation microbenchmark      \n";
    std::cout << "========================================================\n\n";

    RaftClusterState state{};
    state.current_term = 1;
    state.commit_index = 0;
    state.last_applied = 0;
    state.cluster_size = 5;
    state.quorum_size = 3;
    state.current_role = 0; // LEADER
    state.next_fencing_token = 1000;

    const int N = 3000000;
    RaftLogEntry entry{};
    entry.log_index = 100;
    entry.term = 1;
    entry.command_type = 1; // SET
    std::strncpy(entry.key, "account:10492:balance", sizeof(entry.key));
    std::strncpy(entry.value, "50000.75", sizeof(entry.value));
    std::strncpy(entry.client_id, "CLIENT_ALPHA", sizeof(entry.client_id));
    entry.crc32 = compute_entry_crc32(&entry);

    // 1. CRC32 Calculation Throughput
    auto start = std::chrono::high_resolution_clock::now();
    volatile uint32_t dummy_crc = 0;
    for (int i = 0; i < N; ++i) {
        dummy_crc ^= compute_entry_crc32(&entry);
    }
    auto end = std::chrono::high_resolution_clock::now();
    double crc_sec = std::chrono::duration<double>(end - start).count();
    double crc_ops = (double)N / crc_sec;
    double crc_ns = (crc_sec / (double)N) * 1e9;

    std::cout << ">> CRC32 Verification Throughput: " << std::fixed << std::setprecision(2)
              << (crc_ops / 1e6) << " Million entries/sec ("
              << crc_ns << " ns/op)\n";

    // 2. Quorum Log Verification & Commit Advancement
    start = std::chrono::high_resolution_clock::now();
    int32_t committed = 0;
    for (int i = 0; i < N; ++i) {
        entry.log_index = i + 1;
        entry.crc32 = compute_entry_crc32(&entry);
        committed += verify_and_append_log(&state, &entry, 3);
    }
    end = std::chrono::high_resolution_clock::now();
    if (committed != N || state.commit_index != N || state.last_applied != N) {
        std::cerr << "CORRECTNESS FAILURE\n"; return 2;
    }
    std::cout << "Attempted: " << N << " Successful: " << committed << " Failed: " << N-committed << "\n";
    double quorum_sec = std::chrono::duration<double>(end - start).count();
    double quorum_ops = (double)N / quorum_sec;
    double quorum_ns = (quorum_sec / (double)N) * 1e9;

    std::cout << ">> Local quorum validation + advancement:  " << std::fixed << std::setprecision(2)
              << (quorum_ops / 1e6) << " Million commits/sec ("
              << quorum_ns << " ns/op)\n";

    // 3. Monotonic Distributed Lock Fencing Token Generation
    LockResult lres;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < N; ++i) {
        acquire_distributed_lock(&state, "resource:inventory_lock", "WORKER_THREAD", 5000, &lres);
    }
    end = std::chrono::high_resolution_clock::now();
    double lock_sec = std::chrono::duration<double>(end - start).count();
    double lock_ops = (double)N / lock_sec;
    double lock_ns = (lock_sec / (double)N) * 1e9;

    std::cout << ">> Local non-atomic token increment:  " << std::fixed << std::setprecision(2)
              << (lock_ops / 1e6) << " Million local tokens/sec ("
              << lock_ns << " ns/op)\n";

    std::cout << "\nConsensus Final State:\n";
    std::cout << "  Current Term:   " << state.current_term << "\n";
    std::cout << "  Commit Index:   " << state.commit_index << "\n";
    std::cout << "  Fencing Token:  " << lres.fencing_token << "\n";
    std::cout << "========================================================\n";

    std::cout << "CRC sink: " << dummy_crc << " (zero is possible after XOR)\n";
    return 0;
}
