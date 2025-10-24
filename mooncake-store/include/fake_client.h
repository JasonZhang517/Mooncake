#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include "client.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

/**
 * @brief FakeClient simulates a remote data processing service using
 * an actual Mooncake Store Client instance for real data operations.
 *
 * This class demonstrates control-data plane separation by:
 * - Acting as a proxy that uses real Mooncake Store put/get operations
 * - Simulating remote data processing service behavior locally
 * - Providing a testbed for validating the separation architecture
 *
 * The design philosophy: Instead of forwarding requests via RPC to a remote
 * service, this "fake" implementation uses an embedded Client instance to
 * perform actual storage operations, enabling end-to-end testing without
 * deploying separate data processing services.
 */
class FakeClient {
   public:
    /**
     * @brief Constructor
     */
    FakeClient();
    
    /**
     * @brief Destructor
     */
    ~FakeClient();

    // Non-copyable
    FakeClient(const FakeClient&) = delete;
    FakeClient& operator=(const FakeClient&) = delete;

    /**
     * @brief Initialize the internal Mooncake Store client
     *
     * @param local_hostname Local host address (IP:Port)
     * @param metadata_connstring Connection string for metadata service (etcd)
     * @param protocol Transfer protocol ("rdma" or "tcp")
     * @param master_server_entry Master server address
     * @return ErrorCode::OK on success, error code on failure
     *
     * NOTE: This method is thread-safe and must be called before any data operations.
     */
    [[nodiscard]] ErrorCode Init(const std::string& local_hostname,
                                  const std::string& metadata_connstring,
                                  const std::string& protocol,
                                  const std::string& master_server_entry);

    /**
     * @brief Read data using Mooncake Store Get operation
     *
     * @param object_key Key of the object to read
     * @param slices Vector of slices to store the read data
     * @return ErrorCode::OK on success, error code otherwise
     *
     * This method uses the internal Client's Get() to read actual data from storage.
     */
    [[nodiscard]] ErrorCode Read(
        const std::string& object_key,
        std::vector<Slice>& slices);

    /**
     * @brief Write data using Mooncake Store Put operation
     *
     * @param object_key Key of the object to write
     * @param slices Vector of slices containing data to write
     * @param config Replication configuration
     * @return ErrorCode::OK on success, error code otherwise
     *
     * This method uses the internal Client's Put() to write actual data to storage.
     */
    [[nodiscard]] ErrorCode Write(
        const std::string& object_key,
        std::vector<Slice>& slices,
        const ReplicateConfig& config);

    /**
     * @brief Query object metadata without transferring data
     *
     * @param object_key Key of the object to query
     * @return Vector of replica descriptors on success, error code otherwise
     *
     * This method uses the internal Client's Query() to retrieve metadata.
     */
    [[nodiscard]] tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    Query(const std::string& object_key);

    /**
     * @brief Remove an object from storage
     *
     * @param object_key Key of the object to remove
     * @return ErrorCode::OK on success, error code otherwise
     */
    [[nodiscard]] ErrorCode Remove(const std::string& object_key);

    /**
     * @brief Batch read using Mooncake Store BatchGet operation
     *
     * @param object_keys Vector of object keys to read
     * @param slices_map Map of object keys to their data slices
     * @return Vector of results (one per request)
     *
     * This method uses the internal Client's BatchGet() for efficient batch reads.
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchRead(
        const std::vector<std::string>& object_keys,
        std::unordered_map<std::string, std::vector<Slice>>& slices_map);

    /**
     * @brief Batch write using Mooncake Store BatchPut operation
     *
     * @param object_keys Vector of object keys to write
     * @param batched_slices Vector of slice vectors (one per object)
     * @param config Replication configuration
     * @return Vector of results (one per request)
     *
     * This method uses the internal Client's BatchPut() for efficient batch writes.
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchWrite(
        const std::vector<std::string>& object_keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const ReplicateConfig& config);

    /**
     * @brief Check if client is initialized
     */
    [[nodiscard]] bool IsInitialized() const;

   private:
    /**
     * @brief Internal Mooncake Store client for real data operations
     */
    std::shared_ptr<Client> client_;
    mutable Mutex init_mutex_;
    bool initialized_;
};

}  // namespace mooncake
