#include "fake_client.h"

#include <glog/logging.h>

namespace mooncake {

FakeClient::FakeClient() : client_(nullptr), initialized_(false) {
    LOG(INFO) << "FakeClient instance created";
}

FakeClient::~FakeClient() {
    LOG(INFO) << "FakeClient instance destroyed";
}

ErrorCode FakeClient::Init(const std::string& local_hostname,
                            const std::string& metadata_connstring,
                            const std::string& protocol,
                            const std::string& master_server_entry) {
    std::lock_guard<Mutex> lock(init_mutex_);
    
    if (initialized_) {
        LOG(WARNING) << "FakeClient already initialized, reinitializing...";
    }

    // Create Mooncake Store Client instance
    void* protocol_args = nullptr;
    auto client_opt = Client::Create(local_hostname, metadata_connstring,
                                      protocol, &protocol_args,
                                      master_server_entry);
    
    if (!client_opt.has_value()) {
        LOG(ERROR) << "Failed to create Mooncake Store Client";
        initialized_ = false;
        return ErrorCode::INVALID_PARAMS;
    }

    client_ = client_opt.value();
    initialized_ = true;

    LOG(INFO) << "FakeClient initialized successfully with local_hostname="
              << local_hostname << ", protocol=" << protocol;
    
    return ErrorCode::OK;
}

ErrorCode FakeClient::Read(const std::string& object_key,
                            std::vector<Slice>& slices) {
    if (!initialized_ || !client_) {
        LOG(ERROR) << "FakeClient not initialized";
        return ErrorCode::INVALID_PARAMS;
    }

    LOG(INFO) << "FakeClient::Read object_key=" << object_key;

    auto result = client_->Get(object_key, slices);
    if (!result.has_value()) {
        ErrorCode ec = result.error();
        LOG(ERROR) << "FakeClient::Read failed for object_key=" << object_key
                   << ", error=" << static_cast<int>(ec);
        return ec;
    }

    LOG(INFO) << "FakeClient::Read succeeded for object_key=" << object_key;
    return ErrorCode::OK;
}

ErrorCode FakeClient::Write(const std::string& object_key,
                             std::vector<Slice>& slices,
                             const ReplicateConfig& config) {
    if (!initialized_ || !client_) {
        LOG(ERROR) << "FakeClient not initialized";
        return ErrorCode::INVALID_PARAMS;
    }

    LOG(INFO) << "FakeClient::Write object_key=" << object_key;

    auto result = client_->Put(object_key, slices, config);
    if (!result.has_value()) {
        ErrorCode ec = result.error();
        LOG(ERROR) << "FakeClient::Write failed for object_key=" << object_key
                   << ", error=" << static_cast<int>(ec);
        return ec;
    }

    LOG(INFO) << "FakeClient::Write succeeded for object_key=" << object_key;
    return ErrorCode::OK;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
FakeClient::Query(const std::string& object_key) {
    if (!initialized_ || !client_) {
        LOG(ERROR) << "FakeClient not initialized";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    LOG(INFO) << "FakeClient::Query object_key=" << object_key;

    auto result = client_->Query(object_key);
    if (!result.has_value()) {
        ErrorCode ec = result.error();
        LOG(ERROR) << "FakeClient::Query failed for object_key=" << object_key
                   << ", error=" << static_cast<int>(ec);
        return tl::make_unexpected(ec);
    }

    LOG(INFO) << "FakeClient::Query succeeded for object_key=" << object_key
              << ", replica_count=" << result.value().size();
    return result.value();
}

ErrorCode FakeClient::Remove(const std::string& object_key) {
    if (!initialized_ || !client_) {
        LOG(ERROR) << "FakeClient not initialized";
        return ErrorCode::INVALID_PARAMS;
    }

    LOG(INFO) << "FakeClient::Remove object_key=" << object_key;

    auto result = client_->Remove(object_key);
    if (!result.has_value()) {
        ErrorCode ec = result.error();
        LOG(ERROR) << "FakeClient::Remove failed for object_key=" << object_key
                   << ", error=" << static_cast<int>(ec);
        return ec;
    }

    LOG(INFO) << "FakeClient::Remove succeeded for object_key=" << object_key;
    return ErrorCode::OK;
}

std::vector<tl::expected<void, ErrorCode>> FakeClient::BatchRead(
    const std::vector<std::string>& object_keys,
    std::unordered_map<std::string, std::vector<Slice>>& slices_map) {
    
    if (!initialized_ || !client_) {
        LOG(ERROR) << "FakeClient not initialized";
        std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());
        for (auto& result : results) {
            result = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return results;
    }

    LOG(INFO) << "FakeClient::BatchRead count=" << object_keys.size();

    auto results = client_->BatchGet(object_keys, slices_map);

    LOG(INFO) << "FakeClient::BatchRead completed, results=" << results.size();
    return results;
}

std::vector<tl::expected<void, ErrorCode>> FakeClient::BatchWrite(
    const std::vector<std::string>& object_keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    
    if (!initialized_ || !client_) {
        LOG(ERROR) << "FakeClient not initialized";
        std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());
        for (auto& result : results) {
            result = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return results;
    }

    if (object_keys.size() != batched_slices.size()) {
        LOG(ERROR) << "FakeClient::BatchWrite size mismatch: keys="
                   << object_keys.size() << ", slices=" << batched_slices.size();
        std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());
        for (auto& result : results) {
            result = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return results;
    }

    LOG(INFO) << "FakeClient::BatchWrite count=" << object_keys.size();

    auto results = client_->BatchPut(object_keys, batched_slices, config);

    LOG(INFO) << "FakeClient::BatchWrite completed, results=" << results.size();
    return results;
}

bool FakeClient::IsInitialized() const {
    std::lock_guard<Mutex> lock(init_mutex_);
    return initialized_;
}

}  // namespace mooncake
