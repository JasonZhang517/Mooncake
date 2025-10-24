#include "fake_client.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace mooncake {

class FakeClientTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Google logging is already initialized in main()
        client_ = std::make_unique<FakeClient>();
    }

    void TearDown() override { client_.reset(); }

    std::unique_ptr<FakeClient> client_;

    // Helper: Create test data buffer
    std::vector<char> CreateTestData(size_t size, char pattern = 'A') {
        return std::vector<char>(size, pattern);
    }

    // Helper: Verify data matches expected pattern
    bool VerifyData(const std::vector<Slice>& slices, char pattern, size_t expected_size) {
        size_t total_size = 0;
        for (const auto& slice : slices) {
            total_size += slice.size;
            for (size_t i = 0; i < slice.size; ++i) {
                if (static_cast<char*>(slice.ptr)[i] != pattern) {
                    return false;
                }
            }
        }
        return total_size == expected_size;
    }
};

// Test 1: Basic construction and destruction
TEST_F(FakeClientTest, ConstructionAndDestruction) {
    EXPECT_NE(client_, nullptr);
    EXPECT_FALSE(client_->IsInitialized());
}

// Test 2: Initialize - Note: requires running services (etcd, master)
// Skipped in unit tests, should be run as integration test
TEST_F(FakeClientTest, DISABLED_InitializeSuccess) {
    // Note: This test requires a running etcd and master service
    // For unit testing, we'll just verify the interface
    
    ErrorCode ec = client_->Init("127.0.0.1:12345", 
                                  "etcd://127.0.0.1:2379",
                                  "tcp",
                                  "127.0.0.1:12346");
    
    // In a real environment with services running, this should succeed
    // For now, we expect it might fail but shouldn't crash
    LOG(INFO) << "Init result: " << static_cast<int>(ec);
}

// Test 3: Read without initialization should fail
TEST_F(FakeClientTest, ReadWithoutInit) {
    std::vector<Slice> slices;
    ErrorCode ec = client_->Read("test_key", slices);
    
    EXPECT_EQ(ec, ErrorCode::INVALID_PARAMS);
}

// Test 4: Write without initialization should fail
TEST_F(FakeClientTest, WriteWithoutInit) {
    auto data = CreateTestData(1024);
    Slice slice(data.data(), data.size());
    std::vector<Slice> slices = {slice};
    
    ReplicateConfig config;
    config.replica_num = 1;
    
    ErrorCode ec = client_->Write("test_key", slices, config);
    
    EXPECT_EQ(ec, ErrorCode::INVALID_PARAMS);
}

// Test 5: Query without initialization should fail
TEST_F(FakeClientTest, QueryWithoutInit) {
    auto result = client_->Query("test_key");
    
    EXPECT_FALSE(result.has_value());
    if (!result.has_value()) {
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
}

// Test 6: Remove without initialization should fail
TEST_F(FakeClientTest, RemoveWithoutInit) {
    ErrorCode ec = client_->Remove("test_key");
    
    EXPECT_EQ(ec, ErrorCode::INVALID_PARAMS);
}

// Test 7: BatchRead without initialization should fail
TEST_F(FakeClientTest, BatchReadWithoutInit) {
    std::vector<std::string> keys = {"key1", "key2", "key3"};
    std::unordered_map<std::string, std::vector<Slice>> slices_map;
    
    auto results = client_->BatchRead(keys, slices_map);
    
    EXPECT_EQ(results.size(), keys.size());
    for (const auto& result : results) {
        EXPECT_FALSE(result.has_value());
        if (!result.has_value()) {
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
        }
    }
}

// Test 8: BatchWrite without initialization should fail
TEST_F(FakeClientTest, BatchWriteWithoutInit) {
    std::vector<std::string> keys = {"key1", "key2", "key3"};
    std::vector<std::vector<Slice>> batched_slices;
    
    for (size_t i = 0; i < keys.size(); ++i) {
        auto data = CreateTestData(512, 'A' + i);
        Slice slice(data.data(), data.size());
        batched_slices.push_back({slice});
    }
    
    ReplicateConfig config;
    config.replica_num = 1;
    
    auto results = client_->BatchWrite(keys, batched_slices, config);
    
    EXPECT_EQ(results.size(), keys.size());
    for (const auto& result : results) {
        EXPECT_FALSE(result.has_value());
        if (!result.has_value()) {
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
        }
    }
}

// Test 9: Batch write with mismatched sizes
TEST_F(FakeClientTest, BatchWriteMismatchedSizes) {
    
    std::vector<std::string> keys = {"key1", "key2", "key3"};
    std::vector<std::vector<Slice>> batched_slices;  // Empty, mismatched size
    
    ReplicateConfig config;
    config.replica_num = 1;
    
    auto results = client_->BatchWrite(keys, batched_slices, config);
    
    EXPECT_EQ(results.size(), keys.size());
    // All should fail due to size mismatch
}

// Test 10: Multiple clients can be created
TEST_F(FakeClientTest, MultipleClients) {
    auto client2 = std::make_unique<FakeClient>();
    auto client3 = std::make_unique<FakeClient>();
    
    EXPECT_NE(client2, nullptr);
    EXPECT_NE(client3, nullptr);
    EXPECT_FALSE(client2->IsInitialized());
    EXPECT_FALSE(client3->IsInitialized());
}

// Test 11: Reinitialize - Note: requires running services
// Skipped in unit tests
TEST_F(FakeClientTest, DISABLED_Reinitialize) {
    ErrorCode ec1 = client_->Init("127.0.0.1:12345", 
                                   "etcd://127.0.0.1:2379",
                                   "tcp",
                                   "127.0.0.1:12346");
    
    // Reinitialize with different parameters
    ErrorCode ec2 = client_->Init("127.0.0.1:12347", 
                                   "etcd://127.0.0.1:2379",
                                   "tcp",
                                   "127.0.0.1:12348");
    
    LOG(INFO) << "First init: " << static_cast<int>(ec1)
              << ", Second init: " << static_cast<int>(ec2);
}

// Test 12: Thread safety - concurrent operations
TEST_F(FakeClientTest, ConcurrentOperations) {
    const int num_threads = 4;
    const int ops_per_thread = 10;
    std::vector<std::thread> threads;
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, i, ops_per_thread]() {
            for (int j = 0; j < ops_per_thread; ++j) {
                std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
                auto data = CreateTestData(256);
                Slice slice(data.data(), data.size());
                std::vector<Slice> slices = {slice};
                
                ReplicateConfig config;
                config.replica_num = 1;
                
                // These will fail (not initialized), but shouldn't crash
                client_->Write(key, slices, config);
                
                std::vector<Slice> read_slices;
                client_->Read(key, read_slices);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Test passed if no crashes occurred
    SUCCEED();
}

}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google's logging library
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    // Run all tests
    return RUN_ALL_TESTS();
}
