# Detailed File-Level Differences

## Files ONLY in coro_rpc_communicator (Not in Combined 4 PRs)

### Mooncake-EP New Files (26 files)

#### Build Files
- `mooncake-ep/CMakeLists.txt`
- `mooncake-ep/example/CMakeLists.txt`
- `mooncake-ep/include/CMakeLists.txt`
- `mooncake-ep/src/CMakeLists.txt`
- `mooncake-ep/tests/CMakeLists.txt`

#### Headers
- `mooncake-ep/include/mooncake_backend.h`
- `mooncake-ep/include/mooncake_ep_buffer.h`
- `mooncake-ep/include/mooncake_worker.cuh`

#### Source Files
- `mooncake-ep/src/mooncake_backend.cpp`
- `mooncake-ep/src/mooncake_ep_buffer.cpp`
- `mooncake-ep/src/mooncake_worker.cu`
- `mooncake-ep/src/mooncake_worker_thread.cpp`

### Python Integration Files (7 files)
- `mooncake-integration/ep/ep_py.cpp` - Python bindings for EP
- `mooncake-wheel/mooncake/README.md` - Documentation
- `mooncake-wheel/mooncake/mooncake_connector_v1.py` - Connector implementation (762 lines)
- `mooncake-wheel/mooncake/mooncake_ep_buffer.py` - Buffer implementation (156 lines)
- `mooncake-wheel/mooncake/vllm_v1_proxy_server.py` - Proxy server (272 lines)
- `mooncake-wheel/tests/test_mooncake_backend.py` - Backend tests
- `mooncake-wheel/tests/test_mooncake_backend_chunk.py` - Chunk tests
- `mooncake-wheel/tests/test_mooncake_backend_perf.py` - Performance tests

### Deleted/Refactored Files (6 deletions)
Old EP structure removed:
- `D mooncake-ep/csrc/CMakeLists.txt`
- `D mooncake-ep/csrc/mxa_ep.cpp`
- `D mooncake-ep/mxa_ep/__init__.py`
- `D mooncake-ep/mxa_ep/buffer.py`
- `D mooncake-ep/mxa_ep/utils.py`
- `D mooncake-ep/setup.py`

### Renamed/Moved Files (15 renames)
EP code reorganization (from csrc/ to include/src/):

#### Headers moved to include/
- `R100 mooncake-ep/csrc/compiler.h` → `mooncake-ep/include/mooncake_ibgda/compiler.h`
- `R100 mooncake-ep/csrc/configs.cuh` → `mooncake-ep/include/mooncake_ep_configs.cuh`
- `R100 mooncake-ep/csrc/memheap.h` → `mooncake-ep/include/mooncake_ibgda/memheap.h`
- `R100 mooncake-ep/csrc/mlx5_ifc.h` → `mooncake-ep/include/mooncake_ibgda/mlx5_ifc.h`
- `R100 mooncake-ep/csrc/mlx5_prm.h` → `mooncake-ep/include/mooncake_ibgda/mlx5_prm.h`
- `R100 mooncake-ep/csrc/os.h` → `mooncake-ep/include/mooncake_ibgda/os.h`
- `R099 mooncake-ep/csrc/launch.cuh` → `mooncake-ep/include/mooncake_ep_launch.cuh`
- `R099 mooncake-ep/csrc/mlx5gda.h` → `mooncake-ep/include/mooncake_ibgda/mlx5gda.h`
- `R099 mooncake-ep/csrc/utils.cuh` → `mooncake-ep/include/mooncake_ep_utils.cuh`
- `R098 mooncake-ep/csrc/exception.cuh` → `mooncake-ep/include/mooncake_ep_exception.cuh`
- `R077 mooncake-ep/csrc/api.cuh` → `mooncake-ep/include/mooncake_ep_api.cuh`
- `R066 mooncake-ep/csrc/event.hpp` → `mooncake-ep/include/mooncake_ep_event.h`

#### Implementation moved to src/
- `R097 mooncake-ep/csrc/mlx5gda.cpp` → `mooncake-ep/src/mooncake_ibgda/mlx5gda.cpp`
- `R087 mooncake-ep/csrc/mxa_kernel.cu` → `mooncake-ep/src/mooncake_ep_kernel.cu`

#### Tests reorganized
- `R085 mooncake-ep/tests/test_mxa.py` → `mooncake-wheel/tests/test_mooncake_ep.py`
- `R097 mooncake-ep/tests/utils.py` → `mooncake-wheel/tests/ep_test_utils.py`

### Modified Files with Additional Content (17 files)

#### Build System
- `M CMakeLists.txt` - Added WITH_TE, WITH_EP options, pybind11 handling
- `M dependencies.sh` - Updated dependencies
- `M mooncake-transfer-engine/src/CMakeLists.txt` - EP integration
- `M mooncake-transfer-engine/src/transport/CMakeLists.txt` - Build updates

#### Configuration & Common
- `M .devcontainer/Dockerfile` - Dev environment
- `M .typos.toml` - Typo configuration
- `M mooncake-common/common.cmake` - Common build configuration
- `M mooncake-common/src/CMakeLists.txt` - Common CMake
- `M mooncake-common/src/default_config.cpp` - Default configuration
- `M image/components.png` - Updated diagram

#### Python Integration
- `M mooncake-integration/CMakeLists.txt` - EP Python bindings
- `M mooncake-integration/allocator.py` - Allocator updates
- `M mooncake-integration/transfer_engine/transfer_engine_py.h` - Header updates

#### Build Scripts
- `M mooncake-p2p-store/build.sh` - Build script updates
- `M mooncake-transfer-engine/nvlink-allocator/build.sh` - NVLink build
- `M scripts/build_wheel.sh` - Wheel building with EP (42 lines changed)
- `M scripts/run_tests.sh` - Test running updates

#### Transfer Engine Examples
- `M mooncake-transfer-engine/example/transfer_engine_bench.cpp`
- `M mooncake-transfer-engine/example/transfer_engine_bench_with_notify.cpp`
- `M mooncake-transfer-engine/example/transfer_engine_bench_with_retry.cpp`
- `M mooncake-transfer-engine/example/transfer_engine_heterogeneous_ascend_perf_initiator.cpp`
- `M mooncake-transfer-engine/example/transfer_engine_validator.cpp`

#### Transfer Engine Tests
- `M mooncake-transfer-engine/tests/rdma_transport_test.cpp`
- `M mooncake-transfer-engine/tests/rdma_transport_test2.cpp`
- `M mooncake-transfer-engine/tests/tcp_transport_test.cpp`
- `M mooncake-transfer-engine/src/transport/nvlink_transport/nvlink_transport.cpp`

#### Python Wheel
- `M mooncake-wheel/pyproject.toml` - Project metadata
- `M mooncake-wheel/setup.py` - Setup with EP support (42 lines changed)
- `M mooncake-wheel/tests/test_distributed_object_store.py` - Test updates
- `M mooncake-wheel/tests/test_mooncake_config.py` - Config test updates
- `M mooncake-wheel/tests/test_mooncake_ep.py` - EP tests (37 lines changed)
- `M mooncake-wheel/tests/ep_test_utils.py` - Test utilities (6 lines changed)
- `M mooncake-wheel/tests/transfer_engine_initiator_test.py` - Initiator tests

## Summary

**Total additional files in coro_rpc_communicator**: 71 files
- New files: 26
- Deleted files: 6
- Renamed/moved files: 15
- Modified files with significant additional content: 24

**Content differences in common files**: 3,909 insertions, 1,123 deletions

The key difference is the **complete Mooncake-EP (Expert Parallelism) implementation** which includes:
1. Core EP backend and buffer implementations
2. Python bindings and integration layer
3. vLLM v1 proxy server support
4. Comprehensive test suite
5. Build system integration
6. Major code reorganization from flat csrc/ structure to proper include/src/ layout
