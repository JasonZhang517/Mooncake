#!/bin/bash
# Script to build and test DataProcessorClient

set -e  # Exit immediately if a command exits with a non-zero status

# Color definitions
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="${SCRIPT_DIR}/.."

print_info "Project root directory: ${PROJECT_ROOT}"

# Create build directory
BUILD_DIR="${PROJECT_ROOT}/build"
if [ ! -d "${BUILD_DIR}" ]; then
    print_info "Creating build directory: ${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
fi

cd "${BUILD_DIR}"

# Configure CMake
print_info "Configuring CMake..."
cmake .. \
    -DCMAKE_BUILD_TYPE=Debug \
    -DWITH_STORE=ON \
    -DSTORE_USE_ETCD=ON \
    -DBUILD_TESTING=ON

if [ $? -ne 0 ]; then
    print_error "CMake configuration failed"
    exit 1
fi
print_success "CMake configured successfully"

# Build project
print_info "Starting compilation..."
make -j$(nproc) mooncake_store

if [ $? -ne 0 ]; then
    print_error "Build failed"
    exit 1
fi
print_success "Build successful"

# Build tests
print_info "Building test program..."
make -j$(nproc) data_processor_client_test

if [ $? -ne 0 ]; then
    print_error "Test build failed"
    exit 1
fi
print_success "Test build successful"

# Run tests
print_info "Running DataProcessorClient tests..."
cd "${BUILD_DIR}/mooncake-store/tests"

if [ ! -f "./data_processor_client_test" ]; then
    print_error "Test executable not found"
    exit 1
fi

# Set environment variables (optional)
export GLOG_logtostderr=1
export GLOG_v=0

./data_processor_client_test

if [ $? -ne 0 ]; then
    print_error "Test execution failed"
    exit 1
fi

print_success "All tests passed!"
print_info "Test summary:"
print_info "  - Build status: ✓"
print_info "  - Test status: ✓"
print_info "  - Test location: ${BUILD_DIR}/mooncake-store/tests/data_processor_client_test"
