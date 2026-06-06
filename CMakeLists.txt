cmake_minimum_required(VERSION 3.25)
project(momentum_hold CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

# ---- system dependencies (Arch packages) ----
find_package(CURL REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(fmt REQUIRED)
find_package(spdlog REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS s3)

# ---- M0 hello-worlds ----
add_executable(hello_rest apps/hello_rest.cpp)
target_link_libraries(hello_rest PRIVATE
    CURL::libcurl
    nlohmann_json::nlohmann_json
    fmt::fmt
    spdlog::spdlog
)

add_executable(hello_s3 apps/hello_s3.cpp)
target_link_libraries(hello_s3 PRIVATE
    ${AWSSDK_LINK_LIBRARIES}
    fmt::fmt
    spdlog::spdlog
)
target_include_directories(hello_s3 PRIVATE ${AWSSDK_INCLUDE_DIRS})
