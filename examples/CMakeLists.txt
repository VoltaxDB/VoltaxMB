cmake_minimum_required(VERSION 3.10)
project(voltaxmb_client)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(nlohmann_json REQUIRED)

add_executable(voltaxmb_client cpp_client.cpp)

target_link_libraries(voltaxmb_client PRIVATE nlohmann_json::nlohmann_json) 