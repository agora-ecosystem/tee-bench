cmake_minimum_required(VERSION 3.12)

project(FheJoin)

set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(SEAL 3.6.6 EXACT REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

add_executable(fhejoin fhe_join.cpp generator.cpp)

add_executable(dtejoin dte_join.cpp generator.cpp openssl_rsa.cpp rho/radix_join.cpp)

target_link_libraries(fhejoin SEAL::seal)
target_link_libraries(dtejoin OpenSSL::SSL Threads::Threads)
#target_link_libraries(dtejoin PRIVATE Threads::Threads)
