cmake_minimum_required(VERSION 3.10)
project(hlcup2018)

set(CMAKE_CXX_STANDARD 14)

option(WITH_MEASURE_RESP_TIME "Measure response time" OFF)
option(WITH_DPROF "Enable profiler" OFF)

add_executable(hlcup2018 main.cpp db/json.h db/DB.cpp db/DB.h db/Account.cpp db/Account.h db/Iterators.h Server.cpp Server.h Worker.cpp Worker.h Connection.cpp Connection.h Buffer.h db/utils.h db/dictionary.h db/ids_set.h db/groups.h db/groups.cpp db/UpdateQueue.cpp db/UpdateQueue.h)
add_definitions(-D_PTHREADS)
set(CMAKE_CXX_FLAGS "-march=native")
target_link_libraries(hlcup2018 zip pthread)

if(WITH_MEASURE_RESP_TIME)
    MESSAGE(STATUS "WITH_MEASURE_RESP_TIME")
    add_definitions(-DMEASURE_TIME)
endif()

if(WITH_DPROF)
    MESSAGE(STATUS "WITH_DPROF")
    add_definitions(-DPPROF)
    target_link_libraries(hlcup2018 profiler)
endif()

