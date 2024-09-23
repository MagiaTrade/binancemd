cmake_minimum_required(VERSION 3.23)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

project(binancemd VERSION 1.0 LANGUAGES CXX )

set(CMAKE_CXX_STANDARD 17)

include(cmake/binance_md.cmake)

option(BMD_BUILD_TESTS "Build the tests" ON)
option(BMD_BUILD_WITH_BEASTBOYS "Build beastboys dep along with binance-md" ON)

add_library(${PROJECT_NAME} STATIC
    ${BMD_HEADERS}
    ${BMD_SRCS}
)

set (INCLUDED_LIBS)

if(BMD_BUILD_WITH_BEASTBOYS)

  set(MGUTILS_BUILD_TESTS OFF)
  set(MGUTILS_BUILD_EXAMPLES OFF)
  set(MGUTILS_BUILD_WITH_LUA OFF)
  set(MGUTILS_BUILD_WITH_SOL OFF)
  set(BUILD_BB_TESTS OFF)
  set(BUILD_BB_EXAMPLES OFF)

  add_subdirectory(beastboys)

  list(APPEND INCLUDED_LIBS beast_boys)

endif()

target_include_directories(${PROJECT_NAME} PUBLIC
    ${BMD_INCLUDE_DIRS}
)

target_link_libraries(${PROJECT_NAME} PUBLIC
    ${INCLUDED_LIBS}
)

if(BMD_BUILD_TESTS)
  add_subdirectory(tests)
endif()
