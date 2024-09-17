cmake_minimum_required(VERSION 3.23)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

project(binancemd VERSION 1.0 LANGUAGES CXX )

set(CMAKE_CXX_STANDARD 17)

include(cmake/binance_md.cmake)

option(BMD_BUILD_TESTS "Build the tests" ON)
option(BMD_BUILD_EXAMPLES "Build the examples" ON)
option(BMD_BUILD_WITH_BEASTBOYS "Build beastboys dep along with binance-md" ON)

add_library(${PROJECT_NAME} STATIC
    ${BMD_HEADERS}
    ${BMD_SRCS}
)

# Include FetchContent module
include(FetchContent)

# Set the base directory for external dependencies
set(EXTERNAL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external")

# Ensure the external directory exists
file(MAKE_DIRECTORY ${EXTERNAL_DIR})
set (INCLUDED_LIBS)

if(BMD_BUILD_WITH_BEASTBOYS)
  message(STATUS "Fetching beastboys...")
  FetchContent_Declare(
      beastboys
      GIT_REPOSITORY https://github.com/MagiaTrade/beastboys.git
      GIT_TAG main
      SOURCE_DIR ${EXTERNAL_DIR}/beastboys
  )
  set(MGUTILS_BUILD_TESTS OFF)
  set(MGUTILS_BUILD_EXAMPLES OFF)
  set(MGUTILS_BUILD_WITH_LUA ON)
  set(MGUTILS_BUILD_WITH_SOL ON)
  set(BUILD_BB_TESTS OFF)
  set(BUILD_BB_EXAMPLES OFF)
  FetchContent_MakeAvailable(beastboys)
  message(STATUS "beastboys fetched and made available.")

  list(APPEND INCLUDED_LIBS beastboys)
endif()

target_include_directories(${PROJECT_NAME} PUBLIC
    ${BMD_INCLUDE_DIRS}
)

target_link_libraries(${PROJECT_NAME} PUBLIC
    ${INCLUDED_LIBS}
)
