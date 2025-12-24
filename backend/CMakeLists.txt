# ---------------------------------------------------------------------------
# Leanstore
# ---------------------------------------------------------------------------
project(leanstore)

# ---------------------------------------------------------------------------
set(COUNTERS_INI ${CMAKE_SOURCE_DIR}/shared-headers/counters.ini)
set(COUNTERS_HEADER ${CMAKE_SOURCE_DIR}/shared-headers/counters_config.hpp)

add_custom_command(
   OUTPUT ${COUNTERS_HEADER}
   COMMAND ${CMAKE_COMMAND} -E echo "Generating counters_config.h from counters.ini"
   COMMAND python3 ${CMAKE_SOURCE_DIR}/shared-headers/config_parser.py 
           ${COUNTERS_INI} ${COUNTERS_HEADER}
   DEPENDS ${COUNTERS_INI} ${CMAKE_SOURCE_DIR}/shared-headers/config_parser.py
   COMMENT "Generating counters config header file from counters.ini"
   VERBATIM
)
add_custom_target(gen_header DEPENDS ${COUNTERS_HEADER})

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE LEANSTORE_CC **.cpp **/**.cpp **.hpp **/**.hpp)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(leanstore STATIC ${LEANSTORE_CC})

OPTION(SANI "Compile leanstore with sanitizers" OFF)
IF(SANI)
  target_compile_options(leanstore PUBLIC -fsanitize=address)
  target_link_libraries(leanstore asan)
ENDIF(SANI)

add_dependencies(leanstore gen_header)
target_link_libraries(leanstore gflags Threads::Threads aio tbb atomic tabluate rapidjson fdp uring ${Boost_LIBRARIES}) #tbb

# ---------------------------------------------------------------------------
OPTION(PARANOID "Enable sanity checks in release mode" OFF)
IF(PARANOID)
  target_compile_definitions(leanstore PUBLIC PARANOID)
ENDIF(PARANOID)
# ---------------------------------------------------------------------------
SET(COUNTERS_LEVEL "" CACHE STRING "Which counters to leave in leanstore build")
IF(COUNTERS_LEVEL STREQUAL "all")
  target_compile_definitions(leanstore PUBLIC MACRO_COUNTERS_ALL)
ENDIF()

SET(CHECKS_LEVEL "default" CACHE STRING "Which checks to leave in leanstore build")
IF(CHECKS_LEVEL STREQUAL "default")
  IF(CMAKE_BUILD_TYPE MATCHES Debug)
    target_compile_definitions(leanstore PUBLIC MACRO_CHECK_DEBUG)
  ELSEIF(CMAKE_BUILD_TYPE MATCHES RelWithDebInfo OR CMAKE_BUILD_TYPE MATCHES Release)
    target_compile_definitions(leanstore PUBLIC MACRO_CHECK_RELEASE)
  ENDIF()
ELSEIF(CHECKS_LEVEL STREQUAL "debug")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_DEBUG)
ELSEIF(CHECKS_LEVEL STREQUAL "release")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_RELEASE)
ELSEIF(CHECKS_LEVEL STREQUAL "benchmark")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_BENCHMARK)
ENDIF()
# ---------------------------------------------------------------------------
target_include_directories(leanstore PUBLIC ${SHARED_INCLUDE_DIRECTORY})
target_include_directories(leanstore PRIVATE ${CMAKE_CURRENT_LIST_DIR})
# ---------------------------------------------------------------------------
set(LEANSTORE_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET leanstore APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LEANSTORE_INCLUDE_DIR})
