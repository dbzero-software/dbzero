#
# Required variables to be set:
#   DIR_DBZERO_LIB - DBZero installation directory
#
# Exports variables:
#   DBZERO_LIB_RELEASE, DBZERO_LIB_DEBUG
#   DBZERO_INCLUDE_DIR

if(NOT DEFINED DIR_DBZERO_LIB)
    message(FATAL_ERROR "Please set required variable DIR_DBZERO_LIB in upper cmake")
endif()
    
find_library(
    DBZERO_LIB_RELEASE REQUIRED
    NAMES DBZero
    PATHS ${DIR_DBZERO_LIB}/lib
    NO_DEFAULT_PATH
)

find_library(
    DBZERO_LIB_DEBUG REQUIRED
    NAMES DBZeroD
    PATHS ${DIR_DBZERO_LIB}/lib
    NO_DEFAULT_PATH
)

set(DBZERO_INCLUDE_DIR ${DIR_DBZERO_LIB}/include)

if(${CMAKE_BUILD_TYPE} MATCHES Release)
    set(DBZERO_LIB ${DBZERO_LIB_RELEASE})
elseif(${CMAKE_BUILD_TYPE} MATCHES Debug)
    set(DBZERO_LIB ${DBZERO_LIB_DEBUG})
endif()
