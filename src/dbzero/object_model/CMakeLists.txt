file(GLOB srcs "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
file(GLOB hdrs "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp")

add_subdirectory(object/)
add_library(object_model OBJECT ${srcs} ${hdrs})
