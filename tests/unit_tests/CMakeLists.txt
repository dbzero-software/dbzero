file(GLOB srcs "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
file(GLOB hdrs "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp")

add_library( unit_tests OBJECT ${srcs} ${hdrs} )
