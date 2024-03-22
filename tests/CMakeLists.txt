include_directories( ${CMAKE_CURRENT_SOURCE_DIR} )

add_subdirectory("utils/")
add_subdirectory("unit_tests/")

file(GLOB TEST_SOURCE ${TEST_SOURCE} "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
file(GLOB TEST_SOURCE ${TEST_SOURCE} "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp")
        
# unit tests
set(TARGET_TESTS tests${TARGET_SUFFIX}.x)
add_executable(${TARGET_TESTS}
    ${TEST_SOURCE}
    $<TARGET_OBJECTS:unit_test_utils>
    $<TARGET_OBJECTS:unit_tests>
)
    
find_package(Python COMPONENTS Interpreter Development)

target_link_libraries(
    ${TARGET_TESTS}
    ${CONAN_LIBS}
    ${DBZeroLib}
    ${Python_LIBRARIES}
)
