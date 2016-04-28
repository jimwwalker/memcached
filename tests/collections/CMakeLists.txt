ADD_EXECUTABLE(memcached_collections_test
               collections_test.cc
               ${PROJECT_SOURCE_DIR}/daemon/collections.cc
               ${PROJECT_SOURCE_DIR}/utilities/config_parser.c
               ${PROJECT_SOURCE_DIR}/utilities/util.c)
TARGET_LINK_LIBRARIES(memcached_collections_test gtest gtest_main platform)
ADD_TEST(NAME memcached-collections-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_collections_test)