PROJECT(Memcached)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

INCLUDE(CheckIncludeFileCXX)

IF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
   EXECUTE_PROCESS(COMMAND git log -1 --pretty=format:%H
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                   OUTPUT_VARIABLE MEMCACHED_VERSION
                   ERROR_QUIET
                   OUTPUT_STRIP_TRAILING_WHITESPACE)
ELSE (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
   SET(MEMCACHED_VERSION "unknown")
ENDIF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)

CHECK_INCLUDE_FILE_CXX("atomic" HAVE_ATOMIC)
CHECK_INCLUDE_FILE_CXX("cstdatomic" HAVE_CSTDATOMIC)

IF (NOT (HAVE_ATOMIC OR HAVE_CSTDATOMIC))
  MESSAGE(FATAL_ERROR "Can't build without <atomic> or <cstdatomic>")
ENDIF (NOT (HAVE_ATOMIC OR HAVE_CSTDATOMIC))

CHECK_SYMBOL_EXISTS(memalign malloc.h HAVE_MEMALIGN)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
               ${CMAKE_CURRENT_BINARY_DIR}/config.h)

IF (ENABLE_DTRACE)
   ADD_DEFINITIONS(-DENABLE_DTRACE=1)
ENDIF (ENABLE_DTRACE)

ADD_CUSTOM_COMMAND(OUTPUT ${Memcached_BINARY_DIR}/memcached_dtrace.h
                   COMMAND
                     ${DTRACE} -h
                               -s ${Memcached_SOURCE_DIR}/memcached_dtrace.d
                               -o ${Memcached_BINARY_DIR}/memcached_dtrace.h
                   DEPENDS
                         memcached_dtrace.d
                   COMMENT "Generating DTrace probe header file"
                   VERBATIM)

INCLUDE_DIRECTORIES(BEFORE
                    ${LIBEVENT_INCLUDE_DIR}
                    ${SNAPPY_INCLUDE_DIR}
                    ${OPENSSL_INCLUDE_DIR}
                    ${CMAKE_INSTALL_PREFIX}/include
                    ${CMAKE_CURRENT_SOURCE_DIR}/include
                    ${CMAKE_CURRENT_BINARY_DIR}
                    ${CMAKE_CURRENT_SOURCE_DIR}
                    ${Platform_SOURCE_DIR}/include)

IF (MEMORY_ALLOCATOR)
   INCLUDE_DIRECTORIES(AFTER ${MALLOC_INCLUDE_DIR})
ELSE (MEMORY_ALLOCATOR)
   SET(MALLOC_LIBRARIES "")
ENDIF (MEMORY_ALLOCATOR)

IF (APPLE)
   SET(MEMORY_TRACKING_SRCS daemon/alloc_hooks.c
                            daemon/alloc_hooks.h
                            daemon/darwin_zone.c
                            daemon/darwin_zone.h)
ELSE (APPLE)
   SET(MEMORY_TRACKING_SRCS daemon/alloc_hooks.c
                            daemon/alloc_hooks.h)
ENDIF (APPLE)

IF (WIN32)
   INCLUDE_DIRECTORIES(AFTER ${Platform_SOURCE_DIR}/include/win32)
ELSE (WIN32)
   ADD_EXECUTABLE(mcbasher programs/mcbasher.cc)
   TARGET_LINK_LIBRARIES(mcbasher platform ${COUCHBASE_NETWORK_LIBS})
   ADD_EXECUTABLE(timedrun programs/timedrun.c)
ENDIF (WIN32)

ADD_SUBDIRECTORY(support-files)

#
# Add all of the libraries
#
ADD_LIBRARY(mcd_util SHARED
            utilities/config_parser.c
            utilities/engine_loader.c
            utilities/extension_loggers.c
            utilities/util.c)
ADD_LIBRARY(default_engine SHARED
            engines/default_engine/assoc.c
            engines/default_engine/default_engine.c
            engines/default_engine/items.c
            engines/default_engine/slabs.c)
ADD_LIBRARY(nobucket SHARED
            engines/nobucket/nobucket.c)
ADD_LIBRARY(bucket_engine SHARED
            engines/bucket_engine/bucket_engine.c
            engines/bucket_engine/topkeys.c
            engines/bucket_engine/genhash.c)
ADD_LIBRARY(basic_engine_testsuite SHARED testsuite/basic_engine_testsuite.c)
ADD_LIBRARY(blackhole_logger SHARED extensions/loggers/blackhole_logger.c)
ADD_LIBRARY(fragment_rw_ops SHARED extensions/protocol/fragment_rw.c)
ADD_LIBRARY(stdin_term_handler SHARED extensions/daemon/stdin_check.c)
ADD_LIBRARY(tap_mock_engine SHARED engines/tap_mock_engine/tap_mock_engine.cc)
ADD_LIBRARY(bucket_engine_mock_engine SHARED
            engines/bucket_engine/mock_engine.c
            engines/bucket_engine/genhash.c)
ADD_LIBRARY(file_logger SHARED extensions/loggers/file_logger.c)
ADD_LIBRARY(testapp_extension SHARED extensions/protocol/testapp_extension.c)
IF (BUILD_ENTERPRISE)
    ADD_LIBRARY(auditd SHARED auditd/auditd.c)
ELSE (BUILD_ENTERPRISE)
    ADD_LIBRARY(auditd SHARED auditd/auditd.c)
ENDIF (BUILD_ENTERPRISE)

SET_TARGET_PROPERTIES(default_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(nobucket PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(bucket_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(basic_engine_testsuite PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(blackhole_logger PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(fragment_rw_ops PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(stdin_term_handler PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(tap_mock_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(bucket_engine_mock_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(file_logger PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(testapp_extension PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(auditd PROPERTIES PREFIX "")

#
# All of the binaries we're building
#
ADD_EXECUTABLE(bucket_engine_testapp
               engines/bucket_engine/testapp.c
               engines/bucket_engine/genhash.c)
ADD_EXECUTABLE(engine_testapp programs/engine_testapp/engine_testapp.c
                              programs/engine_testapp/mock_server.c
                              programs/engine_testapp/mock_server.h
                              ${MEMORY_TRACKING_SRCS})
ADD_EXECUTABLE(cbsasladm programs/cbsasladm.c
                         programs/utilities.c
                         programs/utilities.h)
ADD_EXECUTABLE(mcstat programs/mcstat.c
                      programs/utilities.c
                      programs/utilities.h)
ADD_EXECUTABLE(mctimings programs/mctimings.c
                         programs/utilities.c
                         programs/utilities.h
                         utilities/protocol2text.c)
ADD_EXECUTABLE(mcctl programs/mcctl.c
                     programs/utilities.c
                     programs/utilities.h
                     utilities/util.c)

ADD_EXECUTABLE(mcbucket programs/mcbucket.c
                     programs/utilities.c
                     programs/utilities.h
                     utilities/util.c)
ADD_EXECUTABLE(mchello programs/mchello.c
                       programs/utilities.c
                       programs/utilities.h)
ADD_EXECUTABLE(memcached_sizes tests/sizes.c)
ADD_EXECUTABLE(memcached
               ${MEMORY_TRACKING_SRCS}
               daemon/cache.c
               daemon/cmdline.c
               daemon/cmdline.h
               daemon/config_util.c
               daemon/config_util.h
               daemon/config_parse.c
               daemon/config_parse.h
               daemon/connections.c
               daemon/connections.h
               daemon/hash.c
               daemon/memcached.c
               daemon/privileges.c
               daemon/stats.c
               daemon/thread.c
               daemon/timings.cc
               daemon/mc_time.c
               daemon/rbac.cc
               daemon/rbac.h
               utilities/protocol2text.c)

IF (ENABLE_DTRACE)
   ADD_CUSTOM_TARGET(generate_memcached_dtrace_h
                     DEPENDS ${Memcached_BINARY_DIR}/memcached_dtrace.h)
   ADD_DEPENDENCIES(memcached generate_memcached_dtrace_h)
   ADD_DEPENDENCIES(default_engine generate_memcached_dtrace_h)

   IF (DTRACE_NEED_INSTUMENT)
      ADD_CUSTOM_COMMAND(TARGET memcached PRE_LINK
                         COMMAND
                         ${DTRACE} -o memcached_dtrace.o
                                   -G
                                   -s ${Memcached_SOURCE_DIR}/memcached_dtrace.d
                                   *.o
                         WORKING_DIRECTORY ${Memcached_BINARY_DIR}/CMakeFiles/memcached.dir/daemon)
      SET_TARGET_PROPERTIES(memcached PROPERTIES LINK_FLAGS
                            "${Memcached_BINARY_DIR}/CMakeFiles/memcached.dir/daemon/memcached_dtrace.o")

      ADD_CUSTOM_COMMAND(TARGET default_engine PRE_LINK
                         COMMAND
                         ${DTRACE} -o de_dtrace.o
                                   -G
                                   -s ${Memcached_SOURCE_DIR}/memcached_dtrace.d
                                   *.o
                         WORKING_DIRECTORY ${Memcached_BINARY_DIR}/CMakeFiles/default_engine.dir/engines/default_engine)
      SET_TARGET_PROPERTIES(default_engine PROPERTIES LINK_FLAGS
                            "${Memcached_BINARY_DIR}/CMakeFiles/default_engine.dir/engines/default_engine/de_dtrace.o")

   ENDIF (DTRACE_NEED_INSTUMENT)
ENDIF (ENABLE_DTRACE)

ADD_EXECUTABLE(memcached_config_test tests/config_test.c
               daemon/config_util.c daemon/config_util.h)
TARGET_LINK_LIBRARIES(memcached_config_test cJSON)
ADD_EXECUTABLE(ssltest tests/ssltest.c
                       programs/utilities.c
                       programs/utilities.h)

ADD_EXECUTABLE(memcached_testapp tests/testapp.c daemon/cache.c programs/utilities.c)

SET(CBSASL_SOURCES include/cbsasl/cbsasl.h include/cbsasl/visibility.h
                   cbsasl/client.c cbsasl/common.c cbsasl/cram-md5/cram-md5.c
                   cbsasl/cram-md5/cram-md5.h cbsasl/cram-md5/hmac.c
                   cbsasl/cram-md5/hmac.h cbsasl/cram-md5/md5.c
                   cbsasl/cram-md5/md5.h cbsasl/hash.c cbsasl/hash.h
                   cbsasl/plain/plain.c cbsasl/plain/plain.h cbsasl/pwfile.c
                   cbsasl/pwfile.h cbsasl/server.c cbsasl/strcmp.c
                   cbsasl/util.h)

ADD_LIBRARY(cbsasl SHARED ${CBSASL_SOURCES})
SET_TARGET_PROPERTIES(cbsasl PROPERTIES SOVERSION 1.1.1)
SET_TARGET_PROPERTIES(cbsasl PROPERTIES COMPILE_FLAGS -DBUILDING_CBSASL=1)
SET_TARGET_PROPERTIES(cbsasl PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib)

ADD_EXECUTABLE(cbsasl_hmac_test tests/cbsasl/hmac_test.c
                                cbsasl/cram-md5/hmac.c
                                cbsasl/cram-md5/hmac.h
                                cbsasl/cram-md5/md5.c
                                cbsasl/cram-md5/md5.h)

ADD_EXECUTABLE(cbsasl_pwfile_test tests/cbsasl/pwfile_test.c
                                  cbsasl/pwfile.c
                                  cbsasl/pwfile.h
                                  cbsasl/hash.c
                                  cbsasl/hash.h)

# We're using an "internal api" of the library in the test so we can't
# use the library we just built
ADD_EXECUTABLE(sasl_test tests/cbsasl/sasl_server_test.c ${CBSASL_SOURCES})
SET_TARGET_PROPERTIES(sasl_test PROPERTIES COMPILE_FLAGS -DBUILDING_CBSASL=1)

ADD_EXECUTABLE(cbsasl_test tests/cbsasl/cbsasl_test.c
                           include/cbsasl/cbsasl.h
                           include/cbsasl/visibility.h)
TARGET_LINK_LIBRARIES(cbsasl_test cbsasl)

TARGET_LINK_LIBRARIES(cbsasl platform)
TARGET_LINK_LIBRARIES(cbsasl_pwfile_test platform)
TARGET_LINK_LIBRARIES(sasl_test platform)

ADD_EXECUTABLE(cbsasl_strcmp_test tests/cbsasl/strcmp_test.c
                                  include/cbsasl/cbsasl.h
                                  include/cbsasl/visibility.h
                                  cbsasl/strcmp.c)

ADD_EXECUTABLE(config_parse_test  tests/config_parse_test.c
                                  daemon/config_util.c daemon/config_util.h
                                  daemon/cmdline.h daemon/cmdline.c
                                  utilities/util.c)
TARGET_LINK_LIBRARIES(config_parse_test cJSON platform ${COUCHBASE_NETWORK_LIBS})
ADD_TEST(memcache-config-parse config_parse_test)

IF(ENABLE_DTRACE)
   ADD_DEPENDENCIES(config_parse_test generate_memcached_dtrace_h)
ENDIF(ENABLE_DTRACE)


#
# Add linker flags to all of the binaries
#
TARGET_LINK_LIBRARIES(bucket_engine mcd_util platform ${COUCHBASE_NETWORK_LIBS} ${COUCHBASE_MATH_LIBS})
TARGET_LINK_LIBRARIES(default_engine mcd_util platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(basic_engine_testsuite mcd_util platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(stdin_term_handler platform)
TARGET_LINK_LIBRARIES(fragment_rw_ops mcd_util platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(engine_testapp mcd_util platform ${MALLOC_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(bucket_engine_testapp mcd_util platform ${COUCHBASE_NETWORK_LIBS} ${COUCHBASE_MATH_LIBS})
TARGET_LINK_LIBRARIES(cbsasladm platform ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(mcstat platform ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(mctimings cJSON platform ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(mcctl platform ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(mcbucket platform ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(mchello platform ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(ssltest platform ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(tap_mock_engine platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(testapp_extension mcd_util platform ${COUCHBASE_NETWORK_LIBS})

TARGET_LINK_LIBRARIES(mcd_util platform)
TARGET_LINK_LIBRARIES(memcached auditd mcd_util cbsasl platform cJSON JSON_checker ${SNAPPY_LIBRARIES} ${MALLOC_LIBRARIES} ${LIBEVENT_LIBRARIES} ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(memcached_testapp mcd_util cbsasl cJSON platform ${SNAPPY_LIBRARIES} ${LIBEVENT_LIBRARIES} ${COUCHBASE_NETWORK_LIBS} ${OPENSSL_LIBRARIES})

TARGET_LINK_LIBRARIES(file_logger platform)

IF (INSTALL_HEADER_FILES)
   INSTALL (FILES include/memcached/allocator_hooks.h
                  include/memcached/callback.h
                  include/memcached/config_parser.h
                  include/memcached/engine.h
                  include/memcached/engine_common.h
                  include/memcached/engine_testapp.h
                  include/memcached/extension.h
                  include/memcached/extension_loggers.h
                  include/memcached/protocol_binary.h
                  include/memcached/protocol_plugin.h
                  include/memcached/server_api.h
                  include/memcached/types.h
                  include/memcached/dcp.h
                  include/memcached/util.h
                  include/memcached/vbucket.h
                  include/memcached/visibility.h
            DESTINATION include/memcached)

   INSTALL(FILES
           include/cbsasl/cbsasl.h
           include/cbsasl/visibility.h
           DESTINATION include/cbsasl)
ENDIF(INSTALL_HEADER_FILES)

SET_TARGET_PROPERTIES(mcd_util PROPERTIES SOVERSION 1.0.0)
INSTALL(TARGETS mcd_util
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/memcached
        ARCHIVE DESTINATION lib/memcached)

INSTALL(TARGETS default_engine nobucket bucket_engine blackhole_logger
                fragment_rw_ops stdin_term_handler file_logger
                auditd
        RUNTIME DESTINATION lib/memcached
        LIBRARY DESTINATION lib/memcached
        ARCHIVE DESTINATION lib/memcached)

INSTALL(FILES daemon/rbac.json DESTINATION etc/security)
IF (BUILD_ENTERPRISE)
    INSTALL(FILES auditd/audit.json DESTINATION etc/security)
ELSE (BUILD_ENTERPRISE)
    INSTALL(FILES auditd/audit.json DESTINATION etc/security)
ENDIF (BUILD_ENTERPRISE)


SET_TARGET_PROPERTIES(mcd_util PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(default_engine PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(bucket_engine PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(blackhole_logger PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(fragment_rw_ops PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(stdin_term_handler PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(file_logger PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(auditd PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)

INSTALL(TARGETS engine_testapp cbsasladm mcctl mcbucket mcstat mctimings memcached
        RUNTIME DESTINATION bin)

INSTALL(TARGETS cbsasl
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

ADD_TEST(memcached-config memcached_config_test)
ADD_TEST(memcached-sizes memcached_sizes)
ADD_TEST(memcached-basic-unit-tests-plain memcached_testapp plain)
ADD_TEST(memcached-basic-unit-tests-SSL memcached_testapp ssl)
ADD_TEST(memcached-bucket_engine-unit-tests bucket_engine_testapp)
ADD_TEST(memcached-basic-engine-tests engine_testapp -E default_engine.so -T basic_engine_testsuite.so)

IF(${COUCHBASE_PYTHON})
    FOREACH(ID RANGE 9)
        LIST(APPEND GENERATED_SRCS ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_${ID}.c)
    ENDFOREACH()

    ADD_CUSTOM_COMMAND(OUTPUT ${GENERATED_SRCS}
                       COMMAND
                           ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/testsuite/breakdancer/engine_test.py
                       DEPENDS
                           testsuite/breakdancer/engine_test.py
                           testsuite/breakdancer/breakdancer.py
                       COMMENT "Generating testsuite")

    # Dummy target to prevent cmake trying to run multiple instances of the
    # above "Generating testsuite" command. The actual shared libraries
    # below in turn depend on this, causing them to be 'blocked' behind this.
    ADD_CUSTOM_TARGET(DUMMY_generated_suite
                      DEPENDS ${GENERATED_SRCS}
                      COMMENT "Waiting for testsuite generation to complete")

    FOREACH(ID RANGE 9)
        ADD_LIBRARY(memcached_generated_testsuite_${ID} SHARED
                    testsuite/breakdancer/suite_stubs.c
                    ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_${ID}.c)
        SET_TARGET_PROPERTIES(memcached_generated_testsuite_${ID} PROPERTIES PREFIX "")
        ADD_DEPENDENCIES(memcached_generated_testsuite_${ID}
                         DUMMY_generated_suite)

        ADD_TEST(memcached-breakdancer-engine-tests_${ID}
                 engine_testapp -E default_engine.so -T memcached_generated_testsuite_${ID}.so)
    ENDFOREACH()
ENDIF(${COUCHBASE_PYTHON})

IF (("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "tcmalloc") OR
    ("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "jemalloc"))
   ADD_EXECUTABLE(memcached_tcmalloc_test ${MEMORY_TRACKING_SRCS}
                                          utilities/extension_loggers.c
                                          tests/tcmalloc_test.cc)
   TARGET_LINK_LIBRARIES(memcached_tcmalloc_test platform ${MALLOC_LIBRARIES})
   ADD_TEST(memcached-tcmalloc-hooks memcached_tcmalloc_test)
ENDIF (("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "tcmalloc") OR
       ("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "jemalloc"))

CONFIGURE_FILE(tests/cert/testapp.pem tests/cert/testapp.pem COPYONLY)
CONFIGURE_FILE(tests/cert/testapp.cert tests/cert/testapp.cert COPYONLY)
# Second copy to test changing at runtime
CONFIGURE_FILE(tests/cert/testapp.pem tests/cert/testapp2.pem COPYONLY)
CONFIGURE_FILE(tests/cert/testapp.cert tests/cert/testapp2.cert COPYONLY)

ADD_TEST(cbsasl-hmac cbsasl_hmac_test)
ADD_TEST(cbsasl-pwfile cbsasl_pwfile_test)
ADD_TEST(cbsasl-sasl sasl_test)
ADD_TEST(cbsasl-client-server cbsasl_test)
ADD_TEST(cbsasl-strcmp cbsasl_strcmp_test)

ADD_CUSTOM_TARGET(memcached-manpage
                  COMMAND a2x -L --doctype manpage --format manpage
                          ${CMAKE_CURRENT_SOURCE_DIR}/man/man4/memcached.json.4.txt
                  COMMAND a2x -L --doctype manpage --format pdf
                          ${CMAKE_CURRENT_SOURCE_DIR}/man/man4/memcached.json.4.txt
                  VERBATIM)

IF (NOT WIN32)
   INSTALL(FILES man/man4/memcached.json.4
           DESTINATION man/man4)
ENDIF(NOT WIN32)

INSTALL(FILES man/man4/memcached.json.4.pdf
        DESTINATION doc)

ADD_EXECUTABLE(memcached_logger_test tests/logger_test.cc)
TARGET_LINK_LIBRARIES(memcached_logger_test mcd_util file_logger dirutils)
ADD_TEST(memcached-logger-test-rotate memcached_logger_test rotate)
ADD_TEST(memcached-logger-test-dedupe memcached_logger_test dedupe)
