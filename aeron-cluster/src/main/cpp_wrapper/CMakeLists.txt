#
# Copyright 2014-2025 Real Logic Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(BUILD_SHARED_LIBS ON)
endif ()

find_package(Java REQUIRED)

# SBE Codec generation
set(CLUSTER_CODEC_SCHEMA_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../resources/cluster")
set(CLUSTER_CODEC_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(CLUSTER_CODEC_WORKING_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../..")

set(CODEC_SCHEMA ${CLUSTER_CODEC_SCHEMA_DIR}/aeron-cluster-codecs.xml)
set(MARK_CODEC_SCHEMA ${CLUSTER_CODEC_SCHEMA_DIR}/aeron-cluster-mark-codecs.xml)
set(NODE_STATE_CODEC_SCHEMA ${CLUSTER_CODEC_SCHEMA_DIR}/aeron-cluster-node-state-codecs.xml)
set(SBE_XSD ${CLUSTER_CODEC_SCHEMA_DIR}/fpl/sbe.xsd)

# Note: SBE generates many header files, we list a few key ones to track generation
# The actual generated files will be in the generated directory structure
# SBE generates files in aeron_cluster_codecs/ subdirectory (default behavior for multiple XML files)
set(GENERATED_CPP_CODECS
    ${CLUSTER_CODEC_TARGET_DIR}/aeron_cluster_codecs/MessageHeader.h
    ${CLUSTER_CODEC_TARGET_DIR}/aeron_cluster_codecs/SessionMessageHeader.h
)

# Find gradle wrapper
find_program(GRADLE_WRAPPER
    NAMES gradlew.bat gradlew
    PATHS ${CLUSTER_CODEC_WORKING_DIR}
    NO_DEFAULT_PATH
)

if (NOT GRADLE_WRAPPER)
    find_program(GRADLE_WRAPPER
        NAMES gradlew.bat gradlew
        PATHS ${CLUSTER_CODEC_WORKING_DIR}
    )
endif()

if (GRADLE_WRAPPER)
    if (WIN32)
        set(GRADLE_WRAPPER_CMD ${GRADLE_WRAPPER})
    else()
        set(GRADLE_WRAPPER_CMD ${GRADLE_WRAPPER})
    endif()

    add_custom_command(OUTPUT ${GENERATED_CPP_CODECS}
        COMMAND ${CMAKE_COMMAND} -E env 
            JAVA_HOME=$ENV{JAVA_HOME} 
            BUILD_JAVA_HOME=$ENV{BUILD_JAVA_HOME} 
            BUILD_JAVA_VERSION=$ENV{BUILD_JAVA_VERSION}
            ${GRADLE_WRAPPER_CMD} 
            -Dcodec.target.dir=${CLUSTER_CODEC_TARGET_DIR}
            :aeron-cluster:generateCppCodecs 
            --no-daemon 
            --console=plain 
            -q
        DEPENDS ${CODEC_SCHEMA} ${MARK_CODEC_SCHEMA} ${NODE_STATE_CODEC_SCHEMA} ${SBE_XSD}
        WORKING_DIRECTORY ${CLUSTER_CODEC_WORKING_DIR}
        COMMENT "Generating C++ Cluster codecs from SBE XML schemas")

    add_custom_target(cluster_cpp_codecs DEPENDS ${GENERATED_CPP_CODECS})
else()
    message(WARNING "Gradle wrapper not found, C++ codecs will not be generated automatically")
    add_custom_target(cluster_cpp_codecs)
endif()

# Collect header files using glob patterns
file(GLOB_RECURSE HEADERS
    "${CMAKE_CURRENT_SOURCE_DIR}/cluster/**/*.h"
)

# Source files - explicitly list them for reliability
set(SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/cluster/ConsensusModuleAgent.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/cluster/Election.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/cluster/ConsensusModule.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/cluster/PendingServiceMessageTracker.cpp"
)

# Regular library with .cpp implementation files
add_library(aeron_cluster_wrapper ${SOURCES})
add_library(aeron::aeron_cluster_wrapper ALIAS aeron_cluster_wrapper)
target_include_directories(aeron_cluster_wrapper
    PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
    "$<BUILD_INTERFACE:${AERON_CLIENT_WRAPPER_SOURCE_PATH}>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>"
    "$<INSTALL_INTERFACE:include/wrapper>"
)

add_dependencies(aeron_cluster_wrapper cluster_cpp_codecs archive_cpp_codecs)

if (MSVC)
    string(REPLACE "/" "\\\\" NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
else ()
    set(NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
endif ()

if (NOT WIN32)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif ()

target_link_libraries(aeron_cluster_wrapper 
    PUBLIC 
    aeron_client_wrapper
    aeron_archive_wrapper
    ${CMAKE_THREAD_LIBS_INIT}
)

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_cluster_wrapper
        EXPORT aeron-targets)
    install(DIRECTORY ./ DESTINATION include/wrapper FILES_MATCHING PATTERN "*.h")
endif ()

