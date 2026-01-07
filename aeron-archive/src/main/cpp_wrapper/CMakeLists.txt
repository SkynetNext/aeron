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

# SBE Codec generation for C++
set(ARCHIVE_CODEC_SCHEMA_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../resources/archive")
set(ARCHIVE_CODEC_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(ARCHIVE_CODEC_WORKING_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../..")

set(CODEC_SCHEMA ${ARCHIVE_CODEC_SCHEMA_DIR}/aeron-archive-codecs.xml)
set(SBE_XSD ${ARCHIVE_CODEC_SCHEMA_DIR}/fpl/sbe.xsd)

# Note: SBE generates files in aeron_archive_client/ subdirectory (default behavior)
set(GENERATED_CPP_CODECS
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_archive_client/MessageHeader.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_archive_client/ControlResponse.h
)

# Find gradle wrapper
find_program(GRADLE_WRAPPER
    NAMES gradlew.bat gradlew
    PATHS ${ARCHIVE_CODEC_WORKING_DIR}
    NO_DEFAULT_PATH
)

if (NOT GRADLE_WRAPPER)
    find_program(GRADLE_WRAPPER
        NAMES gradlew.bat gradlew
        PATHS ${ARCHIVE_CODEC_WORKING_DIR}
    )
endif()

if (GRADLE_WRAPPER)
    if (WIN32)
        set(GRADLE_WRAPPER_CMD ${GRADLE_WRAPPER})
    else()
        set(GRADLE_WRAPPER_CMD ${GRADLE_WRAPPER})
    endif()

    add_custom_command(OUTPUT ${GENERATED_CPP_CODECS}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${ARCHIVE_CODEC_TARGET_DIR}
        COMMAND ${CMAKE_COMMAND} -E env 
            JAVA_HOME=$ENV{JAVA_HOME} 
            BUILD_JAVA_HOME=$ENV{BUILD_JAVA_HOME} 
            BUILD_JAVA_VERSION=$ENV{BUILD_JAVA_VERSION}
            ${GRADLE_WRAPPER_CMD} 
            -Dcodec.target.dir=${ARCHIVE_CODEC_TARGET_DIR}
            :aeron-archive:generateCppCodecs 
            --no-daemon 
            --console=plain 
            -q
        DEPENDS ${CODEC_SCHEMA} ${SBE_XSD}
        WORKING_DIRECTORY ${ARCHIVE_CODEC_WORKING_DIR}
        COMMENT "Generating C++ Archive codecs from SBE XML schema")

    add_custom_target(archive_cpp_codecs DEPENDS ${GENERATED_CPP_CODECS})
else()
    message(WARNING "Gradle wrapper not found, Archive C++ codecs will not be generated automatically")
    add_custom_target(archive_cpp_codecs)
endif()

SET(HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/AeronArchive.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/ArchiveContext.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/CredentialsSupplier.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/RecordingPos.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/ReplayMerge.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/ReplayParams.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/ReplicationParams.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/util/ArchiveExceptions.h
)

# header only library
add_library(aeron_archive_wrapper INTERFACE)
add_library(aeron::aeron_archive_wrapper ALIAS aeron_archive_wrapper)
target_include_directories(aeron_archive_wrapper
    INTERFACE
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
    "$<BUILD_INTERFACE:${AERON_C_ARCHIVE_SOURCE_PATH}>"
    "$<BUILD_INTERFACE:${AERON_CLIENT_WRAPPER_SOURCE_PATH}>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>"
    "$<INSTALL_INTERFACE:include/wrapper>"
)

add_dependencies(aeron_archive_wrapper archive_cpp_codecs)

target_sources(aeron_archive_wrapper INTERFACE "$<BUILD_INTERFACE:${HEADERS}>")

if (MSVC)
    string(REPLACE "/" "\\\\" NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
else ()
    set(NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
endif ()

if (NOT WIN32)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif ()

target_link_libraries(aeron_archive_wrapper INTERFACE ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_archive_wrapper
        EXPORT aeron-targets)
    install(DIRECTORY ./ DESTINATION include/wrapper FILES_MATCHING PATTERN "*.h")
endif ()
