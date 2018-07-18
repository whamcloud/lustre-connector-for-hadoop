// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

# Default to release builds
set(CMAKE_BUILD_TYPE, Release)

include(JNIFlags.cmake NO_POLICY_SCOPE)

# Compile a library with both shared and static variants
function(add_dual_library LIBNAME)
    add_library(${LIBNAME} SHARED ${ARGN})
    add_library(${LIBNAME}_static STATIC ${ARGN})
    set_target_properties(${LIBNAME}_static PROPERTIES OUTPUT_NAME ${LIBNAME})
endfunction(add_dual_library)

# Link both a static and a dynamic target against some libraries
function(target_link_dual_libraries LIBNAME)
    target_link_libraries(${LIBNAME} ${ARGN})
    target_link_libraries(${LIBNAME}_static ${ARGN})
endfunction(target_link_dual_libraries)

function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)

function(dual_output_directory TGT DIR)
    output_directory(${TGT} "${DIR}")
    output_directory(${TGT}_static "${DIR}")
endfunction(dual_output_directory TGT DIR)

#
# This macro alters the behavior of find_package and find_library.
# It does this by setting the CMAKE_FIND_LIBRARY_SUFFIXES global variable. 
# You should save that variable before calling this function and restore it
# after you have accomplished your goal.
#
# The behavior is altered in two ways:
# 1. We always find shared libraries, never static;
# 2. We find shared libraries with the given version number.
#
# On Windows this function is a no-op.  Windows does not encode
# version number information information into library path names.
#
macro(set_find_shared_library_version LVERS)
    IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        # Mac OS uses .dylib
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".${LVERS}.dylib")
    ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
        # FreeBSD has always .so installed.
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
    ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
        # Windows doesn't support finding shared libraries by version.
    ELSE()
        # Most UNIX variants use .so
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".so.${LVERS}")
    ENDIF()
endmacro(set_find_shared_library_version LVERS)

if (NOT GENERATED_JAVAH)
    # Must identify where the generated headers have been placed
    MESSAGE(FATAL_ERROR "You must set the cmake variable GENERATED_JAVAH")
endif (NOT GENERATED_JAVAH)
find_package(JNI REQUIRED)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -O2")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT -D_GNU_SOURCE")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")

SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES CMAKE_FIND_LIBRARY_SUFFIXES)
set_find_shared_library_version("1")

include_directories(
    ${GENERATED_JAVAH}
    main/native/src
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
)

SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
add_dual_library(hadlustre
    main/native/src/lustre_iostreams.c
    main/native/src/lustre_native_io.c
    main/native/src/exception.c    
    main/native/src/errno_enum.c
    main/native/src/file_descriptor.c
    main/native/src/asyncio.c
    main/native/src/lustre_helper.c
)
set(LIB_AIO aio)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    #
    # By embedding '$ORIGIN' into the RPATH of libhadlustre.so,
    # dlopen will look in the directory containing libhadlustre.so.
    # However, $ORIGIN is not supported by all operating systems.
    #
    SET_TARGET_PROPERTIES(hadlustre
        PROPERTIES INSTALL_RPATH "\$ORIGIN/")
ENDIF()

target_link_dual_libraries(hadlustre
    ${LIB_AIO}
    ${JAVA_JVM_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
)
SET(LIBHADLUSTRE_VERSION "1.0.0")
SET_TARGET_PROPERTIES(hadlustre PROPERTIES
    SOVERSION ${LIBHADLUSTRE_VERSION})
dual_output_directory(hadlustre usr/local/lib)
