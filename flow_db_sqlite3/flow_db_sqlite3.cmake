set(${m}_kautil_cmake_heeder CMakeKautilHeader.v0.0.cmake)
if(DEFINED KAUTIL_THIRD_PARTY_DIR AND EXISTS "${KAUTIL_THIRD_PARTY_DIR}/${${m}_kautil_cmake_heeder}")
    include("${KAUTIL_THIRD_PARTY_DIR}/${${m}_kautil_cmake_heeder}")
else()
    if(NOT EXISTS ${CMAKE_BINARY_DIR}/${${m}_kautil_cmake_heeder})
        file(DOWNLOAD https://raw.githubusercontent.com/kautils/CMakeKautilHeader/v0.0/CMakeKautilHeader.cmake ${CMAKE_BINARY_DIR}/${${m}_kautil_cmake_heeder})
    endif()
    include(${CMAKE_BINARY_DIR}/${${m}_kautil_cmake_heeder})
endif()

git_clone(https://raw.githubusercontent.com/kautils/CMakeLibrarytemplate/v0.0/CMakeLibrarytemplate.cmake)
git_clone(https://raw.githubusercontent.com/kautils/CMakeFetchKautilModule/v1.0/CMakeFetchKautilModule.cmake)


CMakeFetchKautilModule(sqlite
        GIT https://github.com/kautils/sqlite3.git
        REMOTE origin
        BRANCH v2.0
        CMAKE_CONFIGURE_MACRO -DCMAKE_CXX_FLAGS="-O2" -DCMAKE_CXX_STANDARD=23
        CMAKE_BUILD_OPTION -j ${${m}_thread_cnt})
find_package(KautilSqlite3.2.0.1.0.static REQUIRED)


set(module_name sqlite3)
unset(srcs)
file(GLOB srcs ${CMAKE_CURRENT_LIST_DIR}/*.cc)
set(${module_name}_common_pref
    MODULE_PREFIX kautil flow db
    MODULE_NAME ${module_name}
    INCLUDES $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}> $<INSTALL_INTERFACE:include> ${CMAKE_CURRENT_LIST_DIR} 
    SOURCES ${srcs}
    LINK_LIBS kautil::sqlite3::2.0.1.0::static
    EXPORT_NAME_PREFIX ${PROJECT_NAME}FilterD
    EXPORT_VERSION ${PROJECT_VERSION}
    EXPORT_VERSION_COMPATIBILITY AnyNewerVersion

    DESTINATION_INCLUDE_DIR include/kautil
    DESTINATION_CMAKE_DIR cmake
    DESTINATION_LIB_DIR lib
)

CMakeLibraryTemplate(${module_name} EXPORT_LIB_TYPE shared ${${module_name}_common_pref} )

set(__t ${${module_name}_shared_tmain})
add_executable(${__t})
target_sources(${__t} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/unit_test.cc)
target_link_libraries(${__t} PRIVATE ${${module_name}_shared})
target_compile_definitions(${__t} PRIVATE ${${module_name}_shared_tmain_ppcs})
