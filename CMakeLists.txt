cmake_minimum_required(VERSION 3.15)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

include(CheckCCompilerFlag)
include(CheckSymbolExists)
include(GitCommands)
include(GenerateScripts)
include(CMakeDependentOption)

option(APACHE_ONLY "only compile apache code" off)
# This requires all tests to run. This defaults to OFF but can be enabled to
# ensure that no tests are skipped because of missing tools.
option(REQUIRE_ALL_TESTS "Require all tests to run." OFF)
option(USE_OPENSSL "Enable use of OpenSSL if available" ON)
option(SEND_TELEMETRY_DEFAULT "The default value for whether to send telemetry"
       ON)
option(
  USE_TELEMETRY
  "Include telemetry functionality in the build. Disabling will exclude all telemetry code from the build."
  ON)

option(REGRESS_CHECKS "PostgreSQL regress checks through installcheck" ON)
option(
  ENABLE_OPTIMIZER_DEBUG
  "Enable OPTIMIZER_DEBUG when building. Requires Postgres server to be built with OPTIMIZER_DEBUG."
  OFF)
option(ENABLE_DEBUG_UTILS "Enable debug utilities for the extension." ON)

# Option to enable assertions. Note that if we include headers from a PostgreSQL
# build that has assertions enabled, we might inherit that setting without
# explicitly enabling assertions via the ASSERTIONS option defined here. Thus,
# this option is mostly useful to enable assertions when the PostgreSQL we
# compile against has it disabled.
option(ASSERTIONS "Compile with assertion checks (default OFF)" OFF)

# Function to call pg_config and extract values.
function(GET_PG_CONFIG var)
  set(_temp)

  # Only call pg_config if the variable didn't already have a value.
  if(NOT ${var})
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E env LC_MESSAGES=C ${PG_CONFIG} ${ARGN}
      OUTPUT_VARIABLE _temp
      OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()

  # On Windows, fields that are not recorded will be given the value "not
  # recorded", so we translate this into <var>-NOTFOUND to make it undefined.
  #
  # It will then also show as, e.g., "PG_LDFLAGS-NOTFOUND" in any string
  # interpolation, making it obvious that it is an undefined CMake variable.
  if("${_temp}" STREQUAL "not recorded")
    set(_temp ${var}-NOTFOUND)
  endif()

  set(${var}
      ${_temp}
      PARENT_SCOPE)
endfunction()

# Search paths for Postgres binaries
if(WIN32)
  find_path(
    PG_PATH postgres.exe
    PATHS "C:/PostgreSQL" "C:/Program Files/PostgreSQL"
    PATH_SUFFIXES bin 15/bin 16/bin
    DOC "The path to a PostgreSQL installation")
elseif(UNIX)
  find_path(
    PG_PATH postgres
    PATHS $ENV{HOME} /opt/local/pgsql /usr/local/pgsql /usr/lib/postgresql
    PATH_SUFFIXES bin 15/bin 16/bin
    DOC "The path to a PostgreSQL installation")
endif()

find_program(
  PG_CONFIG pg_config
  HINTS ${PG_PATH}
  PATH_SUFFIXES bin
  DOC "The path to the pg_config of the PostgreSQL version to compile against")

if(NOT PG_CONFIG)
  message(FATAL_ERROR "Unable to find 'pg_config'")
endif()

configure_file("version.config" "version.config" COPYONLY)
file(READ version.config VERSION_CONFIG)

if(VERSION_CONFIG
   MATCHES
   "(^|.*[^a-z])version[\t ]*=[\t ]*([0-9]+\\.[0-9]+\\.*[0-9]*)(-([a-z]+[0-9]*|dev))?.*"
)
  set(VERSION ${CMAKE_MATCH_2})
  set(VERSION_MOD ${CMAKE_MATCH_4}) # This is used in config.h
  if(CMAKE_MATCH_3)
    set(PROJECT_VERSION_MOD ${CMAKE_MATCH_2}${CMAKE_MATCH_3})
  else()
    set(PROJECT_VERSION_MOD ${CMAKE_MATCH_2})
  endif()
endif()

if(VERSION_CONFIG MATCHES
   ".*previous_version[\t ]*=[\t ]*([0-9]+\\.[0-9]+\\.[0-9]+(-[a-z]+[0-9]*)?).*"
)
  set(PREVIOUS_VERSION ${CMAKE_MATCH_1})
else()
  message(
    FATAL_ERROR "Could not determine previous version from version.config")
endif()

# a hack to avoid change of SQL extschema variable
set(extschema "@extschema@")

# If not explicitly specified, try to use the same compiler as Postgres to avoid
# mismatches and simplify configuration.
if((NOT DEFINED CMAKE_C_COMPILER) AND (NOT DEFINED ENV{CC}))
  get_pg_config(PG_CC --cc)

  # The first word might be ccache or similar, so handle this.
  string(REPLACE " " ";" PG_CC_LIST ${PG_CC})
  list(POP_FRONT PG_CC_LIST PG_CC_FIRST)
  if(PG_CC_LIST)
    # Got multi-word PG CC, treat the first word as ccache.
    find_program(PG_CCACHE_FOUND ${PG_CC_FIRST})
    if((NOT DEFINED CMAKE_C_COMPILER_LAUNCHER) AND PG_CCACHE_FOUND)
      message(
        STATUS
          "Using ${PG_CCACHE_FOUND} as C compiler launcher based on pg_config CC"
      )
      set(CMAKE_C_COMPILER_LAUNCHER ${PG_CCACHE_FOUND})
    endif()
    set(PG_CC ${PG_CC_LIST})
  else()
    # Single-word CC.
    set(PG_CC ${PG_CC_FIRST})
  endif()

  find_program(PG_CC_FOUND ${PG_CC})
  if(PG_CC_FOUND)
    message(STATUS "Using ${PG_CC_FOUND} as CC based on pg_config CC")
    set(CMAKE_C_COMPILER ${PG_CC_FOUND})
  endif()
endif()

# Set project name, version, and language. Language needs to be set for compiler
# checks
project(
  timescaledb
  VERSION ${VERSION}
  LANGUAGES C)

if(NOT CMAKE_BUILD_TYPE)
  # Default to Release builds
  set(CMAKE_BUILD_TYPE
      Release
      CACHE
        STRING
        "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel"
        FORCE)
endif()

set(SUPPORTED_BUILD_TYPES Debug Release RelWithDebInfo MinSizeRel)
if(NOT CMAKE_BUILD_TYPE IN_LIST SUPPORTED_BUILD_TYPES)
  message(
    FATAL_ERROR "Bad CMAKE_BUILD_TYPE. Expected one of ${SUPPORTED_BUILD_TYPES}"
  )
endif()

message(
  STATUS
    "TimescaleDB version ${PROJECT_VERSION_MOD}. Can be updated from version ${PREVIOUS_VERSION}"
)
message(STATUS "Build type is ${CMAKE_BUILD_TYPE}")

set(PROJECT_INSTALL_METHOD
    source
    CACHE STRING "Specify what install platform this binary
is built for")
message(STATUS "Install method is '${PROJECT_INSTALL_METHOD}'")

# Build compilation database by default
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Code coverage is optional and OFF by default
option(CODECOVERAGE "Enable code coverage for the build" OFF)
option(EXPERIMENTAL "Skip postgres version compatibility check" OFF)

# Generate downgrade script
option(GENERATE_DOWNGRADE_SCRIPT
       "Generate downgrade script. Defaults to not generate a downgrade script."
       OFF)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  # CMAKE_BUILD_TYPE is set at CMake configuration type. But usage of
  # CMAKE_C_FLAGS_DEBUG is determined at build time by running cmake --build .
  # --config Debug (at least on Windows). Therefore, we only set these flags if
  # the configuration-time CMAKE_BUILD_TYPE is set to Debug. Then Debug enabled
  # builds will only happen on Windows if both the configuration- and build-time
  # settings are Debug.
  set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DDEBUG=1 -DTS_DEBUG=1")
endif(CMAKE_BUILD_TYPE MATCHES Debug)

set(SUPPORTED_COMPILERS "GNU" "Clang" "AppleClang" "MSVC")

# Check for a supported compiler
if(NOT CMAKE_C_COMPILER_ID IN_LIST SUPPORTED_COMPILERS)
  message(
    FATAL_ERROR
      "Unsupported compiler ${CMAKE_C_COMPILER_ID}. Supported compilers are: ${SUPPORTED_COMPILERS}"
  )
endif()

# Option to treat warnings as errors when compiling (default on for debug
# builds, off for all other build types)
if(CMAKE_BUILD_TYPE STREQUAL Debug)
  option(WARNINGS_AS_ERRORS "Make compiler warnings into errors (default ON)"
         ON)
else()
  option(WARNINGS_AS_ERRORS "Make compiler warnings into errors (default ON)"
         OFF)
endif()

if(WARNINGS_AS_ERRORS)
  if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    add_compile_options(-Werror)
  elseif(CMAKE_C_COMPILER_ID MATCHES "MSVC")
    add_compile_options(/WX)
  endif()
endif(WARNINGS_AS_ERRORS)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|AppleClang|Clang")
  # These two flags generate too many errors currently, but we probably want
  # these optimizations enabled.
  #
  # -fdelete-null-pointer-checks -Wnull-dereference

  # This flag avoid some subtle bugs related to standard conversions, but
  # currently does not compile because we are using too many implicit
  # conversions that potentially lose precision.
  #
  # -Wconversions

  # These flags are supported on all compilers.
  add_compile_options(
    -Wempty-body
    -Wvla
    -Wall
    -Wextra
    # The SQL function arguments macro PG_FUNCTION_ARGS often inroduces unused
    # arguments.
    -Wno-unused-parameter
    -Wundef
    -Wmissing-prototypes
    -Wpointer-arith
    -Werror=vla
    -Wendif-labels
    -fno-strict-aliasing
    -fno-omit-frame-pointer)

  # These flags are just supported on some of the compilers, so we check them
  # before adding them.
  check_c_compiler_flag(-Wno-unused-command-line-argument
                        CC_SUPPORTS_NO_UNUSED_CLI_ARG)
  if(CC_SUPPORTS_NO_UNUSED_CLI_ARG)
    add_compile_options(-Wno-unused-command-line-argument)
  endif()

  check_c_compiler_flag(-Wno-format-truncation CC_SUPPORTS_NO_FORMAT_TRUNCATION)
  if(CC_SUPPORTS_NO_FORMAT_TRUNCATION)
    add_compile_options(-Wno-format-truncation)
  else()
    message(STATUS "Compiler does not support -Wno-format-truncation")
  endif()

  check_c_compiler_flag(-Wstringop-truncation CC_STRINGOP_TRUNCATION)
  if(CC_STRINGOP_TRUNCATION)
    add_compile_options(-Wno-stringop-truncation)
  else()
    message(STATUS "Compiler does not support -Wno-stringop-truncation")
  endif()

  check_c_compiler_flag(-Wimplicit-fallthrough CC_SUPPORTS_IMPLICIT_FALLTHROUGH)
  if(CC_SUPPORTS_IMPLICIT_FALLTHROUGH)
    add_compile_options(-Wimplicit-fallthrough)
  else()
    message(STATUS "Compiler does not support -Wimplicit-fallthrough")
  endif()

  check_c_compiler_flag(-Wnewline-eof CC_SUPPORTS_NEWLINE_EOF)
  if(CC_SUPPORTS_NEWLINE_EOF)
    add_compile_options(-Wnewline-eof)
  endif()

  # strict overflow check produces false positives on gcc < 8
  if(CMAKE_COMPILER_IS_GNUCC AND CMAKE_C_COMPILER_VERSION VERSION_LESS 8)
    add_compile_options(-Wno-strict-overflow)
  endif()

  # -Wclobbered produces false positives on gcc < 9
  if(CMAKE_COMPILER_IS_GNUCC AND CMAKE_C_COMPILER_VERSION VERSION_LESS 9)
    add_compile_options(-Wno-clobbered)
  endif()

  if(CMAKE_COMPILER_IS_GNUCC)
    add_compile_options(
      # Seems to be broken in GCC 11 with designated initializers.
      -Wno-missing-field-initializers)
  endif()

  # On UNIX, the compiler needs to support -fvisibility=hidden to hide symbols
  # by default
  check_c_compiler_flag(-fvisibility=hidden CC_SUPPORTS_VISIBILITY_HIDDEN)

  if(NOT CC_SUPPORTS_VISIBILITY_HIDDEN)
    message(
      FATAL_ERROR
        "The compiler ${CMAKE_C_COMPILER_ID} does not support -fvisibility=hidden"
    )
  endif(NOT CC_SUPPORTS_VISIBILITY_HIDDEN)
endif()

# On Windows, default to only include Release builds so MSBuild.exe 'just works'
if(WIN32 AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_CONFIGURATION_TYPES
      Release
      CACHE
        STRING
        "Semicolon separated list of supported configuration types, only supports Debug, Release, MinSizeRel, and RelWithDebInfo, anything else will be ignored."
        FORCE)
endif()

message(STATUS "Using compiler ${CMAKE_C_COMPILER_ID}")

if(ENABLE_OPTIMIZER_DEBUG)
  message(
    STATUS
      "Enabling OPTIMIZER_DEBUG. Make sure that ${PG_SOURCE_DIR} is installed and built with OPTIMIZER_DEBUG."
  )
  add_definitions(-DOPTIMIZER_DEBUG)
endif()

find_package(Git)

# Check PostgreSQL version
execute_process(
  COMMAND ${PG_CONFIG} --version
  OUTPUT_VARIABLE PG_VERSION_STRING
  OUTPUT_STRIP_TRAILING_WHITESPACE)

if(NOT ${PG_VERSION_STRING} MATCHES
   "^PostgreSQL[ ]+([0-9]+)(\\.([0-9]+)|beta|devel|rc[0-9]+)")
  message(FATAL_ERROR "Could not parse PostgreSQL version ${PG_VERSION_STRING}")
endif()

set(PG_VERSION_MAJOR ${CMAKE_MATCH_1})
if(${CMAKE_MATCH_COUNT} GREATER "2")
  set(PG_VERSION_MINOR ${CMAKE_MATCH_3})
else()
  set(PG_VERSION_MINOR 0)
endif()
set(PG_VERSION "${PG_VERSION_MAJOR}.${PG_VERSION_MINOR}")

message(
  STATUS
    "Compiling against PostgreSQL version ${PG_VERSION} using pg_config '${PG_CONFIG}'"
)

# Ensure that PostgreSQL version is supported and consistent with src/compat.h
# version check
if((${PG_VERSION_MAJOR} LESS "15")
   OR (${PG_VERSION_MAJOR} GREATER "17")
   AND NOT (${EXPERIMENTAL}))
  message(FATAL_ERROR "TimescaleDB only supports PostgreSQL 15, 16 and 17")
endif()

# Get PostgreSQL configuration from pg_config
get_pg_config(PG_INCLUDEDIR --includedir)
get_pg_config(PG_INCLUDEDIR_SERVER --includedir-server)
get_pg_config(PG_LIBDIR --libdir)
get_pg_config(PG_PKGLIBDIR --pkglibdir)
get_pg_config(PG_SHAREDIR --sharedir)
get_pg_config(PG_BINDIR --bindir)
get_pg_config(PG_CFLAGS --cflags)
get_pg_config(PG_CFLAGS_SL --cflags_sl)
get_pg_config(PG_CPPFLAGS --cppflags)
get_pg_config(PG_LDFLAGS --ldflags)
get_pg_config(PG_LIBS --libs)

separate_arguments(PG_CFLAGS)
foreach(option ${PG_CFLAGS})
  if(NOT ${option} MATCHES ^-W)
    set(filtered "${filtered} ${option}")
  endif()
endforeach()
set(PG_CFLAGS "${filtered} ${PG_CFLAGS_SL}")

find_path(
  PG_SOURCE_DIR src/include/pg_config.h.in
  HINTS $ENV{HOME} $ENV{HOME}/projects $ENV{HOME}/Projects
        $ENV{HOME}/development $ENV{HOME}/Development $ENV{HOME}/workspace
  PATH_SUFFIXES postgres postgresql pgsql
  DOC "The path to the PostgreSQL source tree")

option(PG_SOURCE_INCLUDES "Add PG source to include directories" OFF)

if(PG_SOURCE_DIR)
  message(STATUS "Found PostgreSQL source in ${PG_SOURCE_DIR}")
  if(PG_SOURCE_INCLUDES)
    # Add the PostgreSQL source dir include directories to the build system
    # includes BEFORE the installed PG include files. This will allow the LSP
    # (e.g., clangd) to navigate to the PostgreSQL source instead of the install
    # path directory that only has the headers.
    include_directories(BEFORE SYSTEM ${PG_SOURCE_DIR}/src/include)
  endif(PG_SOURCE_INCLUDES)
endif(PG_SOURCE_DIR)

set(EXT_CONTROL_FILE ${PROJECT_NAME}.control)
configure_file(${EXT_CONTROL_FILE}.in ${EXT_CONTROL_FILE})

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${EXT_CONTROL_FILE}
        DESTINATION "${PG_SHAREDIR}/extension")

find_program(
  CLANG_FORMAT
  NAMES clang-format-14 clang-format
  PATHS /usr/bin /usr/local/bin /usr/local/opt/ /usr/local/opt/llvm/bin /opt/bin
  DOC "The path to clang-format")

if(CLANG_FORMAT)
  execute_process(
    COMMAND ${CLANG_FORMAT} --version
    OUTPUT_VARIABLE CLANG_FORMAT_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  if(NOT ${CLANG_FORMAT_VERSION_OUTPUT} MATCHES
     "version[ ]+([0-9]+)\\.([0-9]+)(\\.([0-9]+))*")
    message(
      FATAL_ERROR
        "Could not parse clang-format version ${CLANG_FORMAT_VERSION_OUTPUT}")
  endif()

  if((${CMAKE_MATCH_1} LESS "14"))
    message(WARNING "clang-format version 14 or greater required")
    set(CLANG_FORMAT False)
  endif()
endif()

if(CLANG_FORMAT)
  message(STATUS "Using local clang-format")
  add_custom_target(
    clang-format COMMAND ${CMAKE_COMMAND} -E env CLANG_FORMAT=${CLANG_FORMAT}
                         ${PROJECT_SOURCE_DIR}/scripts/clang_format_all.sh)
endif()

find_program(
  CMAKE_FORMAT
  NAMES cmake-format
  PATHS /usr/bin /usr/local/bin /usr/local/opt/ /usr/local/opt/llvm/bin /opt/bin
  DOC "The path to cmake-format")

if(CMAKE_FORMAT)
  add_custom_target(
    cmake-format COMMAND ${CMAKE_COMMAND} -E env CMAKE_FORMAT=${CMAKE_FORMAT}
                         ${PROJECT_SOURCE_DIR}/scripts/cmake_format_all.sh)
endif()

find_program(
  PERLTIDY
  NAMES perltidy
  PATHS /bin /usr/bin /usr/local/bin /usr/local/opt/ /opt/bin
  DOC "The path to perltidy")

if(PERLTIDY)
  message(STATUS "Using perltidy ${PERLTIDY}")
  add_custom_target(
    perltidy
    COMMAND
      ${CMAKE_COMMAND} -E env PERLTIDY=${PERLTIDY}
      PERLTIDY_CONFIG="${PROJECT_SOURCE_DIR}/.perltidyrc"
      ${PROJECT_SOURCE_DIR}/scripts/perltidy_format_all.sh)
endif()

if(TARGET clang-format
   OR TARGET cmake-format
   OR TARGET perltidy)
  add_custom_target(format)
  if(TARGET clang-format)
    add_dependencies(format clang-format)
  endif()
  if(TARGET cmake-format)
    add_dependencies(format cmake-format)
  endif()
  if(TARGET perltidy)
    add_dependencies(format perltidy)
  endif()
endif()

if(REGRESS_CHECKS)
  find_program(PG_REGRESS pg_regress
               HINTS "${PG_BINDIR}" "${PG_PKGLIBDIR}/pgxs/src/test/regress/")

  if(NOT PG_REGRESS)
    message(STATUS "Regress checks disabled: program 'pg_regress' not found")
  endif()

  find_program(
    PG_ISOLATION_REGRESS
    NAMES pg_isolation_regress
    HINTS ${PG_BINDIR} ${PG_PKGLIBDIR}/pgxs/src/test/isolation
          ${PG_SOURCE_DIR}/src/test/isolation ${BINDIR})

  if(NOT PG_ISOLATION_REGRESS)
    message(
      STATUS
        "Isolation regress checks disabled: 'pg_isolation_regress' not found")
  endif()
else()
  message(STATUS "Regress checks and isolation checks disabled")
endif()

# Linter support via clang-tidy. Enabled when using clang as compiler
option(LINTER "Enable linter support using clang-tidy" OFF)
set(CLANG_TIDY_EXTRA_OPTS
    ""
    CACHE STRING "Additional options for clang-tidy")

if(LINTER)
  find_program(
    CLANG_TIDY clang-tidy
    PATHS /usr/bin /usr/local/bin /usr/local/opt/ /usr/local/opt/llvm/bin
          /opt/bin
    DOC "The path to the clang-tidy linter" REQUIRED)

  message(STATUS "Using clang-tidy ${CLANG_TIDY}")
  execute_process(COMMAND ${CLANG_TIDY} --version)
  string(
    CONCAT
      CMAKE_C_CLANG_TIDY
      "${CLANG_TIDY}"
      ";--checks=clang-diagnostic-*,clang-analyzer-*"
      ",-clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling"
      ",-clang-analyzer-deadcode.DeadStores"
      ",bugprone-*"
      ",-bugprone-branch-clone"
      ",-bugprone-easily-swappable-parameters"
      ",-bugprone-implicit-widening-of-multiplication-result"
      ",-bugprone-narrowing-conversions"
      ",-bugprone-reserved-identifier"
      ",-bugprone-suspicious-include"
      ",readability-*"
      ",-readability-avoid-const-params-in-decls"
      ",-readability-braces-around-statements"
      ",-readability-else-after-return"
      ",-readability-function-cognitive-complexity"
      ",-readability-function-size"
      ",-readability-identifier-length"
      ",-readability-isolate-declaration"
      ",-readability-magic-numbers"
      ",-readability-non-const-parameter"
      "${CLANG_TIDY_EXTRA_OPTS}")
  if(WARNINGS_AS_ERRORS)
    set(CMAKE_C_CLANG_TIDY "${CMAKE_C_CLANG_TIDY};--warnings-as-errors=*")
  else()
    set(CMAKE_C_CLANG_TIDY "${CMAKE_C_CLANG_TIDY};--quiet")
  endif(WARNINGS_AS_ERRORS)
endif(LINTER)

if(NOT EXISTS ${PG_INCLUDEDIR}/pg_config.h)
  message(
    FATAL_ERROR
      "Could not find pg_config.h in ${PG_INCLUDEDIR}. "
      "Make sure PG_PATH points to a valid PostgreSQL installation that includes development headers."
  )
endif()

file(READ ${PG_INCLUDEDIR}/pg_config.h PG_CONFIG_H)
string(REGEX MATCH "#define USE_ASSERT_CHECKING 1" PG_USE_ASSERT_CHECKING
             ${PG_CONFIG_H})

if(PG_USE_ASSERT_CHECKING AND NOT ASSERTIONS)
  message(
    STATUS
      "Assertion checks are OFF although enabled in PostgreSQL build (pg_config.h). "
      "The PostgreSQL setting for assertions will take precedence.")
elseif(ASSERTIONS)
  message(STATUS "Assertion checks are ON")
  add_compile_definitions(USE_ASSERT_CHECKING=1)
elseif(CMAKE_BUILD_TYPE MATCHES Debug)
  message(
    "Assertion checks are OFF in Debug build. Set -DASSERTIONS=ON to enable assertions."
  )
else()
  message(STATUS "Assertion checks are OFF")
endif()

# Check if PostgreSQL has OpenSSL enabled by inspecting pg_config.h. Right now,
# a Postgres header will redefine an OpenSSL function if Postgres is not
# installed --with-openssl, so in order for TimescaleDB to compile correctly
# with OpenSSL, Postgres must also have OpenSSL enabled.
check_symbol_exists(USE_OPENSSL ${PG_INCLUDEDIR}/pg_config.h PG_USE_OPENSSL)

if(USE_OPENSSL AND (NOT PG_USE_OPENSSL))
  message(
    FATAL_ERROR
      "PostgreSQL was built without OpenSSL support, which TimescaleDB needs for full compatibility. Please rebuild PostgreSQL using `--with-openssl` or if you want to continue without OpenSSL, re-run bootstrap with `-DUSE_OPENSSL=0`"
  )
endif(USE_OPENSSL AND (NOT PG_USE_OPENSSL))

# While we dont link directly against OpenSSL on non-Windows, doing this on
# Windows causes linker errors. So on Windows we link directly against the
# OpenSSL libraries.
if(USE_OPENSSL AND MSVC)
  # Try to find a local OpenSSL installation
  find_package(OpenSSL)

  if(NOT OPENSSL_FOUND)
    message(
      FATAL_ERROR
        "TimescaleDB requires OpenSSL but it wasn't found. If you want to continue without OpenSSL, re-run bootstrap with `-DUSE_OPENSSL=0`"
    )
  endif(NOT OPENSSL_FOUND)

  if(${OPENSSL_VERSION} VERSION_LESS "1.0")
    message(FATAL_ERROR "TimescaleDB requires OpenSSL version 1.0 or greater")
  endif()

  set(_libraries)
  foreach(_path ${OPENSSL_LIBRARIES})
    if(EXISTS "${_path}")
      list(APPEND _libraries ${_path})
    else()
      # check if a release version of the libraries are available
      if(CMAKE_BUILD_TYPE STREQUAL "Debug" AND MSVC)
        get_filename_component(_dir ${_path} DIRECTORY)
        get_filename_component(_name ${_path} NAME_WE)
        string(REGEX REPLACE "[Dd]$" "" _fixed ${_name})
        get_filename_component(_ext ${_path} EXT)
        set(_new_path "${_dir}/${_fixed}${_ext}")
        if(EXISTS "${_new_path}")
          list(APPEND _libraries ${_new_path})
        endif()
      endif()
    endif()
  endforeach()
  set(OPENSSL_LIBRARIES ${_libraries})

  foreach(_path ${OPENSSL_LIBRARIES})
    message(STATUS "OpenSSL libraries: ${_path}")
  endforeach()
  message(STATUS "Using OpenSSL version ${OPENSSL_VERSION}")
endif(USE_OPENSSL AND MSVC)

if(CODECOVERAGE)
  message(STATUS "Code coverage is enabled.")
  # Note that --coverage is synonym for the necessary compiler and linker flags
  # for the given compiler.  For example, with GCC, --coverage translates to
  # -fprofile-arcs -ftest-coverage when compiling and -lgcov when linking
  add_compile_options(--coverage -O0)
  add_link_options(--coverage)
endif(CODECOVERAGE)

# TAP test support
option(TAP_CHECKS "Enable TAP test support" ON)

if(TAP_CHECKS)
  find_package(Perl 5.8)

  if(PERL_FOUND)
    get_filename_component(PERL_BIN_PATH ${PERL_EXECUTABLE} DIRECTORY)

    find_program(
      PROVE prove
      HINTS ${PERL_BIN_PATH}
      PATHS "/usr/bin")

    if(NOT PROVE)
      message(STATUS "Not running TAP tests: 'prove' binary not found.")
      set(TAP_CHECKS OFF)
    endif()

    # Check for the IPC::Run module
    execute_process(
      COMMAND ${PERL_EXECUTABLE} -MIPC::Run -e ""
      ERROR_QUIET
      RESULT_VARIABLE PERL_MODULE_STATUS)

    if(PERL_MODULE_STATUS)
      message(STATUS "Not running TAP tests: IPC::Run Perl module not found.")
      set(TAP_CHECKS OFF)
    endif()
  else()
    message(STATUS "Not running TAP tests: Perl not found.")
    set(TAP_CHECKS OFF)
  endif()
endif()

if(UNIX)
  add_subdirectory(scripts)
endif(UNIX)

add_subdirectory(sql)
add_subdirectory(test)
add_subdirectory(src)

if(NOT APACHE_ONLY)
  add_subdirectory(tsl)
endif()

add_custom_target(licensecheck
                  COMMAND ${PROJECT_SOURCE_DIR}/scripts/check_license_all.sh)

# This needs to be the last subdirectory so that other targets are already
# defined
if(CODECOVERAGE)
  add_subdirectory(coverage)
endif()

if(IS_DIRECTORY ${PROJECT_SOURCE_DIR}/.git)
  configure_file(${PROJECT_SOURCE_DIR}/scripts/githooks/commit_msg.py
                 ${PROJECT_SOURCE_DIR}/.git/hooks/commit-msg COPYONLY)
endif()
