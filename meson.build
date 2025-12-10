project('DBZero', 'cpp', default_options :['buildtype=release', 'b_ndebug=if-release'])

if build_machine.system() == 'linux'
    add_project_arguments('-Wno-unused-result',
        '-Wno-unused-local-typedefs',
        '-Wno-deprecated-declarations',
        '-Wno-address-of-packed-member',
        '-Werror=switch',
        '-Werror=return-type',
        '-fPIC',
        '-no-pie',
        '-ftemplate-backtrace-limit=0',
        '-fno-omit-frame-pointer',
        '-ldl',
        '-lm',
        '-lpthread',
        '-fuse-ld=gold',
        language : 'cpp')
        add_project_arguments('-std=c++17', language : 'cpp')   
 else 
    add_project_arguments(
        '-ldl',
        '-lm',
        '-lpthread',
        language : 'cpp')
    add_project_arguments('-std:c++20', language : 'cpp')
 endif

enable_debug_exceptions = get_option('enable_debug_exceptions')

message('Enable debug exceptions: ' + enable_debug_exceptions.to_string())
if enable_debug_exceptions
  message('Enabling debug exceptions')
  add_project_arguments('-DENABLE_DEBUG_EXCEPTIONS=1', language: 'c')
else
  message('Disabling debug exceptions')
  add_project_arguments('-DENABLE_DEBUG_EXCEPTIONS=0', language: 'c')
endif

enable_sanitizers = get_option('enable_sanitizers')
if enable_sanitizers
    message('Enabling address and undefined behavior sanitizers')
    add_project_arguments('-fsanitize=address', language: 'cpp')
    add_project_link_arguments('-fsanitize=address', language: 'cpp')
else
    message('Disabling address and undefined behavior sanitizers')
endif

buildtype = get_option('buildtype')
build_suffix = ''
if buildtype.startswith('release')
    add_project_arguments('-O2',  language : 'cpp')
elif buildtype.startswith('debug')
    build_suffix='D'
endif

dbzero_deps = []
include_dirs = include_directories(['src/'])
include_dirs_test = include_directories(['src/','tests/'])
py3_inst = import('python').find_installation('python3', pure: false)
install_dir = py3_inst.get_install_dir() / 'dbzero' / 'libs'
deps = []

if build_machine.system() == 'linux'
    python_embed_dep = dependency('python3-embed', main:true, required: true)
endif
python_deps = py3_inst.dependency()


deps += python_deps
if build_machine.system() == 'linux'
    deps += python_embed_dep
endif

all_srcs = []
subdir('src/dbzero')
subdir('dbzero')

link_with = []

foreach dep : dbzero_deps
    link_with += dep
endforeach

link_tests_with = link_with

tests_sources = []


gtest_proj = subproject('gtest') 


gtest_dep = gtest_proj.get_variable('gtest_dep') 
gmock_dep = gtest_proj.get_variable('gmock_dep') 
gtest_dep = gtest_proj.get_variable('gtest_dep') 
gtest_dep = dependency('gtest', main : true, required : false) 
gmock_dep = dependency('gmock', main : true, required : false) 

dbzero_lib = static_library('pyzero', [all_srcs], include_directories:include_dirs, dependencies: [deps], install : true, install_dir: install_dir)

build_tests = get_option('build_tests')
if build_tests
    message('Building C++ tests')
    subdir('tests')
    e = executable('tests' + build_suffix + '.x', [all_srcs,tests_sources], dependencies: [deps, gtest_dep, gmock_dep], 
                   link_with: [dbzero_lib, link_with], include_directories:include_dirs_test, link_language : 'cpp',)
    test('gtest tests', e)
else
    message('Skipping C++ tests build')
endif

py3avlw = py3_inst.extension_module('dbzero',
    sources: ['src/dbzero/bindings/python/dbzero.cpp'], 
    dependencies : [deps, gtest_dep, gmock_dep],
    include_directories:include_dirs,
    link_with:[dbzero_lib, link_with],
    link_language : 'cpp',
    override_options: [
        'cpp_rtti=true',
    ],
    install: true,
    subdir: 'dbzero',
)

# License and notice files
license_files = [
    './LICENSE',
    './NOTICE',
    './THIRD_PARTY_LICENSES/GOOGLETEST_LICENSE',
    './THIRD_PARTY_LICENSES/BOOST_LICENSE_1_0',
]

py3_inst.install_sources(
  license_files,
  subdir: 'dbzero'
)
