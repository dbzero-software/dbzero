#!/bin/bash
export G_SLICE=always-malloc
export G_DEBUG=gc-friendly
valgrind -v --tool=memcheck --leak-check=no --num-callers=40 --log-file=valgrind.log ./build/debug/testsD.x --gtest_filter="*testVBVectorItemIterationWithForLoop*"
