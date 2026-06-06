// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/dram/DRAMSpace.hpp>

namespace db0

{

    class Diff_IO;
    class SparsePair;

    struct MetaSpace: public DRAMSpace
    {
        static Memspace create(std::size_t page_size, SparsePair &sparse_pair, Diff_IO &page_io);
    };

}
