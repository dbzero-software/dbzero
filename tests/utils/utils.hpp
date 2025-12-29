// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <cstdint>

namespace db0::tests

{

    bool file_exists(const char *filename);

    void drop(const char *filename);

    std::vector<char> randomPage(std::size_t size);

    bool equal(const std::vector<char> &v1, const std::vector<char> &v2);

    std::string randomToken(int min_len, int max_len);

    template <typename random_iterator> bool equal(random_iterator begin, random_iterator end,
        const std::vector<char> &v2)
    {
        if ((end - begin) != (int)v2.size()) {
            return false;
        }
        unsigned int i = 0;
        for (; begin != end; ++begin, ++i) {
            if (*begin != v2[i]) {
                return false;
            }
        }
        return true;
    }
    
    // Load rows from a comma-separated values (CSV)
    std::vector<std::vector<std::uint32_t> > loadArray(const std::string &file_name);

}