// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
    
#include <vector>
#include <cstdint>
#include <optional>

namespace db0

{

    class SparseBoolMatrix
    {
    public:
        // @param dim2_limit - optional limit for 2nd dimension (bounds not validated)
        SparseBoolMatrix(std::optional<std::uint32_t> dim2_limit = {}, unsigned int sort_threshold = 8);

        // set or reset item at specified position
        void set(std::pair<std::uint32_t, std::uint32_t>, bool value);
        // @return false if item not set / not found
        bool get(std::pair<std::uint32_t, std::uint32_t>) const;

        void clear();
        
    private:
        const std::optional<std::uint32_t> m_dim2_limit;
        const unsigned int m_sort_threshold = 8;
        std::vector<bool> m_dim1;

        struct Column
        {
            bool m_resize = false;
            std::uint32_t m_key = 0;
            std::vector<bool> m_data;

            Column(std::optional<std::uint32_t> dim2_limit, std::uint32_t key);

            void set(std::uint32_t at, bool value);
            bool get(std::uint32_t at) const;
        };

        // sparse, typically short list of columns, sorted by m_key (after reaching sort threshold)
        std::vector<Column> m_dim2;
        
        Column &getColumn(std::uint32_t key);
        const Column *findColumn(std::uint32_t key) const;
    };
    
}
