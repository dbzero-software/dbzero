// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <functional>
#include <map>

namespace db0

{
    // MemoryStorage has interface almost identical to BDevStorage
    // and can be used for testing &debugging purposes (e.g. when used as data mirroring storage)
    class MemoryStorage
    {
    public:
        MemoryStorage(std::size_t page_size);

        void read(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer,
            FlagSet<AccessOptions> = { AccessOptions::read, AccessOptions::write }) const;
        
        void write(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer);

        bool tryFindMutation(std::uint64_t page_num, std::uint64_t state_num, 
            std::uint64_t &mutation_id) const;
        
    private:
        const std::size_t m_page_size;

        struct MemoryPage
        {
            std::vector<char> m_buffer;

            MemoryPage() = default;
            MemoryPage(std::size_t size);            

            MemoryPage &operator=(MemoryPage &&) = default;
        };

        // address / state number
        using PageKey = std::pair<std::uint64_t, std::uint64_t>;
        std::map<PageKey, MemoryPage> m_pages;
    };

}