// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <functional>
#include <dbzero/core/memory/config.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/utils/FlagSet.hpp>
#include "BaseStorage.hpp"

namespace db0

{

    // In-memory implementation of BaseStorage interface
    // the primary purpose is to be used in unit tests or debugging (e.g. as a mirror storage)
    // NOTE: MemBaseStorage does not maintain state history - all reads/writes are against the latest state
    class MemBaseStorage: public BaseStorage
    {
    public:
        MemBaseStorage(std::size_t page_size = 4096);

        void read(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            FlagSet<AccessOptions> = { AccessOptions::read, AccessOptions::write }) const override;
        
        void write(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer) override;

        void writeDiffs(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            const std::vector<std::uint16_t> &diffs, unsigned int);
        bool tryWriteDiffs(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            const std::vector<std::uint16_t> &diffs, unsigned int) override;
        
        StateNumType findMutation(std::uint64_t page_num, StateNumType state_num) const override;
        
        bool tryFindMutation(std::uint64_t page_num, StateNumType state_num, StateNumType &) const override;
        
        std::size_t getPageSize() const override;
        
        bool flush(ProcessTimer * = nullptr) override;
        void close() override;

        StateNumType getMaxStateNum() const override {
            return m_max_state_num;
        }
        
        // throws exception if the contents do not match
        void validateRead(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            FlagSet<AccessOptions> = { AccessOptions::read, AccessOptions::write }) const;
        
    private:
        mutable std::mutex m_mutex;
        const std::size_t m_page_size;
        StateNumType m_max_state_num = 0;
        mutable std::unordered_map<std::uint64_t, std::vector<std::byte> > m_data_pages;
        mutable std::vector<std::byte> m_dp_null;
        mutable std::vector<char> m_temp_buf;
        
        std::vector<std::byte> &getDataPage(std::uint64_t page_num, FlagSet<AccessOptions>) const;
    };
    
}