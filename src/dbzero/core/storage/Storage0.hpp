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
    
    /**
     * The dev0 storage implementation
    */
    class Storage0: public BaseStorage
    {
    public:
        static StateNumType STATE_NULL;
        
        Storage0(std::size_t page_size = 4096);
        ~Storage0() = default;

        void read(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            FlagSet<AccessOptions> = { AccessOptions::read, AccessOptions::write }) const override;
        
        void write(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer) override;
            
        bool tryWriteDiffs(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            const std::vector<std::uint16_t> &diffs, unsigned int) override;

        StateNumType findMutation(std::uint64_t page_num, StateNumType state_num) const override;
        
        bool tryFindMutation(std::uint64_t page_num, StateNumType state_num, StateNumType &) const override;

        std::size_t getPageSize() const override;

        StateNumType getMaxStateNum() const override {
            return 1u;
        }
        
        bool flush(ProcessTimer * = nullptr) override {
            return false;
        }

        void close() override 
        {
        }
        
        std::uint64_t getLastUpdated() const;
                
    private:
        const std::size_t m_page_size;
    };

}