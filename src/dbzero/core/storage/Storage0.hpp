#pragma once

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <functional>
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
        static std::uint64_t STATE_NULL;
        
        Storage0(std::size_t page_size = 4096);
        ~Storage0() = default;

        void read(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer,
            FlagSet<AccessOptions> = { AccessOptions::read, AccessOptions::write }) const override;
        
        void write(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer) override;

        void writeDiffs(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer,
            const std::vector<std::uint16_t> &diffs, unsigned int) override;

        std::uint64_t findMutation(std::uint64_t page_num, std::uint64_t state_num) const override;
        
        bool tryFindMutation(std::uint64_t page_num, std::uint64_t state_num, std::uint64_t &) const override;

        std::size_t getPageSize() const override;

        std::uint32_t getMaxStateNum() const override {
            return 1;
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