#pragma once

#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <functional>
#include <unordered_map>

namespace db0

{

    class ProcessTimer;

    /**
     * Defines the file-oriented storage interface
    */
    class BaseStorage
    {
    public:
        BaseStorage(AccessType);
        virtual ~BaseStorage() = default;

        /**
         * Read data from a specific state into a user provided buffer
         * 
         * @param state_id the state number
         * @param address the starting address of the range to read
         * @param size the range size in bytes
         * @param buffer the buffer to hold the data
        */       
        virtual void read(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer,
            FlagSet<AccessOptions> = { AccessOptions::read, AccessOptions::write }) const = 0;
        
        /**
         * Write data from user providerd buffer into a specific state/range
         * 
         * @param state_id the state number
         * @param state_id the starting address of the range to write
         * @param size the range size in bytes
         * @param buffer the buffer holding data to be written
        */
        virtual void write(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer) = 0;

        /**
         * Look up sparse index to find the state number at which the given range was mutated
         * with respect to the given state number. This functionality is required for caching.
         * 
         * Exception will be raised if read access requested and some of the pages in the range does not exist
        */
        virtual std::uint64_t findMutation(std::uint64_t page_num, std::uint64_t state_num) const = 0;

        /**
         * Try finding mutation
         * @return true if found and mutation_id was set
        */
        virtual bool tryFindMutation(std::uint64_t page_num, std::uint64_t state_num, 
            std::uint64_t &mutation_id) const = 0;
        
        virtual std::size_t getPageSize() const = 0;

        /**
         * Get maximum used state number
        */
        virtual std::uint32_t getMaxStateNum() const = 0;

        virtual AccessType getAccessType() const;
        
        /**
         * Flush all in-memory changes to disk
         * @return true if any changes were flushed (false if there were no modifications to be flushed)
        */
        virtual bool flush(ProcessTimer * = nullptr) = 0;

        /**
         * Flush all changes to disk and close
        */
        virtual void close() = 0;

        // Collect storage statistics where applicable (default implementation is empty)
        virtual void getStats(std::function<void(const std::string &, std::uint64_t)>) const;
        
        virtual std::uint64_t refresh(std::function<void(std::uint64_t updated_page_num, std::uint64_t state_num)> f = {});

        virtual std::uint64_t getLastUpdated() const;
        
#ifndef NDEBUG
        // state number, file offset
        using DRAM_PageInfo = std::pair<std::uint64_t, std::uint64_t>;

        struct DRAM_CheckResult
        {
            std::uint64_t m_address;
            std::uint64_t m_page_num;
            std::uint64_t m_expected_page_num;
        };
        
        virtual void getDRAM_IOMap(std::unordered_map<std::uint64_t, DRAM_PageInfo> &) const;
        virtual void dramIOCheck(std::vector<DRAM_CheckResult> &) const;
#endif        
    protected:
        AccessType m_access_type;
    };
        
}