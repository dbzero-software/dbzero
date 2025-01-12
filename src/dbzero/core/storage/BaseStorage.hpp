#pragma once

#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/config.hpp>
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
        virtual void read(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            FlagSet<AccessOptions> = { AccessOptions::read, AccessOptions::write }) const = 0;
        
        /**
         * Write data from user providerd buffer into a specific state/range
         * 
         * @param state_id the state number
         * @param state_id the starting address of the range to write
         * @param size the range size in bytes
         * @param buffer the buffer holding data to be written
        */
        virtual void write(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer) = 0;
        
        /**
         * Write diff-data into a specific page
         * @param diffs interleved values of diff size / identical sequence size (ignored)
         * @param max_len - the maximum allowed diff-sequence length (when exceeded, the full-DP will be written)
         * NOTE: diff areas must be evaluated page-wise
         */
        virtual void writeDiffs(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            const std::vector<std::uint16_t> &diffs, unsigned int max_len = 32) = 0;
        
        /**
         * Look up sparse index to find the state number at which the given range was mutated
         * with respect to the given state number. This functionality is required for caching.
         * 
         * Exception will be raised if read access requested and some of the pages in the range does not exist
         * @return mutation state number
        */
        virtual StateNumType findMutation(std::uint64_t page_num, StateNumType state_num) const = 0;
        
        /**
         * Try finding mutation
         * @return true if found and mutation_id was set
        */
        virtual bool tryFindMutation(std::uint64_t page_num, StateNumType state_num, 
            StateNumType &mutation_id) const = 0;
        
        virtual std::size_t getPageSize() const = 0;

        /**
         * Get maximum used state number
        */
        virtual StateNumType getMaxStateNum() const = 0;

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

        // Try refreshing the underlying storage
        // @return true if there was any new mergeable contents retireved
        virtual bool beginRefresh();
        
        // Complete the refresh operation after successful invocation of beginRefresh
        // @return last modification timestamp
        virtual std::uint64_t completeRefresh(
            std::function<void(std::uint64_t updated_page_num, StateNumType state_num)> f = {});
        
        /**
         * Allowed in read-only mode only
         * Fetch the most recent changes from the underlying storage
         * @param f optional function to be notified on updated data pages (DP)
         * @return 0 if no changes were applied, last modified timestamp otherwise
        */    
        std::uint64_t refresh(std::function<void(std::uint64_t updated_page_num, StateNumType state_num)> f = {});
        
        virtual std::uint64_t getLastUpdated() const;
        
        // beginCommit / endCommit should be called to indicate the start and end of 
        // transaction data flushing. This might be relevant e.g. to determine if diff-writes should be allowed
        virtual void beginCommit();
        virtual void endCommit();
        
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