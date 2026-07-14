// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "CFile.hpp"
#include "ExtSpace.hpp"
#include <functional>

namespace db0

{
    
    /**
     * Page_IO organizes file's data into blocks of pages
     * pages are identified by absolute numbers which enables fast address calculation
    */
    class Page_IO
    {
    public: 
        // Read/write Page_IO
        // @param header_size the fixed file-level offset to be taken into account when calculating page address
        // @param file the underlying file object
        // @param page_size the size of a single page in bytes
        // @param block_size size of a unit block of pages to be pre-allocated by the stream
        // @param address of the currently active block (for append)
        // @param page_count the number of pages already stored in the current block
        // @param step_size number of blocks per single indivisible step (for REL_Index mapping)
        // @param tail_function a function returning current (unflushed) size of the file (Page IO excluded)
        // @param block_num the block number within the step if it is known
        Page_IO(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size, std::uint64_t address,
            std::uint32_t page_count, std::uint32_t step_size, std::function<std::uint64_t()> tail_function,
            std::optional<std::uint32_t> block_num = {});
        
        // Read-only Page_IO
        // NOTE: step size is irrelevant in read-only mode, will be initialized to 0
        Page_IO(std::size_t header_size, CFile &file, std::uint32_t page_size);
        
        ~Page_IO();
        
        // Appends a new page to the stream
        // @return ever increasing page number (aka storage page number) + is_first_page (of the current step) optional flag
        // NOTE: first block (on first page) must be registered with REL_Index if it's maintained
        std::uint64_t append(const void *buffer, bool *is_first_page = nullptr);
        
        // Appends one or more pages to the stream
        // @return first appended page number (aka storage page number)
        std::uint64_t append(const void *buffer, std::uint64_t page_count);
        
        void read(std::uint64_t page_num, void *buffer) const;
        
        // Read multiple consecutive pages
        void read(std::uint64_t page_num, void *buffer, std::uint32_t page_count) const;
        
        /**
         * Overwrite existing page
        */
        void write(std::uint64_t page_num, void *buffer);
        
        std::uint64_t tail() const;
        
        std::uint32_t getPageSize() const;
        
        // Get the page number which is > all pages currently stored
        // This value can act as a "sentinel" for end-of-stream (at the moment of the call)
        // NOTE: the member is only available in read/write mode
        std::uint64_t getEndPageNum(bool *is_first_page = nullptr) const;
        
        // Get the next page number to be assigned by the "append" method (first)
        // and the number of consecutive pages available in the current block
        std::pair<std::uint64_t, std::uint32_t> getNextPageNum(bool *is_first_page = nullptr);
        
        // Get the number of pages remaining in the current step (for append)
        std::uint32_t getCurrentStepRemainingPages() const;
        
        // @return step size in number of blocks
        std::size_t getStepSize() const {
            return m_step_size;
        }
        
        // @return block size in bytes
        std::size_t getBlockSize() const {
            return m_block_size;
        }

        class StepIterator
        {
        public:
            StepIterator(const ExtSpace &);
            
            bool operator!() const;

            bool is_end() const;
            // @retrun storage page number of the current step
            std::uint64_t operator*() const;

            StepIterator &operator++();
            std::optional<std::size_t> tryGetStepPages() const;

        private:
            std::optional<std::uint64_t> m_current_page_num;
            std::optional<std::uint64_t> m_current_rel_page_num;
            // next step's iterator (may be end)
            std::unique_ptr<typename ExtSpace::const_iterator> m_next_it;
        };

        // Reads entire blocks / steps sequentially
        // until reaching the end_page_num or end-of-stream whichever comes first
        class Reader
        {
        public:
            // @param ext_space optional ExtSpace for locating data "steps" and
            // for translating into relative page numbers
            Reader(const Page_IO &page_io, const ExtSpace &ext_space,
                std::optional<std::uint64_t> end_page_num = {});
            
            // Reads up to max_bytes of data
            // @param start_page_num the first storage page number read in this call
            // @return number of pages read, 0 if end-of-stream reached
            std::uint32_t next(std::vector<std::byte> &, std::uint64_t &start_page_num,
                std::size_t max_bytes = 64u << 20);
            
        private:
            const Page_IO &m_page_io;
            StepIterator m_step_it;
            std::uint64_t m_end_page_num;
            // current storage page number
            std::uint64_t m_current_page_num = 0;
            
            // Calculate end page number from actual file size
            std::uint64_t endPageNum() const;
            // First storage page number to read from
            std::uint64_t getFirstPageNum(const ExtSpace &) const;
        };
        
    protected:
        const std::size_t m_header_size;
        const std::uint32_t m_page_size;
        // block size in bytes (i.e. capacity expressed in bytes)
        const std::uint32_t m_block_size = 0;
        // maximum number of pages in block
        const std::uint32_t m_block_capacity = 0;
        // must be >= 1 in read/write mode
        const std::uint32_t m_step_size = 0;
        
    private:
        CFile &m_file;
        // begin address of the current block
        std::uint64_t m_address = 0;
        // the number of pages already stored in current block
        std::uint32_t m_page_count = 0;
        // number of the 1st page in current block
        std::uint64_t m_first_page_num = 0;
        std::function<std::uint64_t()> m_tail_function;
        const AccessType m_access_type;
        // block number within the step
        std::optional<std::uint32_t> m_block_num;

        std::uint64_t getPageNum(std::uint64_t address) const;
        void allocateNextBlock();
        
        // Update the stream's current location within the current step
        // @param page_count number of pages to move by within the current step
        void moveBy(std::uint32_t page_count);
    };

}