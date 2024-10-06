#pragma once

#include "SparseIndex.hpp"
#include "CFile.hpp"
#include "Storage0.hpp"
#include "BlockIOStream.hpp"
#include "PageIO.hpp"
#include <optional>
#include <cstdio>
#include <dbzero/core/memory/AccessOptions.hpp>
#include "BaseStorage.hpp"
#include "DRAM_IOStream.hpp"
#include "ChangeLogIOStream.hpp"

namespace db0

{
    
    struct [[gnu::packed]] o_prefix_config: public o_fixed<o_prefix_config>
    {
        // magic number for the .db0 file        
        static constexpr std::uint64_t DB0_MAGIC = 0x0DB0DB0DB0DB0DB0;

        std::uint64_t m_magic = DB0_MAGIC;
        std::uint32_t m_version = 1;
        // storage block size
        std::uint32_t m_block_size;
        // the prefix page size
        std::uint32_t m_page_size;
        std::uint64_t m_dram_io_offset = 0;
        std::uint32_t m_dram_page_size;
        std::uint64_t m_wal_offset = 0;
        std::uint64_t m_dram_changelog_io_offset = 0;
        // data pages change log
        std::uint64_t m_dp_changelog_io_offset = 0;

        o_prefix_config(std::uint32_t block_size, std::uint32_t page_size, std::uint32_t dram_page_size);
    };

    /**
     * Block-device based storage implementation
     * the SparseIndex is held in-memory, modifications are written to WAL and serialized to disk on close
    */
    class BDevStorage: public BaseStorage
    {
    public:
        static constexpr std::uint32_t DEFAULT_PAGE_SIZE = 4096;

        /**
         * Opens BDevStorage over an existing file
        */
        BDevStorage(const std::string &file_name, AccessType = AccessType::READ_WRITE);
        ~BDevStorage();

        /**
         * Create a new .db0 file
        */
        static void create(const std::string &file_name, std::optional<std::size_t> page_size = {},
            std::uint32_t dram_page_size_hint = 16 * 1024 - 256);
        
        void read(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer,
            FlagSet<AccessOptions> = { AccessOptions::read, AccessOptions::write }) const override;
        
        void write(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer) override;
        
        std::uint64_t findMutation(std::uint64_t page_num, std::uint64_t state_num) const override;
        
        bool tryFindMutation(std::uint64_t page_num, std::uint64_t state_num, std::uint64_t &mutation_id) const override;

        /**
         * Allowed in read-only mode only
         * Fetch the most recent changes from the underlying storage
         * @param f optional function to be notified on updated data pages (DP)
         * @return 0 if no changes were applied, last modified timestamp otherwise
        */
        std::uint64_t refresh(std::function<void(std::uint64_t updated_page_num, std::uint64_t state_num)> f = {});
        
        bool flush() override;

        void close() override;
        
        std::size_t getPageSize() const override;

        std::uint32_t getMaxStateNum() const override;
        
        void getStats(std::function<void(const std::string &, std::uint64_t)>) const override;

        const DRAM_IOStream &getDramIO() const {
            return m_dram_io;
        }
        
        bool empty() const;
        
        /**
         * Get last update timestamp
        */
        std::uint64_t getLastUpdated() const;
        
    private:
        // all prefix configuration must fit into this block
        static constexpr unsigned int CONFIG_BLOCK_SIZE = 4096;
        CFile m_file;
        const o_prefix_config m_config;

        // DRAM-changelog stream stores the sequence of updates to DRAM pages
        // DRAM-changelog must be initialized before DRAM_IOStream
        ChangeLogIOStream m_dram_changelog_io;
        // data-page change log, each chunk corresponds to a separate data transaction
        // first element from each chunk represents the state number
        ChangeLogIOStream m_dp_changelog_io;
        // memory-mapped file I/O
        DRAM_IOStream m_dram_io;
        // DRAM-backed sparse index tree
        SparseIndex m_sparse_index;
        BlockIOStream m_wal_io;
        // the last / current physical pages block
        PageIO m_page_io;
        // empty flag maintained in read-only mode
        bool m_empty = false;
        // page numbers modified in current transaction (to prevent duplicates in SparseIndex)
        std::unordered_set<std::uint64_t> m_updated_pages;

        static DRAM_IOStream init(DRAM_IOStream &&, ChangeLogIOStream &);

        static ChangeLogIOStream init(ChangeLogIOStream &&);
        
        /**
         * Calculates the total number of blocks stored in this file
         * note that the last block may be partially written
        */
        std::uint64_t getBlockCount(std::uint64_t file_size) const;

        BlockIOStream getBlockIOStream(std::uint64_t first_block_pos, AccessType);

        DRAM_IOStream getDRAMIOStream(std::uint64_t first_block_pos, std::uint32_t dram_page_size, AccessType);

        ChangeLogIOStream getChangeLogIOStream(std::uint64_t first_block_pos, AccessType);

        PageIO getPageIO(std::uint64_t next_page_hint, AccessType);
        
        o_prefix_config readConfig() const;
        
        /**
         * Get the first available address (i.e. end of the file)
         */
        std::uint64_t tail() const;

        std::function<std::uint64_t()> getTailFunction() const;

        std::function<std::uint64_t()> getBlockIOTailFunction() const;
        
        // non-virtual version of tryFindMutation
        bool tryFindMutationImpl(std::uint64_t page_num, std::uint64_t state_num,
            std::uint64_t &mutation_id) const;
    };
    
}