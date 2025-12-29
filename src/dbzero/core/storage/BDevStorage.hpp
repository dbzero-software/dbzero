// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/FixedVersioned.hpp>
#include "SparseIndex.hpp"
#include "SparsePair.hpp"
#include "CFile.hpp"
#include "Storage0.hpp"
#include "BlockIOStream.hpp"
#include "Page_IO.hpp"
#include "Diff_IO.hpp"
#include <optional>
#include <cstdio>
#include <dbzero/core/memory/AccessOptions.hpp>
#include "BaseStorage.hpp"
#include "DRAM_IOStream.hpp"
#include "ChangeLogIOStream.hpp"
#include "MetaIOStream.hpp"
#include <dbzero/workspace/LockFlags.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <shared_mutex>
#include "ExtSpace.hpp"
#include "MemBaseStorage.hpp"

namespace db0

{
    
    class REL_Index;

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_prefix_config: public o_fixed_versioned<o_prefix_config>
    {
        // magic number for the .db0 file        
        static constexpr std::uint64_t DB0_MAGIC = 0x0DB0DB0DB0DB0DB0;

        std::uint64_t m_magic = DB0_MAGIC;
        std::uint32_t m_version = 1;  
        std::uint32_t m_block_size;
        // the prefix page size
        std::uint32_t m_page_size;
        std::uint64_t m_dram_io_offset = 0;
        std::uint32_t m_dram_page_size;
        std::uint64_t m_dram_changelog_io_offset = 0;
        // data pages change log
        std::uint64_t m_dp_changelog_io_offset = 0;
        std::uint64_t m_meta_io_offset = 0;
        // the number of concsecutive blocks created by the PageIO
        // a a single indivisible "step".
        // This value (entire step) corresponts to a single entry in the REL_Index (if it's used)
        std::uint32_t m_page_io_step_size;
        std::uint64_t m_ext_dram_io_offset = 0;
        std::uint32_t m_ext_dram_page_size = 0;
        std::uint64_t m_ext_dram_changelog_io_offset = 0;
        // reserved for future use (0-filled)
        std::array<std::uint64_t, 16> m_reserved;
        
        o_prefix_config(std::uint32_t block_size, std::uint32_t page_size, std::uint32_t dram_page_size,
            std::uint32_t page_io_step_size);
    };
DB0_PACKED_END
    
    /**
     * Block-device based storage implementation
     * the SparseIndex is held in-memory, modifications are written to WAL and serialized to disk on close
    */
    class BDevStorage: public BaseStorage
    {
    public:
        static constexpr std::uint32_t DEFAULT_PAGE_SIZE = 4096;
        static constexpr std::size_t DEFAULT_META_IO_STEP_SIZE = 16 << 20;
        using DRAM_ChangeLogStreamT = ChangeLogIOStream<DRAM_ChangeLogT>;
        using DP_ChangeLogStreamT = ChangeLogIOStream<DP_ChangeLogT>;
        
        /**
         * Opens BDevStorage over an existing file
         * @param meta_io_step_size - the size of the step in the MetaIOStream (16MB by default)
        */
        BDevStorage(const std::string &file_name, AccessType = AccessType::READ_WRITE, LockFlags lock_flags = {},
            std::optional<std::size_t> meta_io_step_size = {}, StorageFlags = {});
        ~BDevStorage();
        
        /**
         * Create a new .db0 file
         * @param step_size_hint defines requested Page IO step size in bytes
        */
        static void create(const std::string &file_name, std::optional<std::size_t> page_size = {},
            std::uint32_t dram_page_size_hint = (16u << 10) - 256, std::optional<std::size_t> step_size_hint = {});
        
        void read(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            FlagSet<AccessOptions> = { AccessOptions::read, AccessOptions::write }) const override;
        
        void write(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer) override;
        
        // @param max_len - the maximum allowed diff-sequence length (when exceeded, the full-DP will be written)
        bool tryWriteDiffs(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            const std::vector<std::uint16_t> &diffs, unsigned int max_len = 32) override;
        
        StateNumType findMutation(std::uint64_t page_num, StateNumType state_num) const override;
        
        bool tryFindMutation(std::uint64_t page_num, StateNumType state_num, StateNumType &mutation_id) const override;
        
        bool beginRefresh() override;
        
        std::uint64_t completeRefresh(
            std::function<void(std::uint64_t updated_page_num, StateNumType state_num)> f = {}) override;
        
        bool flush(ProcessTimer * = nullptr) override;
        
        void beginCommit() override;

        void endCommit() override;
        
        void close() override;
        
        std::size_t getPageSize() const override;
        std::size_t getDRAMPageSize() const;

        StateNumType getMaxStateNum() const override;
        
        void getStats(std::function<void(const std::string &, std::uint64_t)>) const override;

        /**
         * Get last update timestamp
        */
        std::uint64_t getLastUpdated() const override;
        
        BDevStorage &asFile() override;

        const DRAM_IOStream &getDramIO() const {
            return m_dram_io;
        }
        
        // @return total bytes written / diff bytes written
        std::pair<std::size_t, std::size_t> getDiff_IOStats() const;
        
        void fetchDP_ChangeLogs(StateNumType begin_state, std::optional<StateNumType> end_state,
            std::function<void(const DP_ChangeLogT &)> f) const override;
        
        const Page_IO &getPageIO() const {
            return m_page_io;
        }

        const MetaIOStream &getMetaIO() const {
            return m_meta_io;
        }
        
        std::string getFileName() const {
            return m_file.getName();
        }

        // Copy a read-only prefix to an empty BDevStorage
        void copyTo(BDevStorage &);
        
#ifndef NDEBUG
        void getDRAM_IOMap(std::unordered_map<std::uint64_t, DRAM_PageInfo> &) const override;
        void dramIOCheck(std::vector<DRAM_CheckResult> &) const override;        
                
        // write into the validation buffer only
        void writeForValidation(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer);
#endif
        
    protected:
        // all prefix configuration must fit into this block
        static constexpr unsigned int CONFIG_BLOCK_SIZE = 4096;
        CFile m_file;
        const o_prefix_config m_config;
        
        // DRAM-changelog stream stores the sequence of updates to DRAM pages
        // DRAM-changelog must be initialized before DRAM_IOStream
        DRAM_ChangeLogStreamT m_dram_changelog_io;
        // data-page change log, each chunk corresponds to a separate data transaction        
        // holds logical data page numbers mutated in that transaction
        DP_ChangeLogStreamT m_dp_changelog_io;
        // meta-stream keeps meta-data about the other streams
        MetaIOStream m_meta_io;
        // memory-mapped file I/O
        DRAM_IOStream m_dram_io;
        // SparseIndex + DiffIndex (based over the dram_io)
        SparsePair m_sparse_pair;
        // DRAM-backed sparse index tree
        SparseIndex &m_sparse_index;
        DiffIndex &m_diff_index;
        // extension DRAM IO (only initialized when holding extension indexes e.g. REL_Index)
        std::unique_ptr<DRAM_ChangeLogStreamT> m_ext_dram_changelog_io;
        std::unique_ptr<DRAM_IOStream> m_ext_dram_io;
        ExtSpace m_ext_space;
        // the stream for storing & reading full-DPs and diff-encoded DPs
        Diff_IO m_page_io;
#ifndef NDEBUG
        MemBaseStorage m_data_mirror;
#endif
        
        bool m_refresh_pending = false;
        mutable std::shared_mutex m_mutex;
#ifndef NDEBUG
        // total number of bytes from mutated data pages
        std::uint64_t m_page_io_raw_bytes = 0;
        // a pointer to the shared throw counter
        bool m_commit_pending = false;
        unsigned int *m_throw_op_count_ptr = nullptr;
#endif

        static DRAM_IOStream init(DRAM_IOStream &&, DRAM_ChangeLogStreamT &, StorageFlags);
        static std::unique_ptr<DRAM_IOStream> initExt(std::unique_ptr<DRAM_IOStream> &&, DRAM_ChangeLogStreamT *, 
            StorageFlags, std::optional<StateNumType> max_state_num);
        
        static MetaIOStream init(MetaIOStream &&, StorageFlags);
        
        /**
         * Calculates the total number of blocks stored in this file
         * note that the last block may be partially written
        */
        std::uint64_t getBlockCount(std::uint64_t file_size) const;

        std::optional<std::uint64_t> getNextStoragePageNum() const;

        BlockIOStream getBlockIOStream(std::uint64_t first_block_pos, AccessType);
        
        DRAM_IOStream getDRAMIOStream(std::uint64_t first_block_pos, std::uint32_t dram_page_size, AccessType);
        std::unique_ptr<DRAM_IOStream> tryGetDRAMIOStream(std::uint64_t first_block_pos,
            std::uint32_t dram_page_size, AccessType);
        
        template<typename ChangeLogIOStreamT>
        ChangeLogIOStreamT getChangeLogIOStream(std::uint64_t first_block_pos, AccessType access_type)
        {
            return { m_file, first_block_pos, m_config.m_block_size, getTailFunction(), access_type };
        }
        
        template<typename ChangeLogIOStreamT>
        std::unique_ptr<ChangeLogIOStreamT> tryGetChangeLogIOStream(std::uint64_t first_block_pos, AccessType access_type)
        {
            if (first_block_pos) {
                return std::make_unique<ChangeLogIOStreamT>(
                    m_file, first_block_pos, m_config.m_block_size, getTailFunction(), access_type
                );
            } else {
                // stream does not exist
                return {};
            }            
        }

        MetaIOStream getMetaIOStream(std::uint64_t first_block_pos, std::size_t step_size, AccessType);
        
        Diff_IO getPage_IO(std::optional<std::uint64_t> next_page_hint, std::uint32_t step_size);
        
        o_prefix_config readConfig() const;
        
        /**
         * Get the first available address (i.e. end of the file)
         */
        std::uint64_t tail() const;

        std::function<std::uint64_t()> getTailFunction() const;

        std::function<std::uint64_t()> getBlockIOTailFunction() const;
        
        // non-virtual version of tryFindMutation
        bool tryFindMutationImpl(std::uint64_t page_num, StateNumType state_num,
            StateNumType &mutation_id) const;
        
        // @param chain_len length of the diff-storage chain processed while reading
        void _read(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
            FlagSet<AccessOptions> = { AccessOptions::read, AccessOptions::write }, unsigned int *chain_len = nullptr) const;
        
        // Flush ext-space streams only (if existing)
        bool flushExt(StateNumType max_state_num);
        void fsync();
        
        // Synchronization state number for ext-space
        std::optional<StateNumType> getMaxExtStateNum() const;
    };
    
}