// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "BDevStorage.hpp"
#include "SparseIndexQuery.hpp"
#include <unordered_set>
#include <unordered_map>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>
#include <dbzero/core/memory/utils.hpp>
#include "copy_prefix.hpp"

namespace db0

{

    o_prefix_config::o_prefix_config(std::uint32_t block_size, std::uint32_t page_size,
        std::uint32_t dram_page_size, std::uint32_t page_io_step_size)
        : m_block_size(block_size)
        , m_page_size(page_size)
        , m_dram_page_size(dram_page_size)
        , m_page_io_step_size(page_io_step_size)
    {
        std::memset(m_reserved.data(), 0, sizeof(m_reserved));
    }
    
    DRAM_Pair tryGetDRAMPair(DRAM_IOStream *dram_io_ptr)
    {
        if (!dram_io_ptr) {
            return {};
        }
        return dram_io_ptr->getDRAMPair();
    }
    
    BDevStorage::BDevStorage(const std::string &file_name, AccessType access_type, LockFlags lock_flags,
        std::optional<std::size_t> meta_io_step_size, StorageFlags flags)
        : BaseStorage(access_type, flags)
        , m_file(file_name, access_type, lock_flags)
        , m_config(readConfig())
        , m_dram_changelog_io(getChangeLogIOStream<DRAM_ChangeLogStreamT>(
            m_config.m_dram_changelog_io_offset, access_type)
        )
        , m_dp_changelog_io(getChangeLogIOStream<DP_ChangeLogStreamT>(
            m_config.m_dp_changelog_io_offset, access_type)
        )
        , m_meta_io(init(getMetaIOStream(
            m_config.m_meta_io_offset, meta_io_step_size.value_or(DEFAULT_META_IO_STEP_SIZE), access_type), flags)
        )
        , m_dram_io(init(getDRAMIOStream(
            m_config.m_dram_io_offset, m_config.m_dram_page_size, access_type), m_dram_changelog_io, flags)
        )
        , m_sparse_pair(m_dram_io.getDRAMPair(), access_type, flags)
        , m_sparse_index(m_sparse_pair.getSparseIndex())
        , m_diff_index(m_sparse_pair.getDiffIndex())
        , m_ext_dram_changelog_io(tryGetChangeLogIOStream<DRAM_ChangeLogStreamT>(
            m_config.m_ext_dram_changelog_io_offset, access_type)
        )
        , m_ext_dram_io(initExt(tryGetDRAMIOStream(
            m_config.m_ext_dram_io_offset, m_config.m_ext_dram_page_size, access_type), 
            m_ext_dram_changelog_io.get(), 
            // NOTE: the NO_LOAD flag is not applicable to ext DRAM IO since it's created on-demand
            flags & ~ StorageFlags {StorageOptions::NO_LOAD },
            // NOTE: we synchronize up to the maximum state number from DRAM IO (in read/write mode)
            this->getMaxExtStateNum())
        )
        , m_ext_space(tryGetDRAMPair(m_ext_dram_io.get()), access_type)
        , m_page_io(getPage_IO(getNextStoragePageNum(), m_config.m_page_io_step_size))
#ifndef NDEBUG
        , m_data_mirror(m_config.m_page_size)
#endif
    {
        if (m_access_type == AccessType::READ_WRITE && m_flags.test(StorageOptions::NO_LOAD)) {
            THROWF(db0::IOException) << "Cannot open prefix in READ_WRITE mode with NO_LOAD option";
        }
        
        // in read-only mode need to refresh in order to retrieve a consitent DRAM state
        // since other process might be actively modifying the underlying file
        if (m_access_type == AccessType::READ_ONLY && !m_flags.test(StorageOptions::NO_LOAD)) {
            refresh();
        }
        
        // Validate state consistency
        // The state number reported by DRAM IO must NOT superseed the last state number recorded in DP changelog
        if (auto chunk_ptr = m_dp_changelog_io.getLastChangeLogChunk()) {
            auto dp_state_num = chunk_ptr->m_state_num;
            auto dram_state_num = m_sparse_pair.getMaxStateNum();            
            if (dram_state_num > dp_state_num) {
                THROWF(db0::IOException) << "Inconsistent state: DRAM state number " << dram_state_num
                    << " exceeds DP changelog state number " << dp_state_num;
            }
        }
    }
    
    BDevStorage::~BDevStorage()
    {
    }
    
    DRAM_IOStream BDevStorage::init(DRAM_IOStream &&dram_io, DRAM_ChangeLogStreamT &dram_change_log, StorageFlags flags)
    {
        if (!flags[StorageOptions::NO_LOAD]) {
            dram_io.load(dram_change_log);
        }        
        return std::move(dram_io);
    }
    
    std::unique_ptr<DRAM_IOStream> BDevStorage::initExt(std::unique_ptr<DRAM_IOStream> &&dram_io,
        DRAM_ChangeLogStreamT *dram_change_log, StorageFlags flags, std::optional<StateNumType> max_state_num)
    {
        if (dram_io && !flags[StorageOptions::NO_LOAD]) {
            assert(dram_change_log);
            dram_io->load(*dram_change_log, max_state_num);
        }
        return std::move(dram_io);
    }
    
    MetaIOStream BDevStorage::init(MetaIOStream &&io, StorageFlags flags)
    {
        if (!flags[StorageOptions::NO_LOAD]) {
            // exhaust the meta-log stream (position at the last item) and all managed streams
            io.setTailAll();
        }
        return std::move(io);
    }
    
    o_prefix_config BDevStorage::readConfig() const
    {
        std::vector<char> buffer(CONFIG_BLOCK_SIZE);
        m_file.read(0, buffer.size(), buffer.data());
        auto &config = o_prefix_config::__const_ref(buffer.data());
        if (config.m_magic != o_prefix_config::DB0_MAGIC) {
            THROWF(db0::IOException) << "Not a dbzero file: " << m_file.getName();
        }
        return config;
    }
    
    std::uint32_t getPageIOStepSize(std::uint32_t block_size, std::optional<std::size_t> step_size_hint)
    {
        if (step_size_hint && *step_size_hint > 0) {
            // align to full block size
            return (*step_size_hint + block_size - 1) / block_size;
        } else {
            // default to single-block steps
            return 1u;
        }
    }
    
    void BDevStorage::create(const std::string &file_name, std::optional<std::size_t> page_size,
        std::uint32_t dram_page_size_hint, std::optional<std::size_t> step_size_hint)
    {
        if (!page_size) {
            page_size = DEFAULT_PAGE_SIZE;
        }
        
        std::vector<char> buffer(CONFIG_BLOCK_SIZE);
        // calculate block size to be page aligned and sufficient to fit a single sparse index node
        auto min_block_size = dram_page_size_hint + 
            BlockIOStream::sizeOfHeaders(DRAM_IOStream::ENABLE_CHECKSUMS) + DRAM_IOStream::sizeOfHeader();
        // page-align block size
        auto block_size = (min_block_size + *page_size - 1) / (*page_size) * (*page_size);
        // adjust DRAM page size to fit the block
        auto dram_page_size = block_size - BlockIOStream::sizeOfHeaders(DRAM_IOStream::ENABLE_CHECKSUMS) - 
            DRAM_IOStream::sizeOfHeader();

        // create a new config using placement new
        auto config = new (buffer.data()) o_prefix_config(
            block_size, *page_size, dram_page_size, getPageIOStepSize(block_size, step_size_hint)
        );
        
        std::uint64_t offset = CONFIG_BLOCK_SIZE;
        auto next_block_offset = [&]() 
        {
            auto result = offset;
            offset += block_size;
            return result;
        };

        // cofigure offsets for all inner streams (even though they have not been materialized yet)
        config->m_dram_io_offset = next_block_offset();
        config->m_dram_changelog_io_offset = next_block_offset();
        config->m_dp_changelog_io_offset = next_block_offset();
        config->m_meta_io_offset = next_block_offset();

        // initialize ext streams only when needed
        bool has_ext_dram_io = config->m_page_io_step_size > 1;
        if (has_ext_dram_io) {
            config->m_ext_dram_io_offset = next_block_offset();
            // NOTE: use entire block for ext DRAM page
            config->m_ext_dram_page_size = dram_page_size;
            config->m_ext_dram_changelog_io_offset = next_block_offset();
        }
        
        CFile::create(file_name, buffer);
        
        // Create higher-order data structures
        {
            CFile file(file_name, AccessType::READ_WRITE);
            DRAM_ChangeLogStreamT *dram_changelog_io_ptr = nullptr;
            DRAM_IOStream *dram_io_ptr = nullptr;
            std::unique_ptr<DRAM_ChangeLogStreamT> ext_dram_changelog_io_ptr = nullptr;
            std::unique_ptr<DRAM_IOStream> ext_dram_io_ptr = nullptr;
            
            auto tail_function = [&]()
            {
                assert(dram_io_ptr && dram_changelog_io_ptr);                
                // take max from the underlying I/O streams
                auto result = std::max(offset, std::max(dram_io_ptr->tail(), dram_changelog_io_ptr->tail()));
                if (ext_dram_io_ptr && ext_dram_changelog_io_ptr) {
                    result = std::max(result, std::max(ext_dram_io_ptr->tail(), ext_dram_changelog_io_ptr->tail()));
                }
                return result;
            };
            
            auto dram_changelog_io = DRAM_ChangeLogStreamT(file, config->m_dram_changelog_io_offset, config->m_block_size,
                tail_function, AccessType::READ_WRITE);
            dram_changelog_io_ptr = &dram_changelog_io;
            auto dram_io = DRAM_IOStream(file, config->m_dram_io_offset, config->m_block_size, tail_function,
                AccessType::READ_WRITE, config->m_dram_page_size);
            dram_io_ptr = &dram_io;
            
            // Initialize extension streams when needed
            if (has_ext_dram_io) {
                ext_dram_changelog_io_ptr = std::make_unique<DRAM_ChangeLogStreamT>(file,
                    static_cast<std::uint64_t>(config->m_ext_dram_changelog_io_offset), 
                    static_cast<std::uint32_t>(config->m_block_size), tail_function,
                    AccessType::READ_WRITE);
                ext_dram_io_ptr = std::make_unique<DRAM_IOStream>(file,
                    static_cast<std::uint64_t>(config->m_ext_dram_io_offset), 
                    static_cast<std::uint32_t>(config->m_block_size), tail_function, AccessType::READ_WRITE,
                    static_cast<std::uint32_t>(config->m_ext_dram_page_size));
            }
            
            // create then flush an empty sparse pair (i.e. SparseIndex + DiffIndex)
            SparsePair sparse_pair(SparsePair::tag_create(), dram_io.getDRAMPair());
            auto max_state_num = sparse_pair.getMaxStateNum();
            dram_io.flushUpdates(max_state_num, dram_changelog_io);
            dram_changelog_io.flush();
            dram_io.close();
            dram_changelog_io.close();
            
            // create then flush the extension space
            if (has_ext_dram_io) {
                assert(ext_dram_io_ptr && ext_dram_changelog_io_ptr);
                ExtSpace ext_space(ExtSpace::tag_create(), ext_dram_io_ptr->getDRAMPair());
                ext_dram_io_ptr->flushUpdates(max_state_num, *ext_dram_changelog_io_ptr);
                ext_dram_changelog_io_ptr->flush();
                ext_dram_io_ptr->close();
                ext_dram_changelog_io_ptr->close();
            }
            
            file.close();
        }
    }
    
    bool BDevStorage::tryFindMutation(std::uint64_t page_num, StateNumType state_num,
        StateNumType &mutation_id) const
    {
        std::shared_lock<std::shared_mutex> lock(m_mutex);
        return db0::tryFindMutation(m_sparse_index, m_diff_index, page_num, state_num, mutation_id);
    }
    
    StateNumType BDevStorage::findMutation(std::uint64_t page_num, StateNumType state_num) const
    {
        StateNumType result;
        std::shared_lock<std::shared_mutex> lock(m_mutex);
        if (!db0::tryFindMutation(m_sparse_index, m_diff_index, page_num, state_num, result)) {
            assert(false && "BDevStorage::findMutation: page not found");
            THROWF(db0::IOException) 
                << "BDevStorage::findMutation: page_num " << page_num << " not found, state: " << state_num;
        }
        return result;
    }
    
    void BDevStorage::read(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
        FlagSet<AccessOptions> flags) const
    {
        std::shared_lock<std::shared_mutex> lock(m_mutex);
        _read(address, state_num, size, buffer, flags);
    }
    
    void BDevStorage::_read(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
        FlagSet<AccessOptions> flags, unsigned int *chain_len) const
    {     
        assert(state_num > 0 && "BDevStorage::read: state number must be > 0");
        assert((address % m_config.m_page_size == 0) && "BDevStorage::read: address must be page-aligned");
        assert((size % m_config.m_page_size == 0) && "BDevStorage::read: size must be page-aligned");

        if (flags[AccessOptions::write] && m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::IOException) << "BDevStorage::read: invalid write fiag to access read-only resource";
        }

        auto begin_page = address / m_config.m_page_size;
        auto end_page = begin_page + size / m_config.m_page_size;
        
        if (chain_len) {
            *chain_len = 0;
        }
        
        std::byte *read_buf = reinterpret_cast<std::byte *>(buffer);
        // lookup sparse index and read physical pages
        for (auto page_num = begin_page; page_num != end_page; ++page_num, read_buf += m_config.m_page_size) {
            // query sparse index + diff index
            SparseIndexQuery query(m_sparse_index, m_diff_index, page_num, state_num);
            if (query.empty()) {
                if (flags[AccessOptions::read]) {
                    THROWF(db0::IOException) << "BDevStorage::read: page not found: " << page_num << ", state: " << state_num;
                }
                // if requested access is write-only then simply fill the misssing (new) page with 0                
                std::memset(read_buf, 0, m_config.m_page_size);
                continue;
            }
            
            // query.first yields the full-DP (if it exists)            
            std::uint64_t page_io_id = query.first();
            if (page_io_id) {
                if (!!m_ext_space) {
                    // convert relative page number back to absolute
                    page_io_id = m_ext_space.getAbsolute(page_io_id);
                }
                // read full DP
                m_page_io.read(page_io_id, read_buf);
            } else {
                // requesting a diff-DP only encoded page, use zero buffer as a base
                std::memset(read_buf, 0, m_config.m_page_size);
            }
            
            // apply changes from diff-DPs
            std::uint32_t diff_state_num;
            while (query.next(diff_state_num, page_io_id)) {
                if (!!m_ext_space) {
                    // convert relative page number back to absolute
                    page_io_id = m_ext_space.getAbsolute(page_io_id);
                }
                // apply all diff-updates on top of the full-DP
                m_page_io.applyFrom(page_io_id, read_buf, { page_num, diff_state_num });
                // collect chain-len statistics
                if (chain_len) {
                    ++(*chain_len);
                }
            }            
        }
        
#ifndef NDEBUG
        if (Settings::__storage_validation) {
            // validate read against in-memory mirror
            m_data_mirror.validateRead(address, state_num, size, buffer, flags);
        }
#endif
    }

#ifndef NDEBUG    
    void BDevStorage::writeForValidation(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer) {
        m_data_mirror.write(address, state_num, size, buffer);
    }
#endif

    void BDevStorage::write(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer)
    {                        
#ifndef NDEBUG
        if (Settings::__storage_validation) {        
            m_data_mirror.write(address, state_num, size, buffer);
        }
#endif
        assert(state_num > 0 && "BDevStorage::write: state number must be > 0");
        assert((address % m_config.m_page_size == 0) && "BDevStorage::write: address must be page-aligned");
        assert((size % m_config.m_page_size == 0) && "BDevStorage::write: size must be page-aligned");
        
        auto begin_page = address / m_config.m_page_size;
        auto end_page = begin_page + size / m_config.m_page_size;
        
        std::byte *write_buf = reinterpret_cast<std::byte *>(buffer);
        
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        // write as physical pages and register with the sparse index
        for (auto page_num = begin_page; page_num != end_page; ++page_num, write_buf += m_config.m_page_size) {
            // look up if page has already been added in current transaction
            auto item = m_sparse_index.lookup(page_num, state_num);
            if (item && item.m_state_num == state_num) {
                // page already added in current transaction / update in the stream
                // this may happen due to cache overflow and later modification of the same page                
                auto page_io_id = item.m_storage_page_num;
                if (!!m_ext_space) {
                    // convert relative page number back to absolute
                    page_io_id = m_ext_space.getAbsolute(page_io_id);
                }
                m_page_io.write(page_io_id, write_buf);
            } else {
                // append as new page
                bool is_first_page;
                auto page_io_id = m_page_io.append(write_buf, &is_first_page);
                if (!!m_ext_space) {
                    // NOTE: first page (of each step) must be registered with REL_Index if it's maintained
                    // assign a relative page number
                    page_io_id = m_ext_space.assignRelative(page_io_id, is_first_page);
                }
                m_sparse_index.emplace(page_num, state_num, page_io_id);
#ifndef NDEBUG                
                m_page_io_raw_bytes += m_config.m_page_size;
                checkPoisonedOp(Settings::__write_poison);
#endif
            }
        }
    }
    
    bool BDevStorage::tryWriteDiffs(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
        const std::vector<std::uint16_t> &diff_data, unsigned int max_len)
    {
        assert(state_num > 0 && "BDevStorage::writeDiffs: state number must be > 0");
        assert((address % m_config.m_page_size == 0) && "BDevStorage::writeDiffs: address must be page-aligned");
        assert(size == m_config.m_page_size && "BDevStorage::writeDiffs: size must be equal to page size");
        
        auto page_num = address / m_config.m_page_size;
        
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        // Use SparseIndexQuery to determine the current sequence length & check limits
        SparseIndexQuery query(m_sparse_index, m_diff_index, page_num, state_num);
        // if a page has already been written as full-DP in the current transaction then
        // we cannot append as diff but need to overwrite the full page instead
        if (state_num != query.firstStateNum() && query.leftLessThan(max_len)) {
            bool is_first_page;
            // append as diff-page (NOTE: diff-writes are only appended)
            auto [page_io_id, overflow] = m_page_io.appendDiff(buffer, { page_num, state_num }, diff_data, &is_first_page);
            if (!!m_ext_space) {
                // NOTE: first page (of each step) must be registered with REL_Index if it's maintained
                // assign a relative page number
                page_io_id = m_ext_space.assignRelative(page_io_id, is_first_page);
            }
            m_diff_index.insert(page_num, state_num, page_io_id, overflow);
        } else {
            // Unable to write as diff
            // this mey be due to either:
            // - page already added in same transaction (unable to overwrite as diff)
            // - exceeding max chain length            
            return false;
        }

#ifndef NDEBUG
        m_page_io_raw_bytes += m_config.m_page_size;
        checkPoisonedOp(Settings::__write_poison);
#endif

#ifndef NDEBUG
        if (Settings::__storage_validation) {
            m_data_mirror.writeDiffs(address, state_num, size, buffer, diff_data, max_len);     
        }
#endif
        return true;
    }
    
    std::size_t BDevStorage::getPageSize() const {
        return m_config.m_page_size;
    }
    
    std::size_t BDevStorage::getDRAMPageSize() const {
        return m_config.m_dram_page_size;
    }
    
    bool BDevStorage::flushExt(StateNumType max_state_num)
    {        
        if (!m_ext_space) {
            return false;
        }
        m_ext_space.commit();
        assert(m_ext_dram_io);
        assert(m_ext_dram_changelog_io);
        m_ext_dram_io->flushUpdates(max_state_num, *m_ext_dram_changelog_io);
        m_ext_dram_changelog_io->flush();
        return true;
    }
    
    bool BDevStorage::flush(ProcessTimer *parent_timer)
    {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        std::unique_ptr<ProcessTimer> timer;
        if (parent_timer) {
            timer = std::make_unique<ProcessTimer>("BDevStorage::flush", parent_timer);
        }
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::IOException) << "BDevStorage::flush error: read-only stream";
        }
        
        // check if there're any modifications to be flushed        
        if (m_sparse_pair.getChangeLogSize() == 0) {
            // no modifications to be flushed
            return false;
        }        
        
        // save metadata checkpoints before making any updates to the managed streams
        // NOTE: the checkpoint is only saved after exceeding specific threshold of updates in the managed streams
        auto state_num = m_sparse_pair.getMaxStateNum();
        
        m_meta_io.checkAndAppend(state_num);
        m_meta_io.flush();
        
        m_page_io.flush();
        // Extract & flush sparse index change log first (on condition of any updates)
        // we also need to collect the end storage page number, possibly relative (sentinel)
        bool is_first = false;
        auto end_page_io_page_num = m_page_io.getEndPageNum(&is_first);
        if (!!m_ext_space) {
            // convert to relative page number
            end_page_io_page_num = m_ext_space.assignRelative(end_page_io_page_num, is_first);
        }
        
        m_sparse_pair.extractChangeLog(m_dp_changelog_io, end_page_io_page_num);
        m_dram_io.flushUpdates(state_num, m_dram_changelog_io);
        m_dp_changelog_io.flush();
        // Flush ext streams (if existing)
        flushExt(state_num);
        // NOTE: fsync has stronger guarantees than flush in a multi-process environments
        m_file.fsync();
        // flush changelog AFTER all updates from all other streams have been flushed        
        m_dram_changelog_io.flush();
        // the last fsync finalizes the commit
        m_file.fsync();
        
        // commit to collect future updates correctly        
        m_sparse_pair.commit();
        return true;
    }
    
    void BDevStorage::close()
    {    
        if (m_access_type == AccessType::READ_WRITE) {
            flush();
        }
        
        // Close extension streams
        if (m_ext_dram_io) {
            assert(m_ext_dram_changelog_io);
            m_ext_dram_io->close();
            m_ext_dram_changelog_io->close();
        }
        
        m_dram_io.close();
        m_dram_changelog_io.close();
        m_dp_changelog_io.close(); 
        m_meta_io.close();
        m_file.close();
    }
    
    BlockIOStream BDevStorage::getBlockIOStream(std::uint64_t first_block_pos, AccessType access_type) {
        return { m_file, first_block_pos, m_config.m_block_size, getTailFunction(), access_type };
    }
    
    MetaIOStream BDevStorage::getMetaIOStream(std::uint64_t first_block_pos, std::size_t step_size, AccessType access_type)
    {
        // NOTE: currently only the dp-changelog stream is managed by the meta-io
        std::vector<BlockIOStream *> managed_streams = { &m_dp_changelog_io };
        return { m_file, managed_streams, first_block_pos, m_config.m_block_size, getTailFunction(),
            access_type, MetaIOStream::ENABLE_CHECKSUMS, step_size 
        };
    }
    
    DRAM_IOStream BDevStorage::getDRAMIOStream(std::uint64_t first_block_pos, std::uint32_t dram_page_size, AccessType access_type) {
        return { m_file, first_block_pos, m_config.m_block_size, getTailFunction(), access_type, dram_page_size };
    }
    
    std::unique_ptr<DRAM_IOStream> BDevStorage::tryGetDRAMIOStream(std::uint64_t first_block_pos,
            std::uint32_t dram_page_size, AccessType access_type)
    {
        if (!first_block_pos) {
            return nullptr;
        }
        return std::make_unique<DRAM_IOStream>(m_file, first_block_pos, m_config.m_block_size, 
            getTailFunction(), access_type, dram_page_size);
    }
    
    std::uint64_t BDevStorage::tail() const
    {
        // take max from the 4 underlying I/O streams
        auto result = std::max(m_dram_io.tail(), m_meta_io.tail());
        result = std::max(result, m_dram_changelog_io.tail());
        result = std::max(result, m_dp_changelog_io.tail());
        result = std::max(result, m_page_io.tail());

        // include ext streams when initialized
        if (m_ext_dram_io) {
            assert(m_ext_dram_changelog_io);
            result =  std::max(result, std::max(m_ext_dram_io->tail(), m_ext_dram_changelog_io->tail()));
        }
        
        return result;
    }
    
    Diff_IO BDevStorage::getPage_IO(std::optional<std::uint64_t> next_page_hint, std::uint32_t step_size)
    {        
        auto block_capacity = m_config.m_block_size / m_config.m_page_size;
        
        std::optional<std::uint32_t> block_num;
        std::uint64_t address = 0;
        std::uint32_t page_count = 0;
        
        if (next_page_hint) {
            auto block_id = (*next_page_hint * m_config.m_page_size) / m_config.m_block_size;
            address = CONFIG_BLOCK_SIZE + block_id * m_config.m_block_size;
            page_count = static_cast<std::uint32_t>(*next_page_hint % block_capacity);
            
            // position at the end of the last existing block
            if (page_count == 0) {
                address -= m_config.m_block_size;
                page_count = block_capacity;
            }
        } else {        
            // assign first page
            address = std::max(m_dram_io.tail(), m_meta_io.tail());
            address = std::max(address, m_dram_changelog_io.tail());
            address = std::max(address, m_dp_changelog_io.tail());
            if (m_ext_dram_io) {
                assert(m_ext_dram_changelog_io);
                address = std::max(address, m_ext_dram_io->tail());
                address = std::max(address, m_ext_dram_changelog_io->tail());
            }

            // NOTE: initialize with a known block num = 0 (first block of the first step)
            block_num = 0;
        }

        // NOTE: block num is unknown in this case
        return { CONFIG_BLOCK_SIZE, m_file, m_config.m_page_size, m_config.m_block_size, address, page_count,
            step_size, getBlockIOTailFunction(), block_num
        };
    }
    
    std::uint32_t BDevStorage::getMaxStateNum() const {
        return m_sparse_pair.getMaxStateNum();
    }
    
    std::function<std::uint64_t()> BDevStorage::getTailFunction() const
    {
        return [this]() {
            return this->tail();
        };
    }

    std::function<std::uint64_t()> BDevStorage::getBlockIOTailFunction() const
    {
        // get tail from BlockIOStreams
        return [this]() -> std::uint64_t {
            auto result = std::max(m_dram_io.tail(), m_meta_io.tail());
            result = std::max(result, m_dram_changelog_io.tail());
            result = std::max(result, m_dp_changelog_io.tail());
            if (m_ext_dram_io) {
                assert(m_ext_dram_changelog_io);
                result = std::max(result, m_ext_dram_io->tail());
                result = std::max(result, m_ext_dram_changelog_io->tail());
            }
            return result;
        };
    }
    
    bool BDevStorage::beginRefresh()
    {
        if (m_access_type != AccessType::READ_ONLY) {
            THROWF(db0::IOException) << "BDevStorage::refresh allowed only in read-only mode";
        }
        if (!m_refresh_pending) {
            m_refresh_pending = m_dram_changelog_io.refresh();
            // NOTE: inclusion of ext-space is not necessary here since DRAM changelog
            // is sufficient to determine if there're any updates
        }
        return m_refresh_pending;
    }
    
    std::uint64_t BDevStorage::completeRefresh(
        std::function<void(std::uint64_t page_num, StateNumType state_num)> on_page_updated)
    {
        assert(m_access_type == AccessType::READ_ONLY);
        std::uint64_t result = 0;
        // NOTE: in some situations (e.g. very slow reader) we might not be able
        // to grab the consistent snapshot of the DRAM prefix, in such case the operation
        // needs to be retried until successful
        // WARNING: if the reader is much slower that the writer (~100x slower) then this loop might not terminate
        bool is_consistent = true;
        // continue refreshing until all updates are retrieved to guarantee a consistent state
        do {
            // safe stream positions for rollback on file read failure
            auto dram_changelog_io_pos = m_dram_changelog_io.getStreamPos();
            std::pair<std::uint64_t, std::uint64_t> ext_dram_changelog_io_pos;
            if (!!m_ext_space) {
                assert(m_ext_dram_changelog_io);
                ext_dram_changelog_io_pos = m_ext_dram_changelog_io->getStreamPos();
            }
            auto dp_changelog_io_pos = m_dp_changelog_io.getStreamPos();
            // reverts streams to previous positions
            auto revert_streams = [&]() {
                m_dram_changelog_io.setStreamPos(dram_changelog_io_pos);
                m_dp_changelog_io.setStreamPos(dp_changelog_io_pos);
                if (!!m_ext_space) {
                    assert(m_ext_dram_changelog_io);
                    m_ext_dram_changelog_io->setStreamPos(ext_dram_changelog_io_pos);
                }
            };
            
            try {
                auto dram_state_num = m_dram_io.beginApplyChanges(m_dram_changelog_io);
                if (!dram_state_num) {
                    // no updates to process
                    break;
                }
                dram_changelog_io_pos = m_dram_changelog_io.getStreamPos();
                // NOTE: ext DRAM updates have incremental nature so they might preceed DRAM updates
                // without breaking the consistency
                std::optional<StateNumType> ext_dram_state_num;
                if (!!m_ext_space) {
                    assert(m_ext_dram_changelog_io);
                    m_ext_dram_changelog_io->refresh();
                    ext_dram_state_num = m_ext_dram_io->beginApplyChanges(*m_ext_dram_changelog_io);
                    ext_dram_changelog_io_pos = m_ext_dram_changelog_io->getStreamPos();
                }
                
                assert(dram_state_num);
                is_consistent = m_dram_io.completeApplyChanges(*dram_state_num);
                if (!!m_ext_space && ext_dram_state_num) {
                    is_consistent &= m_ext_dram_io->completeApplyChanges(*ext_dram_state_num);
                    m_ext_space.refresh();
                }
                
                if (!is_consistent) {
                    // must continue with the refresh until getting a consistent state
                    m_dram_changelog_io.refresh();
                    continue;
                }
                
                // refresh underlying sparse index / diff index after DRAM update
                m_sparse_pair.refresh();
                
                // this is the state number to sync-up to (which must be identical as dram_state_num)
                auto max_state_num = m_sparse_pair.getMaxStateNum();
                if (dram_state_num != max_state_num) {
                    // NOTE: this critical and irrecoverable error indicates corruption of the DRAM changelog stream
                    THROWF(db0::InternalException) << "Inconsistent state: DRAM changelog state number "
                        << *dram_state_num << " does not match max known state number " << max_state_num;
                }
                
                // send all page-update notifications to the provided handler
                if (on_page_updated) {
                    StateNumType updated_state_num = 0;
                    m_dp_changelog_io.refresh();
                    // NOTE: readers allow reading the same contents multiple times
                    auto reader = m_dp_changelog_io.getStreamReader();
                    // feed the reader with all available chunks, in case of IOException the stream is getting reverted
                    // this is to make the operation atomic
                    while (auto chunk_ptr = reader.readChangeLogChunk()) {
                        if (chunk_ptr->m_state_num == max_state_num) {
                            // stop at the max known state number
                            break;
                        }
                        if (chunk_ptr->m_state_num > max_state_num) {
                            // NOTE: this critical and irrecoverable error indicates corruption of the DP changelog stream
                            THROWF(db0::InternalException) << "Inconsistent state: DP changelog state number "
                                << chunk_ptr->m_state_num << " exceeds max known state number " << max_state_num;   
                        }
                    }
                    
                    // reset to read all updates again
                    reader.reset();
                    for (;;) {
                        auto dp_change_log_ptr = reader.readChangeLogChunk();
                        if (!dp_change_log_ptr || dp_change_log_ptr->m_state_num > max_state_num) {
                            // end of the stream or the max known state number reached
                            break;
                        }
                        
                        assert(dp_change_log_ptr->m_state_num != updated_state_num);
                        updated_state_num = dp_change_log_ptr->m_state_num;
                        // Elements are logical page numbers (mutated in that transaction)
                        for (auto page_num: *dp_change_log_ptr) {
                            on_page_updated(page_num, updated_state_num);                            
                        }                        
                    }
                }
                
            } catch (db0::IOException &) {
                revert_streams();
                // NOTE: this may be a temporary problem, refresh needs repeating
                break;
            }
            
            if (!result) {
                result = m_file.getLastModifiedTime();
            }
            
            m_meta_io.refresh();
            // refresh cycle complete
            m_refresh_pending = false;
        }
        while (beginRefresh() || !is_consistent);
        return result;
    }
    
    std::uint64_t BDevStorage::getLastUpdated() const {
        return m_file.getLastModifiedTime();
    }
    
    void BDevStorage::getStats(std::function<void(const std::string &, std::uint64_t)> callback) const
    {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        callback("dram_io_rand_ops", m_dram_io.getRandOpsCount());
        callback("dram_io_size", m_dram_io.getDRAMPrefix().size());
        auto file_rand_ops = m_file.getRandOps();
        callback("file_rand_read_ops", file_rand_ops.first);
        callback("file_rand_write_ops", file_rand_ops.second);
        auto file_io_bytes = m_file.getIOBytes();
        callback("file_bytes_read", file_io_bytes.first);
        callback("file_bytes_written", file_io_bytes.second);
        // total size of data pages
        callback("dp_size_total", m_sparse_pair.size() * m_page_io.getPageSize());
        callback("prefix_size", m_file.size());
        auto page_io_stats = m_page_io.getStats();
        callback("page_io_total_bytes", page_io_stats.first);
        callback("page_io_diff_bytes", page_io_stats.second);
        if (m_ext_dram_io) {
            callback("ext_dram_io_size", m_ext_dram_io->getDRAMPrefix().size());
        }
        #ifndef NDEBUG
        callback("page_io_raw_bytes", m_page_io_raw_bytes);
        #endif
    }
    
    std::pair<std::size_t, std::size_t> BDevStorage::getDiff_IOStats() const 
    {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        return m_page_io.getStats();
    }

#ifndef NDEBUG
    void BDevStorage::getDRAM_IOMap(std::unordered_map<std::uint64_t, DRAM_PageInfo> &io_map) const {
        m_dram_io.getDRAM_IOMap(io_map);
    }
    
    void BDevStorage::dramIOCheck(std::vector<DRAM_CheckResult> &check_result) const {
        m_dram_io.dramIOCheck(check_result);
    }
#endif
    
    void BDevStorage::fetchDP_ChangeLogs(StateNumType begin_state, std::optional<StateNumType> end_state,
        std::function<void(const DP_ChangeLogT &)> f) const
    {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        if (m_dp_changelog_io.modified()) {
            THROWF(db0::IOException) << "BDevStorage::fetchChangeLogs: dp-changelog is modified and needs to be flushed first";
        }
        auto &dp_changelog_io = const_cast<DP_ChangeLogStreamT &>(m_dp_changelog_io);
        DP_ChangeLogStreamT::State dp_state;
        dp_changelog_io.saveState(dp_state);
        
        {
            std::vector<char> buf;
            // try locating the nearest meta-log entry to position the dp-changelog
            auto meta_log_ptr = m_meta_io.lowerBound(begin_state, buf);
            if (meta_log_ptr) {
                // the 1st meta-item is associated with tha dp_change_log
                auto &item = *meta_log_ptr->getMetaItems().begin();
                dp_changelog_io.setStreamPos(item.m_address, item.m_stream_pos);
            } else {
                // must scan starting from the beginning (head)
                dp_changelog_io.setStreamPosHead();
            }
        }

        try {
            for (;;) {
                auto change_log = dp_changelog_io.readChangeLogChunk();
                if (!change_log) {
                    // end of the stream reached
                    break;
                }
                auto state_num = change_log->m_state_num;
                if (end_state && state_num >= *end_state) {
                    // end of the range reached
                    break;
                }
                if (state_num >= begin_state) {
                    f(*change_log);
                }
            }
        } catch (...) {
            dp_changelog_io.restoreState(dp_state);            
            throw;
        }
        dp_changelog_io.restoreState(dp_state);
    }
    
    void BDevStorage::beginCommit()
    {
#ifndef NDEBUG        
        m_commit_pending = true;
#endif        
    }
    
    void BDevStorage::endCommit()
    {
#ifndef NDEBUG        
        m_commit_pending = false;
#endif        
    }
    
    void BDevStorage::fsync() {
        m_file.fsync();
    }
    
    void BDevStorage::copyTo(BDevStorage &out)
    {
        if (!out.m_ext_space) {
            THROWF(db0::IOException) << "BDevStorage::copyTo: destination storage must have ext-space initialized";
        }
        
        auto writer = out.m_dram_changelog_io.getStreamWriter();
        auto maybe_max_state_num = copyDRAM_IO(m_dram_io, m_dram_changelog_io, out.m_dram_io, writer);
        if (!maybe_max_state_num) {
            // nothing to copy
            return;
        }

        auto max_state_num = *maybe_max_state_num;
        // copy up to the max_state_num (inclusive)
        auto dp_header = copyDPStream(m_dp_changelog_io, out.m_dp_changelog_io, max_state_num);
        if (!dp_header) {
            THROWF(db0::IOException) << "BDevStorage::copyTo: failed to copy DP changelog";
        }
        
        // assure copied streams are consistent
        if (dp_header->m_state_num != max_state_num) {
            THROWF(db0::IOException) 
                << "BDevStorage::copyTo: inconsistent max_state_num in DP changelog: "
                << (StateNumType)(dp_header->m_state_num) << " != " << max_state_num;
        }
        std::uint64_t end_page_num = dp_header->m_end_storage_page_num;
        // NOTE: end_page_num may be relative, need to translate to absolute
        if (!!m_ext_space) {
            end_page_num = m_ext_space.getAbsolute(end_page_num);
        }
        copyPageIO(m_page_io, m_ext_space, out.m_page_io, end_page_num, out.m_ext_space);
        
        // NOTE: meta_is stream can't be copied since it's structure depends on the managed streams
        // NOTE: for simplicity we don't generate the entire meta-io, just save the last checkpoint
        out.m_meta_io.checkAndAppend(max_state_num);

        // flush ext-space only, the other streams are already flushed by copy operators
        // NOTE: we need to use max state num from the source storage since the desination
        // did not load the sparse index (it was only copied)
        out.flushExt(max_state_num);
        out.fsync();
        // flush DRAM-changelog as the last step (important for consitency)
        writer.flush();
        out.fsync();
    }
    
    BDevStorage &BDevStorage::asFile() {
        return *this;
    }
    
    std::optional<std::uint64_t> BDevStorage::getNextStoragePageNum() const
    {        
        // NOTE: in no-load mode we cannot use sparse_pair
        // therefore will calculate end page bound from the file size (absolute page number)
        if (m_flags[StorageOptions::NO_LOAD]) {
            return (m_file.size() - CONFIG_BLOCK_SIZE) / m_config.m_page_size;
        }
        
        auto page_io_id = m_sparse_pair.getNextStoragePageNum();
        if (!!m_ext_space && page_io_id) {
            // convert to absolute page number
            page_io_id = m_ext_space.getAbsolute(*page_io_id);
        }
        return page_io_id;
    }

    std::optional<StateNumType> BDevStorage::getMaxExtStateNum() const
    {
        if (m_access_type == AccessType::READ_ONLY) {
            // no synchronization required in read-only mode
            return std::nullopt;
        }
        // synchronize to the same state as the DRAM IO
        return getMaxStateNum();
    }
    
}