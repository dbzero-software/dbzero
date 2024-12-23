#include "BDevStorage.hpp"
#include <unordered_set>
#include <unordered_map>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>

namespace db0

{
    
    BlockIOStream readAll(BlockIOStream &&io)
    {
       // FIXME: implement WAL processing
        std::vector<char> buf;
        for (;;) {
            if (!io.readChunk(buf)) {
                break;
            }
        }
        return std::move(io);
    }

    o_prefix_config::o_prefix_config(std::uint32_t block_size, std::uint32_t page_size, std::uint32_t dram_page_size)
        : m_block_size(block_size)
        , m_page_size(page_size)
        , m_dram_page_size(dram_page_size)
    {
    }
    
    BDevStorage::BDevStorage(const std::string &file_name, AccessType access_type, LockFlags lock_flags)
        : BaseStorage(access_type)
        , m_file(file_name, access_type, lock_flags)
        , m_config(readConfig())
        , m_dram_changelog_io(getChangeLogIOStream(m_config.m_dram_changelog_io_offset, access_type))
        , m_dp_changelog_io(init(getChangeLogIOStream(m_config.m_dp_changelog_io_offset, access_type)))
        , m_dram_io(init(getDRAMIOStream(m_config.m_dram_io_offset, m_config.m_dram_page_size, access_type), m_dram_changelog_io))
        , m_sparse_index(m_dram_io.getDRAMPair(), access_type)
        , m_wal_io(readAll(getBlockIOStream(m_config.m_wal_offset, AccessType::READ_ONLY)))
        , m_page_io(getPage_IO(m_sparse_index.getNextStoragePageNum(), access_type))
        // mark empty until retrieving actual data
        , m_empty(m_dram_io.empty())
    {
        if (m_access_type == AccessType::READ_ONLY) {
            refresh();
        }
    }
    
    BDevStorage::~BDevStorage()
    {
    }

    DRAM_IOStream BDevStorage::init(DRAM_IOStream &&dram_io, ChangeLogIOStream &change_log)
    {
        if (dram_io.getAccessType() == AccessType::READ_WRITE) {
            // simply exhaust the change-log stream
            while (change_log.readChangeLogChunk());
        } else {
            // apply all changes from the change-log
            dram_io.applyChanges(change_log);
        }
        return std::move(dram_io);
    }

    ChangeLogIOStream BDevStorage::init(ChangeLogIOStream &&io)
    {
        while (io.readChangeLogChunk());
        return std::move(io);
    }

    o_prefix_config BDevStorage::readConfig() const
    {
        std::vector<char> buffer(CONFIG_BLOCK_SIZE);
        m_file.read(0, buffer.size(), buffer.data());
        auto &config = o_prefix_config::__const_ref(buffer.data());
        if (config.m_magic != o_prefix_config::DB0_MAGIC) {
            THROWF(db0::IOException) << "Invalid DB0 file: " << m_file.getName();
        }
        return config;
    }
    
    void BDevStorage::create(const std::string &file_name, std::optional<std::size_t> page_size,
        std::uint32_t dram_page_size_hint)
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
        auto config = new (buffer.data()) o_prefix_config(block_size, *page_size, dram_page_size);

        auto offset = CONFIG_BLOCK_SIZE;
        auto next_block_offset = [&]() 
        {
            auto result = offset;
            offset += block_size;
            return result;
        };        

        // cofigure offsets for all inner streams (even though they have not been materialized yet)
        config->m_dram_io_offset = next_block_offset();
        config->m_wal_offset = next_block_offset();
        config->m_dram_changelog_io_offset = next_block_offset();
        config->m_dp_changelog_io_offset = next_block_offset();
        
        CFile::create(file_name, buffer);
    }
    
    bool BDevStorage::tryFindMutation(std::uint64_t page_num, std::uint64_t state_num,
        std::uint64_t &mutation_id) const
    {
        return tryFindMutationImpl(page_num, state_num, mutation_id);
    }
    
    bool BDevStorage::tryFindMutationImpl(std::uint64_t page_num, std::uint64_t state_num,
        std::uint64_t &mutation_id) const
    {
        auto item  = m_sparse_index.lookup(page_num, state_num);
        if (!item) {
            return false;
        }
        mutation_id = item.m_state_num;
        return true;
    }
    
    std::uint64_t BDevStorage::findMutation(std::uint64_t page_num, std::uint64_t state_num) const
    {
        std::uint64_t result;
        if (!tryFindMutationImpl(page_num, state_num, result)) {
            assert(false && "BDevStorage::findMutation: page not found");
            THROWF(db0::IOException) 
                << "BDevStorage::findMutation: page_num " << page_num << " not found, state: " << state_num;
        }
        return result;
    }
    
    void BDevStorage::read(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer,
        FlagSet<AccessOptions> flags) const
    {
        assert(state_num > 0 && "BDevStorage::read: state number must be > 0");
        assert((address % m_config.m_page_size == 0) && "BDevStorage::read: address must be page-aligned");
        assert((size % m_config.m_page_size == 0) && "BDevStorage::read: size must be page-aligned");

        if (flags[AccessOptions::write] && m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::IOException) << "BDevStorage::read: invalid write fiag to access read-only resource";
        }

        auto begin_page = address / m_config.m_page_size;
        auto end_page = begin_page + size / m_config.m_page_size;

        std::byte *read_buf = reinterpret_cast<std::byte *>(buffer);
        // lookup sparse index and read physical pages
        for (auto page_num = begin_page; page_num != end_page; ++page_num, read_buf += m_config.m_page_size) {
            auto item  = m_sparse_index.lookup(page_num, state_num);
            if (item) {
                m_page_io.read(item.m_storage_page_num, read_buf);
            } else {
                if (flags[AccessOptions::read]) {
                    THROWF(db0::IOException) << "BDevStorage::read: page not found: " << page_num << ", state: " << state_num;
                }
                 // if requested access is write-only then simply fill the misssing (new) page with 0
                memset(read_buf, 0, m_config.m_page_size);
            }
        }
    }
    
    void BDevStorage::write(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer)
    {    
        assert(state_num > 0 && "BDevStorage::write: state number must be > 0");
        assert((address % m_config.m_page_size == 0) && "BDevStorage::write: address must be page-aligned");
        assert((size % m_config.m_page_size == 0) && "BDevStorage::write: size must be page-aligned");
        
        auto begin_page = address / m_config.m_page_size;
        auto end_page = begin_page + size / m_config.m_page_size;
        
        std::byte *write_buf = reinterpret_cast<std::byte *>(buffer);
        // write as physical pages and register with the sparse index
        for (auto page_num = begin_page; page_num != end_page; ++page_num, write_buf += m_config.m_page_size) {
            // look up if page has already been added in current transaction
            auto item = m_sparse_index.lookup(page_num, state_num);
            if (item && item.m_state_num == state_num) {
                // page already added in current transaction / update in the stream
                // this may happen due to cache overflow and later modification of the same page
                m_page_io.write(item.m_storage_page_num, write_buf);
            } else {
                // append as new page
                auto storage_page_id = m_page_io.append(write_buf);
                m_sparse_index.emplace(page_num, state_num, storage_page_id);
            }
        }
    }
    
    void BDevStorage::writeDiffs(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer,
        const std::vector<std::uint16_t> &diffs) 
    {
        // FIXME: implement
    }

    std::size_t BDevStorage::getPageSize() const {
        return m_config.m_page_size;
    }
    
    bool BDevStorage::flush(ProcessTimer *parent_timer)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (parent_timer) {
            timer = std::make_unique<ProcessTimer>("BDevStorage::flush", parent_timer);
        }
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::IOException) << "BDevStorage::flush error: read-only stream";
        }
        
        // no modifications to be flushed
        if (m_sparse_index.getChangeLogSize() == 0) {
            return false;
        }
        
        // Extract & flush sparse index change log first (on condition of any updates)
        m_sparse_index.extractChangeLog(m_dp_changelog_io);
        m_dram_io.flushUpdates(m_sparse_index.getMaxStateNum(), m_dram_changelog_io);
        m_dp_changelog_io.flush();
        // flush changelog AFTER all updates from dram_io have been flushed
        m_dram_changelog_io.flush();
        m_wal_io.flush();
        m_file.flush();
        return true;
    }
    
    void BDevStorage::close()
    {        
        if (m_access_type == AccessType::READ_WRITE) {
            flush();
        }
        m_dram_io.close();
        m_dram_changelog_io.close();
        m_dp_changelog_io.close();
        m_wal_io.close();
        m_file.close();
    }
    
    BlockIOStream BDevStorage::getBlockIOStream(std::uint64_t first_block_pos, AccessType access_type) {
        return { m_file, first_block_pos, m_config.m_block_size, getTailFunction(), access_type };
    }
    
    DRAM_IOStream BDevStorage::getDRAMIOStream(std::uint64_t first_block_pos, std::uint32_t dram_page_size, AccessType access_type) {
        return { m_file, first_block_pos, m_config.m_block_size, getTailFunction(), access_type, dram_page_size };
    }

    ChangeLogIOStream BDevStorage::getChangeLogIOStream(std::uint64_t first_block_pos, AccessType access_type) {
        return { m_file, first_block_pos, m_config.m_block_size, getTailFunction(), access_type };
    }
    
    std::uint64_t BDevStorage::tail() const
    {
        // take max from the 4 underlying I/O streams
        auto result = std::max(m_dram_io.tail(), m_wal_io.tail());
        result = std::max(result, m_dram_changelog_io.tail());
        result = std::max(result, m_dp_changelog_io.tail());
        result = std::max(result, m_page_io.tail());

        return result;
    }
    
    Page_IO BDevStorage::getPage_IO(std::uint64_t next_page_hint, AccessType access_type)
    {   
        if (access_type == AccessType::READ_ONLY) {
            // return empty page IO
            return { CONFIG_BLOCK_SIZE, m_file, m_config.m_page_size };
        }

        assert(access_type == AccessType::READ_WRITE);
        auto block_id = (next_page_hint * m_config.m_page_size) / m_config.m_block_size;
        auto block_capacity = m_config.m_block_size / m_config.m_page_size;

        if (next_page_hint == 0) {
            // assign first page
            auto address = std::max(m_dram_io.tail(), m_wal_io.tail());
            address = std::max(address, m_dram_changelog_io.tail());
            address = std::max(address, m_dp_changelog_io.tail());
            return { CONFIG_BLOCK_SIZE, m_file, m_config.m_page_size, m_config.m_block_size, address, 0,
                getBlockIOTailFunction() };
        }
        
        auto address = CONFIG_BLOCK_SIZE + block_id * m_config.m_block_size;
        auto page_count = static_cast<std::uint32_t>(next_page_hint % block_capacity);
        
        // position at the end of the last existing block
        if (page_count == 0) {
            address -= m_config.m_block_size;
            page_count = block_capacity;
        }

        return { CONFIG_BLOCK_SIZE, m_file, m_config.m_page_size, m_config.m_block_size, address, page_count,
            getBlockIOTailFunction() };
    }
    
    std::uint32_t BDevStorage::getMaxStateNum() const {
        return m_empty ? 0 : m_sparse_index.getMaxStateNum();
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
            auto result = std::max(m_dram_io.tail(), m_wal_io.tail());
            result = std::max(result, m_dram_changelog_io.tail());
            result = std::max(result, m_dp_changelog_io.tail());
            return result;
        };
    }
    
    bool BDevStorage::beginRefresh()
    {
        if (m_access_type != AccessType::READ_ONLY) {
            THROWF(db0::IOException) << "BDevStorage::refresh allowed only in read-only mode";
        }        
        return m_dram_changelog_io.refresh();
    }
    
    std::uint64_t BDevStorage::completeRefresh(
        std::function<void(std::uint64_t page_num, std::uint64_t state_num)> on_page_updated)
    {
        assert(m_access_type == AccessType::READ_ONLY);
        std::uint64_t result = 0;
        // continue refreshing until all updates retrieved to guarantee a consistent state
        do {
            if (!result) {
                result = m_file.getLastModifiedTime();
            }
            if (m_dram_io.applyChanges(m_dram_changelog_io)) {
                // refresh underlying sparse index after DRAM update
                m_sparse_index.refresh();
                // clear empty flag if it was set
                m_empty = false;
            }
            m_dp_changelog_io.refresh();
            // send all page-update notifications to the provided handler
            if (on_page_updated) {
                std::uint64_t updated_state_num = 0;
                for (;;) {
                    auto dp_change_log_ptr = m_dp_changelog_io.readChangeLogChunk();
                    if (!dp_change_log_ptr) {
                        break;
                    }
                    
                    // First element from the chunk is the updated state number
                    auto it = dp_change_log_ptr->begin(), end = dp_change_log_ptr->end();
                    assert(it != end);
                    // First element in the log is the updated state number
                    assert(*it != updated_state_num);
                    updated_state_num = *it;
                    // All other elements are page numbers
                    ++it;
                    for (; it != end; ++it) {
                        on_page_updated(*it, updated_state_num);
                    }
                }
            }
            
            m_wal_io.refresh();
        }
        while (m_dram_changelog_io.refresh());        
        return result;
    }
    
    bool BDevStorage::empty() const {
        return m_empty;
    }
    
    std::uint64_t BDevStorage::getLastUpdated() const {
        return m_file.getLastModifiedTime();
    }
    
    void BDevStorage::getStats(std::function<void(const std::string &, std::uint64_t)> callback) const
    {
        callback("dram_io_rand_ops", m_dram_io.getRandOpsCount());
        callback("dram_prefix_size", m_dram_io.getDRAMPrefix().size());
        auto file_rand_ops = m_file.getRandOps();
        callback("file_rand_read_ops", file_rand_ops.first);
        callback("file_rand_write_ops", file_rand_ops.second);
        auto file_io_bytes = m_file.getIOBytes();
        callback("file_bytes_read", file_io_bytes.first);
        callback("file_bytes_written", file_io_bytes.second);
        // total size of data pages
        callback("dp_size_total", m_sparse_index.size() * m_page_io.getPageSize());
        callback("prefix_size", m_file.size());        
    }
    
#ifndef NDEBUG
    void BDevStorage::getDRAM_IOMap(std::unordered_map<std::uint64_t, DRAM_PageInfo> &io_map) const {
        m_dram_io.getDRAM_IOMap(io_map);
    }

    void BDevStorage::dramIOCheck(std::vector<DRAM_CheckResult> &check_result) const {
        m_dram_io.dramIOCheck(check_result);
    }
#endif
    
}