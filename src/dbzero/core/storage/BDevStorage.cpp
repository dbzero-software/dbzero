// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "BDevStorage.hpp"
#include "SparseIndexQuery.hpp"
#include "SparsePairQuery.hpp"
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/MetaAllocator.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>
#include <dbzero/core/memory/utils.hpp>
#include "copy_prefix.hpp"
#include <numeric>
#include <algorithm>

namespace db0

{

    o_prefix_config::o_prefix_config(std::uint32_t block_size, std::uint32_t page_size,
        std::uint32_t dram_page_size, std::uint32_t page_io_step_size,
        std::uint32_t descriptor_page_size, std::uint64_t desc_io_head)
        : m_block_size(block_size)
        , m_page_size(page_size)
        , m_dram_page_size(dram_page_size)
        , m_page_io_step_size(page_io_step_size)
        , m_descriptor_page_size(descriptor_page_size)
        , m_desc_io_head(desc_io_head)
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

    Address getRootSparsePairAddress(const DRAM_IOStream &dram_io)
    {
        return dram_io.getDRAMPair().second->firstAlloc();
    }

    StorageOptions normalizeOptions(StorageOptions options, const o_prefix_config &config)
    {
        if (!options.m_storage_slab_bucketing) {
            auto slot_size = static_cast<std::uint64_t>(config.m_page_size) * (1ull << 24);
            auto bucketing = MetaAllocator::getStorageSlabBucketingFunction(0, config.m_page_size, slot_size);
            options.m_storage_slab_bucketing = [bucketing](std::uint64_t address) {
                return bucketing(address);
            };
            options.m_storage_slab_bucket = [bucketing, page_size = config.m_page_size](std::uint64_t address) {
                return bucketing.getBucket(address, page_size);
            };
        }
        return options;
    }

    MappingPolicy getOpenMetaMappingPolicy(const StorageOptions &options, StorageFlags flags)
    {
        return flags[StorageFlagOption::NO_LOAD]
            ? MappingPolicy::lazy
            : options.m_meta_mapping_policy;
    }
    
    void appendSparsePairManagerChangeLog(BDevStorage::DRAM_ChangeLogStreamT &changelog_io,
        std::vector<std::uint64_t> &&page_nums, StateNumType state_num)
    {
        std::sort(page_nums.begin(), page_nums.end());
        ChangeLogData cl_data;
        for (auto page_num: page_nums) {
            cl_data.m_rle_builder.append(page_num, false);
        }
        changelog_io.appendChangeLog(std::move(cl_data), state_num);
    }

    template <typename CallbackT>
    void scanChangeLogs(BDevStorage::DRAM_ChangeLogStreamT &changelog_io, CallbackT callback,
        StateNumType begin_state = 0, std::optional<StateNumType> end_state = std::nullopt)
    {
        auto reader = changelog_io.getStreamReader();
        while (auto change_log = reader.readChangeLogChunk()) {
            auto state_num = change_log->m_state_num;
            if (end_state && state_num >= *end_state) {
                break;
            }
            if (state_num < begin_state) {
                continue;
            }
            callback(*change_log);
        }
    }

    template <typename ChangeLogStreamT>
    void setChangeLogTail(ChangeLogStreamT &changelog_io)
    {
        auto reader = changelog_io.getStreamReader();
        while (reader.readChangeLogChunk()) {
        }
    }

    BDevStorage::BDevStorage(const std::string &file_name, AccessType access_type, LockFlags lock_flags,
        std::optional<std::size_t> meta_io_step_size, StorageFlags flags, StorageOptions options)
        : BaseStorage(access_type, flags)
        , m_file(file_name, access_type, lock_flags)
        , m_config(readConfig())
        , m_dram_changelog_io(getChangeLogIOStream<DRAM_ChangeLogStreamT>(
            m_config.m_dram_changelog_io_offset, access_type)
        )
        , m_desc_changelog_io(getChangeLogIOStream<DRAM_ChangeLogStreamT>(
            m_config.m_desc_changelog_io_offset, access_type)
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
        , m_root_sparse_pair(m_dram_io.getDRAMPair(), access_type, getRootSparsePairAddress(m_dram_io), flags)
        , m_ext_dram_changelog_io(tryGetChangeLogIOStream<DRAM_ChangeLogStreamT>(
            m_config.m_ext_dram_changelog_io_offset, access_type)
        )
        , m_ext_dram_io(initExt(tryGetDRAMIOStream(
            m_config.m_ext_dram_io_offset, m_config.m_ext_dram_page_size, access_type), 
            m_ext_dram_changelog_io.get(), 
            // NOTE: the NO_LOAD flag is not applicable to ext DRAM IO since it's created on-demand
            flags & ~ StorageFlags {StorageFlagOption::NO_LOAD },
            // NOTE: we synchronize up to the maximum state number from DRAM IO (in read/write mode)
            this->getMaxExtStateNum())
        )
        , m_ext_space(tryGetDRAMPair(m_ext_dram_io.get()), access_type)
        , m_options(normalizeOptions(std::move(options), m_config))
        , m_page_io(getPage_IO(getNextStoragePageNum(), m_config.m_page_io_step_size))
        , m_desc_io(getDesc_IO())
        , m_meta_space(MS_MetaSpace::create(
            m_config.m_descriptor_page_size, m_root_sparse_pair, m_desc_io))    
        , m_sparse_pair_manager(m_meta_space, access_type, flags, getOpenMetaMappingPolicy(m_options, flags))
#ifndef NDEBUG
        , m_data_mirror(m_config.m_page_size)
#endif
    {
        if (m_access_type == AccessType::READ_WRITE && m_flags.test(StorageFlagOption::NO_LOAD)) {
            THROWF(db0::IOException) << "Cannot open prefix in READ_WRITE mode with NO_LOAD option";
        }
        
        // in read-only mode need to refresh in order to retrieve a consitent DRAM state
        // since other process might be actively modifying the underlying file
        if (m_access_type == AccessType::READ_ONLY) {
            if (m_flags.test(StorageFlagOption::NO_LOAD)) {
                setChangeLogTail(m_dram_changelog_io);                
                setChangeLogTail(m_dp_changelog_io);
                if (m_ext_dram_changelog_io) {
                    setChangeLogTail(*m_ext_dram_changelog_io);
                }
            } else {
                refresh();
            }
        }
        
        // NOTE: since the desc-changelog is not required for the initial load
        // (descriptor pages are loaded according based on index from the root_sparse_pair)
        // we need to advance its position to tail without any initial processing
        setChangeLogTail(m_desc_changelog_io);
    }
    
    BDevStorage::~BDevStorage()
    {
    }
    
    DRAM_IOStream BDevStorage::init(DRAM_IOStream &&dram_io, DRAM_ChangeLogStreamT &dram_change_log, StorageFlags flags)
    {
        if (!flags[StorageFlagOption::NO_LOAD]) {
            dram_io.load(dram_change_log);
        }        
        return std::move(dram_io);
    }
    
    std::unique_ptr<DRAM_IOStream> BDevStorage::initExt(std::unique_ptr<DRAM_IOStream> &&dram_io,
        DRAM_ChangeLogStreamT *dram_change_log, StorageFlags flags, std::optional<StateNumType> max_state_num)
    {
        if (dram_io && !flags[StorageFlagOption::NO_LOAD]) {
            assert(dram_change_log);
            dram_io->load(*dram_change_log, max_state_num);
        }
        return std::move(dram_io);
    }
    
    MetaIOStream BDevStorage::init(MetaIOStream &&io, StorageFlags flags)
    {
        if (!flags[StorageFlagOption::NO_LOAD]) {
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
        if (config.m_version != o_prefix_config::DB0_VERSION) {
            THROWF(db0::IOException) << "Unsupported dbzero file version: " << config.m_version
                << ", expected: " << o_prefix_config::DB0_VERSION;
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

    std::uint32_t getDiffIOStepSize(std::uint32_t block_size, std::uint32_t page_size,
        std::optional<std::size_t> step_size_hint)
    {
        auto step_size = getPageIOStepSize(block_size, step_size_hint);
        auto block_capacity = block_size / page_size;
        if (block_capacity * step_size < 2) {
            step_size = (2 + block_capacity - 1) / block_capacity;
        }
        return step_size;
    }

    std::uint64_t alignStorageAddress(std::uint64_t address, std::uint32_t page_size, std::uint64_t header_size)
    {
        if (address <= header_size) {
            return header_size;
        }
        auto rel_address = address - header_size;
        auto rel_pages = (rel_address + page_size - 1) / page_size;
        return header_size + rel_pages * page_size;
    }

    std::uint32_t getDesc_IOStride(std::uint32_t page_size, std::uint32_t descriptor_page_size,
        std::uint32_t stride_hint = 64)
    {
        if (descriptor_page_size < page_size || descriptor_page_size % page_size != 0) {
            THROWF(db0::InternalException)
                << "Descriptor page size must be a multiple of storage page size";
        }
        auto page_ratio = descriptor_page_size / page_size;
        if (stride_hint <= 1) {
            return page_ratio + 1;
        }
        auto descriptor_capacity = (stride_hint - 1 + page_ratio - 1) / page_ratio;
        return descriptor_capacity * page_ratio + 1;
    }

    void createDesc_IO(CFile &file, o_prefix_config &config, std::vector<char> &config_buffer,
        std::function<std::uint64_t()> tail_function, std::uint32_t descriptor_stream_stride,
        std::uint64_t config_block_size)
    {
        auto block_capacity = config.m_block_size / config.m_page_size;
        auto initial_page_io_address = alignStorageAddress(tail_function(), config.m_page_size, config_block_size);

        // Open a temporary page_io cursor after the block-backed streams that
        // were already materialized during prefix creation.
        auto page_io = Diff_IO(
            config_block_size, file, config.m_page_size, config.m_block_size,
            initial_page_io_address, block_capacity, config.m_page_io_step_size,
            tail_function, config.m_page_io_step_size - 1);

        // Reserve the descriptor RandomIO stream control chunk on top of
        // page_io and write the initial empty stream control page.
        RandomIO_Stream desc_io(page_io, descriptor_stream_stride, config.m_descriptor_page_size);
        desc_io.flush();

        // Flush the backing page_io so the descriptor stream control page is
        // present before the config starts pointing at it.
        page_io.flush();

        // Persist the fixed descriptor stream head in the prefix config.
        config.m_desc_io_head = desc_io.getHeadPageNum();
        file.write(0, config_block_size, config_buffer.data());
    }
    
    void BDevStorage::create(const std::string &file_name, std::optional<std::size_t> page_size,
        std::uint32_t dram_page_size_hint, std::optional<std::size_t> step_size_hint,
        std::optional<std::size_t> descriptor_page_size)
    {
        if (!page_size) {
            page_size = DEFAULT_PAGE_SIZE;
        }
        if (!descriptor_page_size) {
            descriptor_page_size = 16u << 10;
        }
        auto descriptor_stream_stride = getDesc_IOStride(*page_size, *descriptor_page_size);
        
        std::vector<char> buffer(CONFIG_BLOCK_SIZE);
        // calculate block size to be page aligned and sufficient to fit a single sparse index node
        auto min_block_size = dram_page_size_hint + 
            BlockIOStream::sizeOfHeaders(DRAM_IOStream::ENABLE_CHECKSUMS) + DRAM_IOStream::sizeOfHeader();
        // page-align block size
        auto page_alignment = std::lcm(*page_size, *descriptor_page_size);
        auto block_size = (min_block_size + page_alignment - 1) / page_alignment * page_alignment;
        // adjust DRAM page size to fit the block
        auto dram_page_size = block_size - BlockIOStream::sizeOfHeaders(DRAM_IOStream::ENABLE_CHECKSUMS) - 
            DRAM_IOStream::sizeOfHeader();
        auto page_io_step_size = getDiffIOStepSize(block_size, *page_size, step_size_hint);
        auto block_capacity = block_size / *page_size;
        auto min_descriptor_step_size = static_cast<std::uint32_t>(
            (descriptor_stream_stride + block_capacity - 1) / block_capacity);
        page_io_step_size = std::max(page_io_step_size, min_descriptor_step_size);

        // create a new config using placement new
        auto config = new (buffer.data()) o_prefix_config(
            block_size, *page_size, dram_page_size, page_io_step_size, *descriptor_page_size
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
        config->m_desc_changelog_io_offset = next_block_offset();
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
            DRAM_ChangeLogStreamT *desc_changelog_io_ptr = nullptr;
            DRAM_IOStream *dram_io_ptr = nullptr;
            std::unique_ptr<DRAM_ChangeLogStreamT> ext_dram_changelog_io_ptr = nullptr;
            std::unique_ptr<DRAM_IOStream> ext_dram_io_ptr = nullptr;
            
            auto tail_function = [&]()
            {
                assert(dram_io_ptr && dram_changelog_io_ptr);                
                // take max from the underlying I/O streams
                assert(desc_changelog_io_ptr);
                auto result = std::max(offset, std::max(
                    std::max(dram_io_ptr->tail(), dram_changelog_io_ptr->tail()),
                    desc_changelog_io_ptr->tail()));
                if (ext_dram_io_ptr && ext_dram_changelog_io_ptr) {
                    result = std::max(result, std::max(ext_dram_io_ptr->tail(), ext_dram_changelog_io_ptr->tail()));
                }
                return result;
            };
            
            auto dram_changelog_io = DRAM_ChangeLogStreamT(file, config->m_dram_changelog_io_offset, config->m_block_size,
                tail_function, AccessType::READ_WRITE);
            dram_changelog_io_ptr = &dram_changelog_io;
            auto desc_changelog_io = DRAM_ChangeLogStreamT(file, config->m_desc_changelog_io_offset, config->m_block_size,
                tail_function, AccessType::READ_WRITE);
            desc_changelog_io_ptr = &desc_changelog_io;
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
            desc_changelog_io.close();
            
            // create then flush the extension space
            if (has_ext_dram_io) {
                assert(ext_dram_io_ptr && ext_dram_changelog_io_ptr);
                ExtSpace ext_space(ExtSpace::tag_create(), ext_dram_io_ptr->getDRAMPair());
                ext_dram_io_ptr->flushUpdates(max_state_num, *ext_dram_changelog_io_ptr);
                ext_dram_changelog_io_ptr->flush();
                ext_dram_io_ptr->close();
                ext_dram_changelog_io_ptr->close();
            }

            createDesc_IO(file, *config, buffer, tail_function, descriptor_stream_stride,
                CONFIG_BLOCK_SIZE);
            
            file.flush();
            file.close();
        }
    }

    Allocator::SlotId BDevStorage::getMetaSlotId(std::uint64_t page_num) const
    {
        auto address = page_num * static_cast<std::uint64_t>(m_config.m_page_size);
        return m_options.m_storage_slab_bucketing(address);
    }

    bool BDevStorage::tryFindMutation(std::uint64_t page_num, StateNumType state_num,
        StateNumType &mutation_id) const
    {
        std::shared_lock<std::shared_mutex> lock(m_mutex);
        auto *sparse_pair = m_sparse_pair_manager.tryGetExisting(getMetaSlotId(page_num));
        if (!sparse_pair) {
            return false;
        }
        return db0::tryFindMutation(
            sparse_pair->getSparseIndex(), sparse_pair->getDiffIndex(), page_num, state_num, mutation_id);
    }
    
    StateNumType BDevStorage::findMutation(std::uint64_t page_num, StateNumType state_num) const
    {
        if (state_num == 0) {
            return 0;
        }

        StateNumType result;
        std::shared_lock<std::shared_mutex> lock(m_mutex);
        auto *sparse_pair = m_sparse_pair_manager.tryGetExisting(getMetaSlotId(page_num));
        if (!sparse_pair || !db0::tryFindMutation(
            sparse_pair->getSparseIndex(), sparse_pair->getDiffIndex(), page_num, state_num, result)) {
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
        auto &manager = const_cast<SparsePairManager &>(m_sparse_pair_manager);
        SparsePairQuery<true> sparse_pair_query(m_options, m_config.m_page_size, begin_page, end_page, manager);
        
        std::byte *read_buf = reinterpret_cast<std::byte *>(buffer);
        // lookup sparse index and read physical pages
        for (; sparse_pair_query.hasNext(); ++sparse_pair_query, read_buf += m_config.m_page_size) {
            // query sparse index + diff index
            auto *sparse_pair = sparse_pair_query.currentSparsePair();
            if (!sparse_pair) {
                if (flags[AccessOptions::read]) {
                    THROWF(db0::IOException) << "BDevStorage::read: page not found: "
                        << sparse_pair_query.pageNum() << ", state: " << state_num;
                }
                std::memset(read_buf, 0, m_config.m_page_size);
                continue;
            }
            SparseIndexQuery query(
                sparse_pair->getSparseIndex(), sparse_pair->getDiffIndex(), sparse_pair_query.pageNum(), state_num);
            if (query.empty()) {
                if (flags[AccessOptions::read]) {
                    THROWF(db0::IOException) << "BDevStorage::read: page not found: "
                        << sparse_pair_query.pageNum() << ", state: " << state_num;
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
                m_page_io.applyFrom(page_io_id, read_buf, { sparse_pair_query.pageNum(), diff_state_num });
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
        SparsePairQuery<false> sparse_pair_query(
            m_options, m_config.m_page_size, begin_page, end_page, m_sparse_pair_manager);
        // write as physical pages and register with the sparse index
        for (; sparse_pair_query.hasNext(); ++sparse_pair_query, write_buf += m_config.m_page_size) {
            auto &sparse_pair = sparse_pair_query.currentOrCreateSparsePair();
            auto &sparse_index = sparse_pair.getSparseIndex();
            // look up if page has already been added in current transaction
            auto item = sparse_index.lookup(sparse_pair_query.pageNum(), state_num);
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
                sparse_index.emplace(sparse_pair_query.pageNum(), state_num, page_io_id);
                m_root_sparse_pair.recordMaxStateNum(state_num);
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
        auto slot_id = getMetaSlotId(page_num);
        auto &sparse_pair = m_sparse_pair_manager.getOrCreate(slot_id);
        // Use SparseIndexQuery to determine the current sequence length & check limits
        SparseIndexQuery query(sparse_pair.getSparseIndex(), sparse_pair.getDiffIndex(), page_num, state_num);
        // if a page has already been written as full-DP in the current transaction then
        // we cannot append as diff but need to overwrite the full page instead
        if (state_num != query.firstStateNum() && query.leftLessThan(max_len)) {
            bool is_first_page;
            // append as diff-page (NOTE: diff-writes are only appended)
            auto [page_io_id, overflow] = m_page_io.appendDiff(
                buffer, { page_num, state_num }, diff_data, &is_first_page
            );
            if (!!m_ext_space) {
                page_io_id = m_ext_space.assignRelative(page_io_id, is_first_page);
            }
            sparse_pair.getDiffIndex().insert(page_num, state_num, page_io_id, overflow);
            m_root_sparse_pair.recordMaxStateNum(state_num);
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

    std::size_t BDevStorage::getDescriptorPageSize() const {
        return m_config.m_descriptor_page_size;
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

        auto descriptor_io_was_modified = m_desc_io.modified();
        auto application_changed = m_sparse_pair_manager.commit();
        auto &meta_prefix = *m_meta_space.getMSPrefixPtr();
        auto state_num = m_root_sparse_pair.getMaxStateNum();
        auto meta_space_dirty = meta_prefix.getDirtySize() != 0;
        if (meta_space_dirty && state_num < meta_prefix.getStateNum(false)) {
            THROWF(db0::InternalException)
                << "BDevStorage::flush requires caller to register state high watermark before flushing dirty metadata"
                << "; root max state: " << state_num
                << "; metadata state: " << meta_prefix.getStateNum(false)
                << "; sparse pair manager changelog size: " << m_sparse_pair_manager.getChangeLogSize();
        }
        auto meta_space_flushed = db0::flush(meta_prefix, m_desc_io, timer.get());
        if (meta_space_flushed) {
            m_meta_space.commit(timer.get());
        }
        auto descriptor_io_modified = descriptor_io_was_modified || m_desc_io.modified() || meta_space_flushed;
        bool root_metadata_changed = false;
        if (descriptor_io_modified) {
            if (state_num == 0) {
                THROWF(db0::InternalException)
                    << "BDevStorage::flush requires registered state high watermark before flushing descriptor metadata";
            }
        }
        auto root_change_log_size = m_root_sparse_pair.getChangeLogSize();

        // check if there're any modifications to be flushed
        if (!application_changed && !root_metadata_changed && root_change_log_size == 0) {
            if (descriptor_io_modified) {
                m_desc_io.flush();
                m_page_io.flush();
                m_file.fsync();
                return false;
            }
            // no modifications to be flushed
            return false;
        }        
        
        // save metadata checkpoints before making any updates to the managed streams
        // NOTE: the checkpoint is only saved after exceeding specific threshold of updates in the managed streams
        if (application_changed) {
            m_meta_io.checkAndAppend(state_num);
            m_meta_io.flush();
        }
        
        m_desc_io.flush();
        m_page_io.flush();
        // Extract & flush sparse index change log first (on condition of any updates)
        // we also need to collect the end storage page number, possibly relative (sentinel)
        bool is_first = false;
        auto end_page_io_page_num = m_page_io.getEndPageNum(&is_first);
        if (!!m_ext_space) {
            // convert to relative page number
            end_page_io_page_num = m_ext_space.assignRelative(end_page_io_page_num, is_first);
        }
        
        if (application_changed) {
            auto changed_page_nums = m_sparse_pair_manager.extractChangeLogPages();
            appendSparsePairManagerChangeLog(m_desc_changelog_io, std::move(changed_page_nums), state_num);
            m_root_sparse_pair.recordMaxStateNum(state_num);
            m_root_sparse_pair.recordNextStoragePageNum(end_page_io_page_num);
        }
        m_dram_io.flushUpdates(state_num, m_dram_changelog_io);
        m_desc_io.flush();
        m_page_io.flush();
        // Flush ext streams (if existing)
        flushExt(state_num);
        // NOTE: fsync has stronger guarantees than flush in a multi-process environments
        m_file.fsync();
        // flush changelog AFTER all updates from all other streams have been flushed        
        m_desc_changelog_io.flush();
        m_dram_changelog_io.flush();
        // the last fsync finalizes the commit
        m_file.fsync();
        
        // commit to collect future updates correctly        
        m_sparse_pair_manager.commit();
        m_root_sparse_pair.commit();
        return application_changed;
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
        m_desc_changelog_io.close();
        m_dp_changelog_io.close(); 
        m_meta_io.close();
        m_desc_io.close();
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
        auto block_size = m_config.m_block_size;
        return std::make_unique<DRAM_IOStream>(m_file, first_block_pos, block_size, 
            getTailFunction(), access_type, dram_page_size);
    }
    
    std::uint64_t BDevStorage::tail() const
    {
        // take max from the 4 underlying I/O streams
        auto result = std::max(m_dram_io.tail(), m_meta_io.tail());
        result = std::max(result, m_dram_changelog_io.tail());
        result = std::max(result, m_desc_changelog_io.tail());
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
        auto descriptor_end_page_num = m_config.m_desc_io_head
            + getDesc_IOStride(m_config.m_page_size, m_config.m_descriptor_page_size);
        if (!next_page_hint && m_flags[StorageFlagOption::NO_LOAD]) {
            next_page_hint = (m_file.size() - CONFIG_BLOCK_SIZE) / m_config.m_page_size;
        }
        if (!next_page_hint || *next_page_hint < descriptor_end_page_num) {
            next_page_hint = descriptor_end_page_num;
        }
        auto tail_function = getPageIOTailFunction();
        auto block_tail_address = alignStorageAddress(m_file.size(), m_config.m_page_size, CONFIG_BLOCK_SIZE);
        auto block_tail_page_num = (block_tail_address - CONFIG_BLOCK_SIZE) / m_config.m_page_size;
        if (!next_page_hint || *next_page_hint < block_tail_page_num) {
            next_page_hint = block_tail_page_num;
        }
        auto initial_tail_address = next_page_hint ? 0 : tail_function();
        return getDiff_IO(
            next_page_hint, m_config.m_page_size, step_size, tail_function, initial_tail_address);
    }

    RandomIO_Stream BDevStorage::getDesc_IO()
    {
        return {
            m_page_io, m_config.m_desc_io_head,
            getDesc_IOStride(m_config.m_page_size, m_config.m_descriptor_page_size),
            m_access_type, m_config.m_descriptor_page_size
        };
    }

    Diff_IO BDevStorage::getDiff_IO(std::optional<std::uint64_t> next_page_hint, std::uint32_t page_size,
        std::uint32_t step_size, std::function<std::uint64_t()> tail_function, std::uint64_t initial_tail_address)
    {
        auto block_capacity = m_config.m_block_size / page_size;
        
        std::optional<std::uint32_t> block_num;
        std::uint64_t address = 0;
        std::uint32_t page_count = 0;
        
        if (next_page_hint) {
            auto block_id = (*next_page_hint * page_size) / m_config.m_block_size;
            address = CONFIG_BLOCK_SIZE + block_id * m_config.m_block_size;
            page_count = static_cast<std::uint32_t>(*next_page_hint % block_capacity);
            
            // position at the end of the last existing block
            if (page_count == 0) {
                address -= m_config.m_block_size;
                page_count = block_capacity;
                --block_id;
            }
            block_num = static_cast<std::uint32_t>(block_id % step_size);
        } else {
            address = alignStorageAddress(initial_tail_address, page_size, CONFIG_BLOCK_SIZE);
            // Seed the cursor as a full previous block. The first real write
            // will call allocateNextBlock(), which consults tail_function()
            // after all BDevStorage streams have been constructed.
            page_count = block_capacity;
            block_num = step_size - 1;
        }

        // NOTE: block num is unknown in this case
        return { CONFIG_BLOCK_SIZE, m_file, page_size, m_config.m_block_size, address, page_count,
            step_size, tail_function, block_num
        };
    }
    
    std::uint32_t BDevStorage::getMaxStateNum() const {
        return m_root_sparse_pair.getMaxStateNum();
    }
    
    std::function<std::uint64_t()> BDevStorage::getTailFunction() const
    {
        return [this]() {
            return this->tail();
        };
    }

    std::uint64_t BDevStorage::blockIOTail() const
    {
        auto result = std::max(m_dram_io.tail(), m_meta_io.tail());
        result = std::max(result, m_dram_changelog_io.tail());
        result = std::max(result, m_desc_changelog_io.tail());
        result = std::max(result, m_dp_changelog_io.tail());
        if (m_ext_dram_io) {
            assert(m_ext_dram_changelog_io);
            result = std::max(result, m_ext_dram_io->tail());
            result = std::max(result, m_ext_dram_changelog_io->tail());
        }
        return result;
    }

    std::function<std::uint64_t()> BDevStorage::getPageIOTailFunction() const
    {
        return [this]() -> std::uint64_t {
            return blockIOTail();
        };
    }
    
    bool BDevStorage::beginRefresh()
    {
        if (m_access_type != AccessType::READ_ONLY) {
            THROWF(db0::IOException) << "BDevStorage::refresh allowed only in read-only mode";
        }
        if (!m_refresh_pending) {
            // The main DRAM changelog is the transaction marker. Descriptor and
            // extension changelogs are refreshed after observing a new DRAM
            // transaction in completeRefresh().
            m_refresh_pending = m_dram_changelog_io.refresh();
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
            auto desc_changelog_io_pos = m_desc_changelog_io.getStreamPos();
            std::pair<std::uint64_t, std::uint64_t> ext_dram_changelog_io_pos;
            if (!!m_ext_space) {
                assert(m_ext_dram_changelog_io);
                ext_dram_changelog_io_pos = m_ext_dram_changelog_io->getStreamPos();
            }
            auto dp_changelog_io_pos = m_dp_changelog_io.getStreamPos();
            // reverts streams to previous positions
            auto revert_streams = [&]() {
                m_dram_changelog_io.setStreamPos(dram_changelog_io_pos);
                m_desc_changelog_io.setStreamPos(desc_changelog_io_pos);
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
                    m_refresh_pending = false;
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
                if (!!m_ext_space && (!ext_dram_state_num || *ext_dram_state_num != *dram_state_num)) {
                    is_consistent = false;
                }
                
                if (!is_consistent) {
                    // must continue with the refresh until getting a consistent state
                    m_dram_changelog_io.refresh();
                    continue;
                }
                
                m_desc_changelog_io.refresh();
                auto desc_changelog_io_end = m_desc_changelog_io.getStreamPos();

                // Descriptor changelog entries are stored separately from DRAM IO.
                // Reload pages after restoring stream position because page reload
                // may consult stream tails.
                std::vector<std::uint64_t> updated_desc_pages;
                m_desc_changelog_io.setStreamPos(desc_changelog_io_pos);
                auto desc_state_is_consistent = true;
                // NOTE: descriptor pages don't report to the on_page_updated callback
                scanChangeLogs(m_desc_changelog_io,
                    [&](const DRAM_ChangeLogT &change_log) {
                        if (change_log.m_state_num > *dram_state_num) {
                            desc_state_is_consistent = false;
                            return;
                        }
                        for (auto entry: change_log) {
                            updated_desc_pages.push_back(entry);
                        }
                    }
                );
                if (!desc_state_is_consistent) {
                    m_desc_changelog_io.setStreamPos(desc_changelog_io_pos);
                    continue;
                }
                m_desc_changelog_io.setStreamPos(desc_changelog_io_end);

                // Root metadata is part of DRAM IO. Refresh it before applying
                // sparse-pair-manager changelog entries so slot detaches see
                // the latest MetaSpace allocator state.
                m_flags = m_flags & ~StorageFlags { StorageFlagOption::NO_LOAD };
                m_root_sparse_pair.refresh();
                // refresh the updated descriptor pages
                m_sparse_pair_manager.refreshPages(updated_desc_pages);

                m_dp_changelog_io.refresh();
            } catch (db0::IOException &) {
                revert_streams();
                // NOTE: this may be a temporary problem, refresh needs repeating
                m_refresh_pending = false;
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
        std::uint64_t sparse_pair_size = 0;
        auto &manager = const_cast<SparsePairManager &>(m_sparse_pair_manager);
        manager.forCachedPairs([&](Allocator::SlotId, PlainSparsePair &sparse_pair) {
            sparse_pair_size += sparse_pair.size();
        });
        callback("dp_size_total", sparse_pair_size * m_page_io.getPageSize());
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
        std::function<void(const DP_ChangeLogT &)> callback) const
    {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        auto &dp_changelog_io = const_cast<DP_ChangeLogStreamT &>(m_dp_changelog_io);
        DP_ChangeLogStreamT::State dp_state;
        dp_changelog_io.saveState(dp_state);
        
        {
            std::vector<char> buf;
            // try locating the nearest meta-log entry to position the dp-changelog
            auto meta_log_ptr = m_meta_io.lowerBound(begin_state, buf);
            if (meta_log_ptr) {
                // the 1st meta-item is associated with the dp_change_log
                auto &item = *meta_log_ptr->getMetaItems().begin();
                dp_changelog_io.setStreamPos(item.m_address, item.m_stream_pos);
            } else {
                // must scan starting from the beginning (head)
                dp_changelog_io.setStreamPosHead();
            }
        }

        try {
            std::map<StateNumType, std::vector<std::uint64_t> > change_log_pages;
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
                if (state_num >= begin_state && change_log->begin() != change_log->end()) {
                    auto &page_nums = change_log_pages[state_num];
                    for (auto page_num: *change_log) {
                        page_nums.push_back(page_num);
                    }
                }
            }
            
            std::vector<char> buffer;
            for (auto &[state_num, page_nums]: change_log_pages) {
                if (page_nums.empty()) {
                    continue;
                }
                std::sort(page_nums.begin(), page_nums.end());
                ChangeLogData data(page_nums, false, false, true);
                auto size_of = DP_ChangeLogT::measure(data, state_num, 0);
                buffer.resize(size_of);
                auto &dp_change_log = DP_ChangeLogT::__new(buffer.data(), data, state_num, 0);
                callback(dp_change_log);
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
    
    void loadRootSparsePairForNoLoadCopy(DRAM_IOStream &dram_io,
        BDevStorage::DRAM_ChangeLogStreamT &dram_changelog_io, SparsePair &root_sparse_pair,
        AccessType access_type, StorageFlags flags)
    {
        dram_changelog_io.setStreamPosHead();
        dram_io.setStreamPosHead();
        dram_io.load(dram_changelog_io);
        root_sparse_pair.refresh();
    }
    
    void BDevStorage::copyTo(BDevStorage &out)
    {
        if (!out.m_ext_space) {
            THROWF(db0::IOException) << "BDevStorage::copyTo: destination storage must have ext-space initialized";
        }
        
        if (m_flags[StorageFlagOption::NO_LOAD]) {
            auto dram_changelog_io = getChangeLogIOStream<DRAM_ChangeLogStreamT>(
                m_config.m_dram_changelog_io_offset, m_access_type);
            auto dram_io = getDRAMIOStream(
                m_config.m_dram_io_offset, m_config.m_dram_page_size, m_access_type);
            loadRootSparsePairForNoLoadCopy(
                dram_io, dram_changelog_io, m_root_sparse_pair, m_access_type, m_flags);
        }
        auto copy_state_num = m_root_sparse_pair.getMaxStateNum();
        auto writer = out.m_dram_changelog_io.getStreamWriter();
        auto maybe_max_state_num = copyDRAM_IO(
            m_dram_io, m_dram_changelog_io, out.m_dram_io, writer, copy_state_num);
        if (!maybe_max_state_num) {
            // nothing to copy
            return;
        }

        auto max_state_num = *maybe_max_state_num;
        auto src_page_tail = getNextStoragePageNum();
        // copy up to the max_state_num (inclusive)
        auto dp_header = copyDPStream(m_dp_changelog_io, out.m_dp_changelog_io, max_state_num);        
        writer.flush();
        
        out.m_dram_changelog_io.setStreamPosHead();
        out.m_dram_io.setStreamPosHead();
        out.m_dram_io.load(out.m_dram_changelog_io, max_state_num);
        out.m_root_sparse_pair.refresh();

        out.m_ext_space.refresh();
        out.m_ext_space.clearMappings();

        out.m_page_io.setAtTail();
        std::uint64_t end_page_num = 0;
        if (dp_header) {
            end_page_num = dp_header->m_end_storage_page_num;
        }
        if (!!m_ext_space && end_page_num != 0) {
            end_page_num = m_ext_space.getAbsolute(end_page_num);
        }
        if (src_page_tail) {
            end_page_num = std::max(end_page_num, *src_page_tail);
        }
        copyPageIO(m_page_io, m_ext_space, out.m_page_io, end_page_num, out.m_ext_space);
                
        // NOTE: meta_is stream can't be copied since it's structure depends on the managed streams
        // NOTE: for simplicity we don't generate the entire meta-io, just save the last checkpoint
        out.m_meta_io.checkAndAppend(max_state_num);
        out.m_meta_io.flush();

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
        auto page_io_id = m_root_sparse_pair.getNextStoragePageNum();
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
        // Synchronize ext-space to the root metadata state. DP changelog entries
        // may be absent when a transaction only updates sparse-pair metadata.
        return m_root_sparse_pair.getMaxStateNum();
    }
    
}
