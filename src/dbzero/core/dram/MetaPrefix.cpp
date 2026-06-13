// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MetaPrefix.hpp"
#include <dbzero/core/memory/diff_utils.hpp>
#include <dbzero/core/storage/RandomIO_Stream.hpp>
#include <dbzero/core/storage/SparseIndexQuery.hpp>
#include <dbzero/core/storage/SparsePair.hpp>
#include <cstring>
#include <map>
#include <utility>
#include <vector>

namespace db0

{

    namespace
    {
        std::vector<std::uint64_t> collectReusableFullPageNums(const SparsePair &sparse_pair, StateNumType state_num)
        {
            std::vector<std::uint64_t> reusable_full_pages;

            bool have_page = false;
            std::uint64_t current_page_num = 0;
            std::size_t retained_count = 0;
            SI_Item oldest_retained;
            SI_Item newest_retained;

            auto retain = [&](const SI_Item &item) {
                if (!have_page || item.m_page_num != current_page_num) {
                    have_page = true;
                    current_page_num = item.m_page_num;
                    retained_count = 0;
                }

                if (retained_count == 0) {
                    oldest_retained = item;
                    retained_count = 1;
                    return;
                }
                if (retained_count == 1) {
                    newest_retained = item;
                    retained_count = 2;
                    return;
                }

                reusable_full_pages.push_back(oldest_retained.m_storage_page_num);
                oldest_retained = newest_retained;
                newest_retained = item;
            };

            for (auto it = sparse_pair.getSparseIndex().cbegin(); !it.is_end(); ++it) {
                auto item = *it;
                if (!!item && item.m_page_num != 0 && item.m_storage_page_num != 0 && item.m_state_num <= state_num) {
                    retain(item);
                }
            }
            return reusable_full_pages;
        }
    }

    MetaPrefix::MetaPrefix(std::size_t page_size, SparsePair &parent_index)
        : DRAM_Prefix(page_size)
        , m_parent_index(parent_index)
    {
    }

    void load(MetaPrefix &prefix, RandomIO_Stream &page_io)
    {        
        // Collect unique page numbers first (there might more than one state number available per page)
        std::uint64_t last_page_num = 0;
        std::vector<std::uint64_t> page_nums;
        for (auto it = prefix.m_parent_index.getSparseIndex().cbegin(); !it.is_end(); ++it) {
            auto item = *it;
            if (!!item && item.m_page_num != 0 && item.m_page_num != last_page_num) {
                page_nums.push_back(item.m_page_num);
                last_page_num = item.m_page_num;
            }
        }
        db0::load(prefix, page_io, page_nums);
    }

    struct Load_OP
    {
        std::uint64_t m_storage_page_num;
        // target buffer
        void *m_buffer;
    };

    struct LoadDiff_OP
    {
        std::uint64_t m_storage_page_num;
        std::uint64_t m_page_num;
        StateNumType m_diff_state_num;
        // target buffer
        void *m_buffer;
    };
    
    // fetch a single page from storage
    bool fetchPage(MetaPrefix &prefix, RandomIO_Stream &page_io, std::uint64_t page_num, StateNumType state_num,
        void *buffer)
    {
        SparseIndexQuery query(prefix.m_parent_index.getSparseIndex(), prefix.m_parent_index.getDiffIndex(),
            page_num, state_num);
        if (query.empty()) {
            return false;
        }

        auto storage_page_num = query.first();
        if (storage_page_num) {
            page_io.readRandom(storage_page_num, buffer);
        } else {
            std::memset(buffer, 0, prefix.getPageSize());
        }

        StateNumType diff_state_num = 0;
        while (query.next(diff_state_num, storage_page_num)) {
            page_io.applyFrom(storage_page_num, buffer, { page_num, diff_state_num });
        }
        return true;
    }

    void load(MetaPrefix &prefix, RandomIO_Stream &page_io, const std::vector<std::uint64_t> &page_nums,
        DRAM_Allocator::Updater &&updater)
    {
        load(prefix, page_io, page_nums.data(), page_nums.data() + page_nums.size(), std::move(updater));
    }

    void load(MetaPrefix &prefix, RandomIO_Stream &page_io, const std::uint64_t *page_num,
        const std::uint64_t *end, DRAM_Allocator::Updater &&updater)
    {
        auto state_num = prefix.getStateNum(false);
        // For I/O performace we first determine the operations and then execute ordered for better locality
        std::vector<Load_OP> load_ops;
        std::vector<LoadDiff_OP> load_diff_ops;

        auto &sparse_index = prefix.m_parent_index.getSparseIndex();
        auto &diff_index = prefix.m_parent_index.getDiffIndex();
        for (;page_num != end; ++page_num) {
            SparseIndexQuery query(sparse_index, diff_index, *page_num, state_num);
            if (query.empty()) {
                continue;
            }

            auto page_buf = prefix.update(*page_num, false);
            if (!!updater) {
                updater(*page_num * prefix.getPageSize());
            }
            auto storage_page_num = query.first();
            if (storage_page_num) {
                load_ops.push_back(Load_OP { storage_page_num, page_buf });
            } else {
                std::memset(page_buf, 0, prefix.getPageSize());
            }

            StateNumType diff_state_num = 0;
            while (query.next(diff_state_num, storage_page_num)) {
                load_diff_ops.push_back(LoadDiff_OP { storage_page_num, *page_num, diff_state_num, page_buf });
            }
        }

        // sort both ops-buffers by storage page number for better locality
        std::sort(load_ops.begin(), load_ops.end(), [](const Load_OP &a, const Load_OP &b) {
            return a.m_storage_page_num < b.m_storage_page_num;
        });

        // Load full pages first
        for (const auto &op: load_ops) {
            page_io.readRandom(op.m_storage_page_num, op.m_buffer);
        }
        
        // Apply diffs next
        std::sort(load_diff_ops.begin(), load_diff_ops.end(), [](const LoadDiff_OP &a, const LoadDiff_OP &b) {
            return a.m_storage_page_num < b.m_storage_page_num;
        });
        for (const auto &op: load_diff_ops) {
            page_io.applyFrom(op.m_storage_page_num, op.m_buffer, { op.m_page_num, op.m_diff_state_num });
        }
    }
    
    MemLock MetaPrefix::mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode)
    {
        bool became_dirty = false;
        auto lock = mapRangeImpl(address, size, access_mode, &became_dirty);
        if (became_dirty) {
            auto page_num = address / getPageSize();
            // copy for diff generation on flush
            captureCoWPage(page_num, lock);
        }
        return lock;
    }

    void MetaPrefix::captureCoWPage(std::uint64_t page_num, const MemLock &lock)
    {
        // Avoid SparseIndexQuery here; a loaded DRAM page is enough to decide
        // whether keeping an in-memory previous version is useful for diff flush.
        if (!hasPage(page_num)) {
            return;
        }

        auto resource_lock = lock.lock();
        if (!resource_lock) {
            THROWF(db0::InternalException) << "MetaPrefix: missing page lock for previous page capture";
        }
        auto &cow_page = m_cow_pages[page_num];
        cow_page.resize(getPageSize());
        std::memcpy(cow_page.data(), resource_lock->getBuffer(), cow_page.size());
    }

    std::uint64_t MetaPrefix::commit(ProcessTimer *)
    {
        // MetaPrefix dirty pages must already be persisted by flush(MetaPrefix &, RandomIO_Stream &).
        // Commit is only the post-flush transaction boundary; accepting dirty pages here
        // would hide a missed detach/cache-commit preparation step in the owner.
        if (isDirty()) {
            THROWF(db0::InternalException) 
                << "MetaPrefix::commit requires flush(MetaPrefix &, RandomIO_Stream &) for dirty pages";
        }

        // The sparse pair belongs to this MetaPrefix and may still have pending
        // sparse/diff index write-backs. Commit it before dirty-page detection so
        // the flush scans the final metadata image for this transaction.
        m_parent_index.commit();
        m_cow_pages.clear();
        return getStateNum(false);
    }

    bool flush(MetaPrefix &prefix, RandomIO_Stream &page_io, ProcessTimer *)
    {
        // The owner must complete metadata detach/cache-commit preparation before
        // this scan. Flush only persists an already registered application state;
        // it must not advance state or perform hidden write-back preparation.        
        bool was_dirty = false;
        auto state_num = prefix.getStateNum(false);
        prefix.flushDirty([&](std::uint64_t page_num, const void *buffer) {
            was_dirty |= prefix.flushPage(page_io, page_num, buffer, state_num);
        });
        
        if (!was_dirty) {
            return false;
        }

        page_io.flush();
        prefix.commit();
        return true;
    }

    bool MetaPrefix::flushPage(RandomIO_Stream &page_io, std::uint64_t page_num, const void *buffer, StateNumType state_num)
    {
        auto cow_page = m_cow_pages.find(page_num);
        if (cow_page != m_cow_pages.end()) {
            std::vector<std::uint16_t> diffs;
            if (getDiffs(cow_page->second.data(), buffer, getPageSize(), diffs) && !diffs.empty()) {
                bool is_first_page = false;
                auto [storage_page_num, overflow] = page_io.appendDiff(
                    buffer, { page_num, state_num }, diffs, &is_first_page
                );
                m_parent_index.getDiffIndex().insert(page_num, state_num, storage_page_num, overflow);
                return true;
            }
            if (diffs.empty()) {
                return false;
            }
        }

        auto storage_page_num = page_io.appendRandom(buffer);
        if (storage_page_num == 0) {
            THROWF(db0::InternalException) << "MetaPrefix: storage page 0 is reserved as an empty full-DP sentinel";
        }
        m_parent_index.getSparseIndex().emplace(page_num, state_num, storage_page_num);
        return true;
    }

    std::uint64_t MetaPrefix::writeFullPage(RandomIO_Stream &page_io, const void *buffer,
        std::uint64_t reusable_storage_page_num)
    {
        if (reusable_storage_page_num != 0) {
            page_io.writeRandom(reusable_storage_page_num, buffer);
            return reusable_storage_page_num;
        }

        auto storage_page_num = page_io.appendRandom(buffer);
        if (storage_page_num == 0) {
            THROWF(db0::InternalException) << "MetaPrefix: storage page 0 is reserved as an empty full-DP sentinel";
        }
        return storage_page_num;
    }

    void MetaPrefix::publishCompactedState(StateNumType state_num)
    {
        m_parent_index.recordMaxStateNum(state_num);
        m_parent_index.commit();        
        m_cow_pages.clear();
        flushDirty([&](std::uint64_t, const void *) {});
    }

    bool compact(MetaPrefix &prefix, RandomIO_Stream &page_io, ProcessTimer *)
    {
        std::map<std::uint64_t, const void *> dirty_pages;
        prefix.forEachDirtyPage([&](std::uint64_t page_num, const void *buffer) {
            dirty_pages[page_num] = buffer;
        });

        std::vector<std::uint64_t> sparse_page_nums;
        sparse_page_nums.reserve(prefix.m_parent_index.getSparseIndex().size());
        std::uint64_t previous_page_num = 0;
        for (auto it = prefix.m_parent_index.getSparseIndex().cbegin(); !it.is_end(); ++it) {
            auto item = *it;
            if (!!item && item.m_page_num != 0 && item.m_page_num != previous_page_num) {
                sparse_page_nums.push_back(item.m_page_num);
                previous_page_num = item.m_page_num;
            }
        }

        std::vector<std::uint64_t> page_nums;
        page_nums.reserve(sparse_page_nums.size() + dirty_pages.size());
        auto sparse_it = sparse_page_nums.begin();
        auto dirty_it = dirty_pages.begin();
        while (sparse_it != sparse_page_nums.end() || dirty_it != dirty_pages.end()) {
            if (dirty_it == dirty_pages.end()
                || (sparse_it != sparse_page_nums.end() && *sparse_it < dirty_it->first)) {
                page_nums.push_back(*sparse_it);
                ++sparse_it;
            } else if (sparse_it == sparse_page_nums.end() || dirty_it->first < *sparse_it) {
                if (dirty_it->first != 0) {
                    page_nums.push_back(dirty_it->first);
                }
                ++dirty_it;
            } else {
                page_nums.push_back(*sparse_it);
                ++sparse_it;
                ++dirty_it;
            }
        }

        if (page_nums.empty()) {
            return false;
        }

        auto before_state_num = prefix.getStateNum(false);
        auto new_state_num = before_state_num + 1;
        auto reusable_full_pages = collectReusableFullPageNums(prefix.m_parent_index, before_state_num);
        std::size_t next_reusable_page = 0;
        std::vector<std::byte> page_buffer(prefix.getPageSize());

        for (auto page_num: page_nums) {
            auto dirty_it = dirty_pages.find(page_num);
            if (dirty_it != dirty_pages.end()) {
                std::memcpy(page_buffer.data(), dirty_it->second, page_buffer.size());
            } else if (prefix.hasPage(page_num)) {
                auto lock = prefix.mapRange(page_num * prefix.getPageSize(), prefix.getPageSize(), { AccessOptions::read });
                std::memcpy(page_buffer.data(), static_cast<void *>(lock), page_buffer.size());
            } else if (!fetchPage(prefix, page_io, page_num, before_state_num, page_buffer.data())) {
                continue;
            }

            auto reusable_storage_page_num = next_reusable_page < reusable_full_pages.size()
                ? reusable_full_pages[next_reusable_page++]
                : 0;
            auto storage_page_num = prefix.writeFullPage(page_io, page_buffer.data(), reusable_storage_page_num);
            prefix.m_parent_index.getSparseIndex().update(page_num, new_state_num, storage_page_num);
        }

        prefix.publishCompactedState(new_state_num);
        return true;
    }

    StateNumType MetaPrefix::getStateNum() const
    {
        return m_parent_index.getMaxStateNum();
    }

    StateNumType MetaPrefix::getStateNum(bool) const
    {
        return m_parent_index.getMaxStateNum();
    }
    
    std::size_t MetaPrefix::flushDirty(std::size_t)
    {
        THROWF(db0::InternalException) << "MetaPrefix::flushDirty(std::size_t) is unsupported; use flush(MetaPrefix &, RandomIO_Stream &)";
        return 0;
    }
    
    void MetaPrefix::forAllocatedAddresses(std::function<void(std::uint64_t)> sink) const
    {
        std::uint64_t last_page_num = 0;
        for (auto it = m_parent_index.getSparseIndex().cbegin(); !it.is_end(); ++it) {
            auto item = *it;
            if (!!item && item.m_page_num != 0 && item.m_page_num != last_page_num) {
                sink(item.m_page_num * getPageSize());
                last_page_num = item.m_page_num;
            }
        }
    }

}
