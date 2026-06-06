// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MetaPrefix.hpp"
#include <dbzero/core/memory/diff_utils.hpp>
#include <dbzero/core/storage/Diff_IO.hpp>
#include <dbzero/core/storage/SparseIndexQuery.hpp>
#include <dbzero/core/storage/SparsePair.hpp>
#include <cstring>
#include <map>
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

    MetaPrefix::MetaPrefix(std::size_t page_size, SparsePair &sparse_pair)
        : DRAM_Prefix(page_size)
        , m_sparse_pair(sparse_pair)
        , m_state_num(sparse_pair.getMaxStateNum())
    {
    }

    void load(MetaPrefix &prefix, Diff_IO &page_io)
    {
        if (prefix.m_state_num == 0) {
            return;
        }

        std::vector<std::byte> buffer(prefix.getPageSize());
        std::uint64_t previous_page_num = 0;
        for (auto it = prefix.m_sparse_pair.getSparseIndex().cbegin(); !it.is_end(); ++it) {
            auto item = *it;
            if (!!item && item.m_page_num != 0 && item.m_page_num != previous_page_num
                && prefix.readPage(page_io, item.m_page_num, prefix.m_state_num, buffer.data())) {
                auto page_buffer = prefix.update(item.m_page_num, false);
                std::memcpy(page_buffer, buffer.data(), buffer.size());
                previous_page_num = item.m_page_num;
            }
        }
    }

    MemLock MetaPrefix::mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode)
    {
        bool became_dirty = false;
        auto lock = mapRangeImpl(address, size, access_mode, &became_dirty);
        if (became_dirty) {
            auto page_num = address / getPageSize();
            capturePreviousPage(page_num, lock);
        }
        return lock;
    }

    void MetaPrefix::capturePreviousPage(std::uint64_t page_num, const MemLock &lock)
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
        auto &previous_page = m_previous_pages[page_num];
        previous_page.resize(getPageSize());
        std::memcpy(previous_page.data(), resource_lock->getBuffer(), previous_page.size());
    }

    bool MetaPrefix::readPage(Diff_IO &page_io, std::uint64_t page_num, StateNumType state_num, void *buffer) const
    {
        SparseIndexQuery query(m_sparse_pair.getSparseIndex(), m_sparse_pair.getDiffIndex(), page_num, state_num);
        if (query.empty()) {
            return false;
        }

        auto storage_page_num = query.first();
        if (storage_page_num) {
            page_io.read(storage_page_num, buffer);
        } else {
            std::memset(buffer, 0, getPageSize());
        }

        StateNumType diff_state_num = 0;
        while (query.next(diff_state_num, storage_page_num)) {
            page_io.applyFrom(storage_page_num, buffer, { page_num, diff_state_num });
        }
        return true;
    }

    std::uint64_t MetaPrefix::commit(ProcessTimer *)
    {
        if (getDirtySize() != 0) {
            THROWF(db0::InternalException) << "MetaPrefix::commit requires flush(MetaPrefix &, Diff_IO &) for dirty pages";
        }
        return m_state_num;
    }

    bool flush(MetaPrefix &prefix, Diff_IO &page_io, ProcessTimer *)
    {
        if (prefix.getDirtySize() == 0) {
            return false;
        }

        auto new_state_num = prefix.m_state_num + 1;
        bool wrote_anything = false;
        prefix.flushDirty([&](std::uint64_t page_num, const void *buffer) {
            wrote_anything |= prefix.flushPage(page_io, page_num, buffer, new_state_num);
        });

        page_io.flush();
        if (wrote_anything) {
            prefix.m_state_num = new_state_num;
            prefix.m_sparse_pair.commit();
            prefix.m_last_updated = prefix.m_state_num;
        }
        prefix.m_previous_pages.clear();
        return wrote_anything;
    }

    bool MetaPrefix::flushPage(Diff_IO &page_io, std::uint64_t page_num, const void *buffer, StateNumType state_num)
    {
        auto previous_page = m_previous_pages.find(page_num);
        bool has_base = previous_page != m_previous_pages.end();

        if (has_base) {
            std::vector<std::uint16_t> diffs;
            if (getDiffs(previous_page->second.data(), buffer, getPageSize(), diffs) && !diffs.empty()) {
                bool is_first_page = false;
                auto [storage_page_num, overflow] = page_io.appendDiff(
                    buffer, { page_num, state_num }, diffs, &is_first_page
                );
                m_sparse_pair.getDiffIndex().insert(page_num, state_num, storage_page_num, overflow);
                return true;
            }
            if (diffs.empty()) {
                return false;
            }
        }

        bool is_first_page = false;
        auto storage_page_num = page_io.append(buffer, &is_first_page);
        if (storage_page_num == 0) {
            THROWF(db0::InternalException) << "MetaPrefix: storage page 0 is reserved as an empty full-DP sentinel";
        }
        m_sparse_pair.getSparseIndex().emplace(page_num, state_num, storage_page_num);
        return true;
    }

    std::uint64_t MetaPrefix::writeFullPage(Diff_IO &page_io, const void *buffer,
        std::uint64_t reusable_storage_page_num)
    {
        if (reusable_storage_page_num != 0) {
            page_io.write(reusable_storage_page_num, const_cast<void *>(buffer));
            return reusable_storage_page_num;
        }

        bool is_first_page = false;
        auto storage_page_num = page_io.append(buffer, &is_first_page);
        if (storage_page_num == 0) {
            THROWF(db0::InternalException) << "MetaPrefix: storage page 0 is reserved as an empty full-DP sentinel";
        }
        return storage_page_num;
    }

    void MetaPrefix::publishCompactedState(StateNumType state_num)
    {
        m_state_num = state_num;
        m_sparse_pair.commit();
        m_last_updated = m_state_num;
        m_previous_pages.clear();
        flushDirty([&](std::uint64_t, const void *) {});
    }

    bool compact(MetaPrefix &prefix, Diff_IO &page_io, ProcessTimer *)
    {
        std::map<std::uint64_t, const void *> dirty_pages;
        prefix.forEachDirtyPage([&](std::uint64_t page_num, const void *buffer) {
            dirty_pages[page_num] = buffer;
        });

        std::vector<std::uint64_t> sparse_page_nums;
        sparse_page_nums.reserve(prefix.m_sparse_pair.getSparseIndex().size());
        std::uint64_t previous_page_num = 0;
        for (auto it = prefix.m_sparse_pair.getSparseIndex().cbegin(); !it.is_end(); ++it) {
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

        auto before_state_num = prefix.m_state_num;
        auto new_state_num = prefix.m_state_num + 1;
        auto reusable_full_pages = collectReusableFullPageNums(prefix.m_sparse_pair, before_state_num);
        std::size_t next_reusable_page = 0;
        std::vector<std::byte> page_buffer(prefix.getPageSize());

        for (auto page_num: page_nums) {
            auto dirty_it = dirty_pages.find(page_num);
            if (dirty_it != dirty_pages.end()) {
                std::memcpy(page_buffer.data(), dirty_it->second, page_buffer.size());
            } else if (prefix.hasPage(page_num)) {
                auto lock = prefix.mapRange(page_num * prefix.getPageSize(), prefix.getPageSize(), { AccessOptions::read });
                std::memcpy(page_buffer.data(), static_cast<void *>(lock), page_buffer.size());
            } else if (!prefix.readPage(page_io, page_num, before_state_num, page_buffer.data())) {
                continue;
            }

            auto reusable_storage_page_num = next_reusable_page < reusable_full_pages.size()
                ? reusable_full_pages[next_reusable_page++]
                : 0;
            auto storage_page_num = prefix.writeFullPage(page_io, page_buffer.data(), reusable_storage_page_num);
            prefix.m_sparse_pair.getSparseIndex().update(page_num, new_state_num, storage_page_num);
        }

        prefix.publishCompactedState(new_state_num);
        return true;
    }

    StateNumType MetaPrefix::getStateNum(bool) const
    {
        return m_state_num;
    }

    std::size_t MetaPrefix::getDirtySize() const
    {
        std::size_t result = 0;
        forEachDirtyPage([&](std::uint64_t, const void *) {
            result += getPageSize();
        });
        return result;
    }

    std::size_t MetaPrefix::flushDirty(std::size_t)
    {
        THROWF(db0::InternalException) << "MetaPrefix::flushDirty(std::size_t) is unsupported; use flush(MetaPrefix &, Diff_IO &)";
        return 0;
    }

    std::uint64_t MetaPrefix::getLastUpdated() const
    {
        return m_last_updated;
    }

    void MetaPrefix::forAllocatedAddresses(DRAM_Allocator::AddressSinkFunction sink) const
    {
        std::uint64_t previous_page_num = 0;
        for (auto it = m_sparse_pair.getSparseIndex().cbegin(); !it.is_end(); ++it) {
            auto item = *it;
            if (!!item && item.m_page_num != 0 && item.m_page_num != previous_page_num) {
                sink(item.m_page_num * getPageSize());
                previous_page_num = item.m_page_num;
            }
        }
    }

}
