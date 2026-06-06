// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MetaPrefix.hpp"
#include <dbzero/core/memory/diff_utils.hpp>
#include <dbzero/core/storage/Diff_IO.hpp>
#include <dbzero/core/storage/SparseIndexQuery.hpp>
#include <dbzero/core/storage/SparsePair.hpp>
#include <algorithm>
#include <cstring>
#include <vector>

namespace db0

{

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
        prefix.m_sparse_pair.getSparseIndex().forAll([&](const SI_Item &item) {
            if (item && prefix.readPage(page_io, item.m_page_num, prefix.m_state_num, buffer.data())) {
                auto page_buffer = prefix.update(item.m_page_num, false);
                std::memcpy(page_buffer, buffer.data(), buffer.size());
            }
        });
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
        std::vector<std::uint64_t> page_nums;
        m_sparse_pair.getSparseIndex().forAll([&](const SI_Item &item) {
            if (item && item.m_page_num != 0) {
                page_nums.push_back(item.m_page_num);
            }
        });

        std::sort(page_nums.begin(), page_nums.end());
        page_nums.erase(std::unique(page_nums.begin(), page_nums.end()), page_nums.end());

        for (auto page_num: page_nums) {
            sink(page_num * getPageSize());
        }
    }

}
