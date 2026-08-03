// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include "v_bvector.hpp"

#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/utils/hash_func.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/vspace/v_object.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <utility>
#include <vector>

namespace db0

{

DB0_PACKED_BEGIN
    template <typename ItemT, typename PtrT = Address>
    struct DB0_PACKED_ATTR o_cached_bvector: public db0::o_fixed_versioned<o_cached_bvector<ItemT, PtrT> >
    {
        db0_ptr<v_bvector<ItemT, PtrT> > m_vector_ptr;
        std::uint64_t m_materialized_hash = 0;

        o_cached_bvector() = default;
    };
DB0_PACKED_END

    template <typename ItemT, typename PtrT = Address>
    class CachedVBVector: public db0::v_object<o_cached_bvector<ItemT, PtrT> >
    {
    public:
        using vector_t = v_bvector<ItemT, PtrT>;
        using overlay_t = o_cached_bvector<ItemT, PtrT>;
        using super_t = db0::v_object<overlay_t>;
        using value_type = ItemT;
        using KeyChangeCallback = std::function<void(const ItemT &, bool added)>;

        static_assert(std::is_trivially_copyable_v<ItemT>,
            "CachedVBVector is intended for simple value types");

        CachedVBVector() = default;

        CachedVBVector(Memspace &memspace, BVectorFlags flags = {}, AccessFlags access_mode = {},
            KeyChangeCallback key_change_callback = {})
            : super_t(memspace, access_mode)
            , m_vector(memspace, flags, access_mode)
            , m_key_change_callback(std::move(key_change_callback))
        {
            auto &self = this->modify();
            self.m_vector_ptr = m_vector;
            self.m_materialized_hash = calculateHash(m_cache);
            m_cache_hash = self.m_materialized_hash;
            m_detached = false;
        }

        CachedVBVector(mptr ptr, AccessFlags access_mode = {}, KeyChangeCallback key_change_callback = {})
            : super_t(ptr, access_mode)
            , m_vector(this->getMemspace().myPtr((*this)->m_vector_ptr.getAddress()), access_mode)
            , m_key_change_callback(std::move(key_change_callback))
        {
            refreshCacheFromVector();
            m_detached = false;
        }

        bool isNull() const
        {
            return super_t::operator!();
        }

        void setKeyChangeCallback(KeyChangeCallback key_change_callback)
        {
            m_key_change_callback = std::move(key_change_callback);
        }

        void init(Memspace &memspace, BVectorFlags flags = {}, AccessFlags access_mode = {})
        {
            std::vector<ItemT> removed_items;
            if (m_key_change_callback) {
                removed_items = m_cache;
            }
            super_t::init(memspace, access_mode);
            m_vector.init(memspace, flags, access_mode);
            m_cache.clear();
            auto &self = this->modify();
            self.m_vector_ptr = m_vector;
            self.m_materialized_hash = calculateHash(m_cache);
            m_cache_hash = self.m_materialized_hash;
            m_detached = false;
            for (const auto &item: removed_items) {
                notifyKeyChange(item, false);
            }
        }

        void init(mptr ptr, AccessFlags access_mode = {})
        {
            super_t::operator=(super_t(ptr, access_mode));
            m_vector.init(this->getMemspace().myPtr((*this)->m_vector_ptr.getAddress()), access_mode);
            refreshCacheFromVector();
            m_detached = false;
        }

        const std::vector<ItemT> &cached() const
        {
            ensureAttached();
            return m_cache;
        }

        std::uint64_t materializedHash() const
        {
            return isNull() ? 0 : (*this)->m_materialized_hash;
        }

        std::uint64_t cacheHash() const
        {
            ensureAttached();
            return m_cache_hash;
        }

        std::uint64_t size() const
        {
            ensureAttached();
            return m_cache.size();
        }

        bool empty() const
        {
            return size() == 0;
        }

        void push_back(const ItemT &item)
        {
            ensureAttached();
            m_vector.push_back(item);
            m_cache.push_back(item);
            updateMaterializedHash(true);
            notifyKeyChange(item, true);
        }

        void erase(std::uint64_t position)
        {
            ensureAttached();
            auto removed_item = m_cache.at(position);
            m_vector.erase(position);
            m_cache.erase(m_cache.begin() + static_cast<std::ptrdiff_t>(position));
            updateMaterializedHash(true);
            notifyKeyChange(removed_item, false);
        }

        void clear()
        {
            ensureAttached();
            auto items_changed = !m_cache.empty();
            std::vector<ItemT> removed_items;
            if (m_key_change_callback) {
                removed_items = m_cache;
            }
            m_vector.clear();
            m_cache.clear();
            updateMaterializedHash(items_changed);
            for (const auto &item: removed_items) {
                notifyKeyChange(item, false);
            }
        }

        void detach() const
        {
            if (isNull()) {
                return;
            }

            m_vector.detach();
            super_t::detach();
            m_detached = true;
        }

        void commit() const
        {
            if (isNull()) {
                return;
            }
            m_vector.commit();
            super_t::commit();
        }

    private:
        static std::uint64_t calculateHash(const std::vector<ItemT> &items)
        {
            static const ItemT empty = {};
            auto data = items.empty() ? &empty : items.data();
            return db0::murmurhash64A(data, items.size() * sizeof(ItemT), items.size());
        }

        void refreshCacheFromVector() const
        {
            std::vector<ItemT> previous_cache;
            if (m_key_change_callback) {
                previous_cache = m_cache;
            }
            m_cache.clear();
            m_cache.reserve(static_cast<std::size_t>(m_vector.size()));
            for (const auto &item: m_vector) {
                m_cache.push_back(item);
            }
            m_cache_hash = (*this)->m_materialized_hash;
            notifyKeyChanges(previous_cache, m_cache);
        }

        void notifyKeyChange(const ItemT &item, bool added) const
        {
            if (m_key_change_callback) {
                m_key_change_callback(item, added);
            }
        }

        void notifyKeyChanges(const std::vector<ItemT> &previous_items,
            const std::vector<ItemT> &current_items) const
        {
            if (!m_key_change_callback) {
                return;
            }

            std::vector<bool> matched(current_items.size(), false);
            for (const auto &previous_item: previous_items) {
                std::size_t index = 0;
                while (index < current_items.size()
                    && (matched[index] || !(current_items[index] == previous_item))) {
                    ++index;
                }
                if (index < current_items.size()) {
                    matched[index] = true;
                } else {
                    notifyKeyChange(previous_item, false);
                }
            }
            for (std::size_t index = 0; index < current_items.size(); ++index) {
                if (!matched[index]) {
                    notifyKeyChange(current_items[index], true);
                }
            }
        }

        void ensureAttached() const
        {
            if (isNull() || !m_detached) {
                return;
            }

            if ((*this)->m_materialized_hash != m_cache_hash) {
                m_vector.detach();
                refreshCacheFromVector();
            }
            m_detached = false;
        }

        void updateMaterializedHash(bool items_changed)
        {
            auto previous_hash = (*this)->m_materialized_hash;
            auto new_hash = calculateHash(m_cache);
            if (items_changed && new_hash == previous_hash) {
                ++new_hash;
            }
            this->modify().m_materialized_hash = new_hash;
            m_cache_hash = new_hash;
        }

        mutable vector_t m_vector;
        mutable std::vector<ItemT> m_cache;
        mutable std::uint64_t m_cache_hash = 0;
        mutable bool m_detached = false;
        KeyChangeCallback m_key_change_callback;
    };

}
