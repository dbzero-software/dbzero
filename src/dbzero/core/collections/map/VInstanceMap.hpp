// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <memory>
#include <type_traits>
#include <unordered_map>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/full_text/key_value.hpp>
#include <dbzero/core/memory/VObjectCache.hpp>

namespace db0

{

    /**
     * Ordered numeric-key map from KeyT to persisted instances of ValueT.
     *
     * Values are stored as v-object addresses in the underlying v_bindex and
     * materialized through VObjectCache on lookup.
     */
    template <typename KeyT, typename ValueT, typename ValueAddrT = Address>
    class VInstanceMap: public db0::v_bindex<key_value<KeyT, ValueAddrT>, Address>
    {
    public:
        using MapItemT = key_value<KeyT, ValueAddrT>;
        using super_t = db0::v_bindex<MapItemT, Address>;
        using iterator = typename super_t::iterator;
        using const_iterator = typename super_t::const_iterator;

        VInstanceMap() = default;

        VInstanceMap(Memspace &memspace, VObjectCache &cache)
            : super_t(memspace)
            , m_cache(&cache)
        {
        }

        VInstanceMap(mptr ptr, VObjectCache &cache)
            : super_t(ptr, ptr.getPageSize())
            , m_cache(&cache)
        {
        }

        VInstanceMap(VInstanceMap &&other)
            : super_t(std::move(other))
            , m_cache(other.m_cache)
            , m_active_instances(std::move(other.m_active_instances))
        {
        }

        VInstanceMap &operator=(VInstanceMap &&other)
        {
            super_t::operator=(std::move(other));
            m_cache = other.m_cache;
            m_active_instances = std::move(other.m_active_instances);
            return *this;
        }

        template <typename... Args>
        std::shared_ptr<ValueT> insert(KeyT key, Args&&... args)
        {
            // This is a new map value: constructor args are creation args for ValueT,
            // so pull allocates new persisted state and caches the wrapper.
            auto value = m_cache->template pull<ValueT>(true, this->getMemspace(), std::forward<Args>(args)...);
            MapItemT item(key, value->getAddress());
            MapItemT old_item(key);
            if (super_t::updateExisting(item, &old_item)) {
                destroyValue(old_item.value, std::forward<Args>(args)...);
                m_cache->erase(old_item.value);
                m_active_instances.erase(cacheKey(old_item.value));
            } else {
                super_t::insert(item);
            }
            track(value);
            return value;
        }

        template <typename... Args>
        std::shared_ptr<ValueT> findOrCreate(KeyT key, Args&&... args)
        {
            MapItemT item(key);
            if (super_t::findOne(item)) {
                return openExisting(item.value, std::forward<Args>(args)...);
            }
            return insert(key, std::forward<Args>(args)...);
        }

        template <typename... Args>
        std::shared_ptr<ValueT> tryGet(KeyT key, Args&&... args) const
        {
            MapItemT item(key);
            if (!super_t::findOne(item)) {
                return nullptr;
            }
            return open(item.value, std::forward<Args>(args)...);
        }

        template <typename... Args>
        std::shared_ptr<ValueT> get(KeyT key, Args&&... args) const
        {
            auto result = tryGet(key, std::forward<Args>(args)...);
            if (!result) {
                THROWF(db0::InputException) << "VInstanceMap key not found" << THROWF_END;
            }
            return result;
        }

        template <typename... Args>
        bool erase(KeyT key, Args&&... args)
        {
            auto it = super_t::find(MapItemT(key));
            if (it == super_t::end()) {
                return false;
            }

            auto address = (*it).value;
            auto value = open(address, std::forward<Args>(args)...);
            value->destroy();
            super_t::erase(it);
            m_cache->erase(address);
            m_active_instances.erase(cacheKey(address));
            return true;
        }

        template <typename F>
        std::size_t forEachActive(F &&f) const
        {
            std::size_t active_count = 0;
            for (auto it = m_active_instances.begin(); it != m_active_instances.end();) {
                auto value = it->second.lock();
                if (!value) {
                    it = m_active_instances.erase(it);
                    continue;
                }

                ++active_count;
                ++it;
                if constexpr(std::is_void_v<std::invoke_result_t<F&, ValueT&> >) {
                    f(*value);
                } else {
                    static_assert(std::is_convertible_v<std::invoke_result_t<F&, ValueT&>, bool>,
                        "VInstanceMap::forEachActive callback must return void or bool");
                    if (!f(*value)) {
                        break;
                    }
                }
            }
            return active_count;
        }

    private:
        template <typename... Args>
        void destroyValue(ValueAddrT address, Args&&... args)
        {
            auto tracked_value = tryFindTracked(address);
            if (tracked_value) {
                tracked_value->destroy();
                return;
            }

            auto cached_value = m_cache->template tryFind<ValueT>(address);
            if (cached_value) {
                cached_value->destroy();
                return;
            }

            if constexpr(std::is_constructible_v<ValueT, mptr, Args...>) {
                open(address, std::forward<Args>(args)...)->destroy();
            } else {
                static_assert(std::is_constructible_v<ValueT, mptr>,
                    "ValueT must be constructible from mptr or still present in VObjectCache to destroy replaced values");
                open(address)->destroy();
            }
        }

        template <typename... Args>
        std::shared_ptr<ValueT> open(ValueAddrT address, Args&&... args) const
        {
            // This map item already stores a persisted ValueT address. The numeric
            // address is the cache key; the mptr is the constructor argument that
            // makes pull open the existing object instead of allocating a replacement.
            auto ptr = this->getMemspace().myPtr(address);
            auto value = m_cache->template findOrPull<ValueT>(
                address.getOffset(),
                true,
                ptr,
                std::forward<Args>(args)...
            );
            track(value);
            return value;
        }

        template <typename... Args>
        std::shared_ptr<ValueT> openExisting(ValueAddrT address, Args&&... args) const
        {
            if constexpr(std::is_constructible_v<ValueT, mptr, Args...>) {
                return open(address, std::forward<Args>(args)...);
            } else {
                static_assert(std::is_constructible_v<ValueT, mptr>,
                    "ValueT must be constructible from mptr or from mptr plus the supplied arguments");
                return open(address);
            }
        }

        void track(const std::shared_ptr<ValueT> &value) const
        {
            m_active_instances[value->getAddress().getOffset()] = value;
        }

        std::shared_ptr<ValueT> tryFindTracked(ValueAddrT address) const
        {
            auto it = m_active_instances.find(cacheKey(address));
            if (it == m_active_instances.end()) {
                return nullptr;
            }
            auto value = it->second.lock();
            if (!value) {
                m_active_instances.erase(it);
            }
            return value;
        }

        static std::uint64_t cacheKey(ValueAddrT address)
        {
            return address.getOffset();
        }

        VObjectCache *m_cache = nullptr;
        mutable std::unordered_map<std::uint64_t, std::weak_ptr<ValueT> > m_active_instances;
    };

}
