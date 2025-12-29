// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <unordered_map>
#include "key_value.hpp"
#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>
#include <dbzero/core/memory/VObjectCache.hpp>

namespace db0

{
    
    template <typename KeyT = UniqueAddress, typename ValueT = Address>
    ValueT addressOfMBIndex(const db0::MorphingBIndex<KeyT> &mb_index)
    {
        // use high 4-bits for index type
        assert(mb_index.getAddress().getOffset() < 0x0FFFFFFFFFFFFFFF);
        return Address::fromOffset(mb_index.getAddress().getOffset() | (static_cast<std::uint64_t>(mb_index.getIndexType()) << 60));
    }
    
    template <typename KeyT = UniqueAddress, typename ValueT = Address>
    std::shared_ptr<db0::MorphingBIndex<KeyT> > indexFromAddress(VObjectCache &cache, ValueT address)
    {
        // use high 4-bits for index type
        auto index_type = static_cast<db0::bindex::type>(address.getOffset() >> 60);
        auto mb_addr = Address::fromOffset(address & 0x0FFFFFFFFFFFFFFF);
        // NOTE: first address is for cache, the latter for MorphingBIndex
        // NOTE: MorphingBIndex does not provide detach functionality
        return cache.findOrCreate<db0::MorphingBIndex<KeyT> >(mb_addr, false, mb_addr, index_type);
    }    
    
    template <typename IndexKeyT = std::uint64_t, typename KeyT = UniqueAddress, typename ValueT = Address>
    class InvertedIndex: public db0::v_bindex<key_value<IndexKeyT, ValueT>, Address>
    {
        mutable progressive_mutex m_mutex;

    public:
        using ListT = db0::MorphingBIndex<KeyT>;
        using MapItemT = key_value<IndexKeyT, ValueT>;
        using super_t = db0::v_bindex<key_value<IndexKeyT, ValueT>, Address>;
        // convert inverted list to value
        using ValueFunctionT = std::function<ValueT(const ListT &)>;
        // extract inverted list address from value, pull through cache
        using ListFunctionT = std::function<std::shared_ptr<ListT>(VObjectCache &, ValueT)>;
        using iterator = typename super_t::iterator;

        /**
         * Construct as null / invalid
         */
        InvertedIndex() = default;

        InvertedIndex(Memspace &memspace, VObjectCache &, ValueFunctionT = addressOfMBIndex<KeyT, ValueT>,
            ListFunctionT = indexFromAddress<KeyT, ValueT>);

        InvertedIndex(mptr ptr, VObjectCache &, ValueFunctionT = addressOfMBIndex<KeyT, ValueT>, 
            ListFunctionT = indexFromAddress<KeyT, ValueT>);

        InvertedIndex(InvertedIndex &&);

        /**
         * Pull existing or create new key inverted list
         * @param key key to retrieve/create the inverted list by
         * @return the inverted list object
         */
        std::shared_ptr<ListT> findOrCreateInvertedList(IndexKeyT key);
        
        /**
         * Similar as getObjectIndex but performed in a bulk operation for all provided keys
         * @param keys ***MUST BE SORTED*** ascendingS
         * @param callback_ptr optional callback to be called for each newly created inverted list
         * NOTICE: result iterator may point at null (not initialized list and must be initialized)
         */
        template<typename InputIterator>
        std::vector<iterator> bulkGetInvertedLists(InputIterator first_key, InputIterator last_key,
            std::function<void(IndexKeyT)> *callback_ptr = nullptr)
        {
            std::vector<iterator> result;
            using InputIteratorCategory = typename std::iterator_traits<InputIterator>::iterator_category;
            if constexpr(std::is_same_v<InputIteratorCategory, std::random_access_iterator_tag>) {
                // We only preallocate vector when number of keys can be computed easily
                result.reserve(std::distance(first_key, last_key));
            }
            
            std::function<void(key_value<IndexKeyT, ValueT>)> callback;
            if (callback_ptr) {
                callback = [callback_ptr](key_value<IndexKeyT, ValueT> item) {
                    (*callback_ptr)(item.key);
                };
            }            
            
            // First pass will insert non existing items
            this->bulkInsertUnique(first_key, last_key, callback ? &callback : nullptr);
            
            // Second pass is to pull results (can run on many threads when this makes sense)
            {                
                auto it = this->beginJoin(1);
                std::for_each(first_key, last_key,
                [&](const IndexKeyT &index_key) {
                    [[maybe_unused]] bool join_result = it.join(index_key, 1);
                    assert(join_result);
                    assert((*it).key == index_key);
                    result.emplace_back(it.getIterator());
                });
            }
            return result;
        }

        /**
         * Pull list by map item
         */
        std::shared_ptr<ListT> getInvertedList(const MapItemT &item) const;

        /**
         * Pull existing index from under iterator (or create new)
         */
        std::shared_ptr<ListT> getInvertedList(iterator &it);
        
        /**
         * Pull through cache, throw if no such inverted index found
         */
        std::shared_ptr<ListT> getExistingInvertedList(IndexKeyT) const;

        std::shared_ptr<ListT> tryGetExistingInvertedList(IndexKeyT) const;
        
        inline VObjectCache &getVObjectCache() const {
            return m_cache;
        }

    private:
        VObjectCache &m_cache;
        ValueFunctionT m_value_function;
        ListFunctionT m_list_function;
    };

    template <typename IndexKeyT, typename KeyT, typename ValueT> 
    InvertedIndex<IndexKeyT, KeyT, ValueT>::InvertedIndex(Memspace &memspace,
        VObjectCache &cache, ValueFunctionT value_function, ListFunctionT list_function)
        : super_t(memspace)
        , m_cache(cache)
        , m_value_function(value_function)
        , m_list_function(list_function)
    {        
    }

    template <typename IndexKeyT, typename KeyT, typename ValueT>
    InvertedIndex<IndexKeyT, KeyT, ValueT>::InvertedIndex(mptr ptr,
        VObjectCache &cache, ValueFunctionT value_function, ListFunctionT list_function)
        : super_t(ptr, ptr.getPageSize())
        , m_cache(cache)
        , m_value_function(value_function)
        , m_list_function(list_function)
    {
    }

    template <typename IndexKeyT, typename KeyT, typename ValueT>
    InvertedIndex<IndexKeyT, KeyT, ValueT>::InvertedIndex(InvertedIndex &&other)
        : super_t(std::move(other))
        , m_cache(other.m_cache)
        , m_value_function(other.m_value_function)
        , m_list_function(other.m_list_function)
    {
    }
    
    template <typename IndexKeyT, typename KeyT, typename ValueT>
    std::shared_ptr<typename InvertedIndex<IndexKeyT, KeyT, ValueT>::ListT>
    InvertedIndex<IndexKeyT, KeyT, ValueT>::findOrCreateInvertedList(IndexKeyT key)
    {
		MapItemT item(key);
		auto it = super_t::find(item);
		if (it == super_t::end()) {
			// construct as empty (pull through cache)
			auto list_ptr = m_cache.create<ListT>();
			item.value =  m_value_function(*list_ptr);
			super_t::insert(item);
			return list_ptr;
		} else {
			// fetch existing
            return m_list_function(m_cache, it->value);			
		}
    }
    
    template <typename IndexKeyT, typename KeyT, typename ValueT> 
    std::shared_ptr<typename InvertedIndex<IndexKeyT, KeyT, ValueT>::ListT>
    InvertedIndex<IndexKeyT, KeyT, ValueT>::getInvertedList(const MapItemT &item) const
    {
        // pull dbzero existing        
        return m_list_function(m_cache, item.value);
    }

    template <typename IndexKeyT, typename KeyT, typename ValueT> 
    std::shared_ptr<typename InvertedIndex<IndexKeyT, KeyT, ValueT>::ListT> 
    InvertedIndex<IndexKeyT, KeyT, ValueT>::getInvertedList(iterator &it)
    {
		if ((*it).value == ValueT()) {
            // assume ListT as non-detachable (e.g. MorphingBIndex)
            auto list_ptr = m_cache.create<ListT>(false);
            it.modifyItem().value = m_value_function(*list_ptr);
            return list_ptr;
		} else {
			// fetch existing (pull through cache)
            return m_list_function(m_cache, (*it).value);
		}
    }
    
    template <typename IndexKeyT, typename KeyT, typename ValueT>
    std::shared_ptr<typename InvertedIndex<IndexKeyT, KeyT, ValueT>::ListT> 
    InvertedIndex<IndexKeyT, KeyT, ValueT>::getExistingInvertedList(IndexKeyT key) const
    {
		auto result_ptr = tryGetExistingInvertedList(key);
        if (!result_ptr) {
            THROWF(db0::InputException) << "Inverted list not found" << THROWF_END;
        }

		return result_ptr;
    }

    template <typename IndexKeyT, typename KeyT, typename ValueT>
    std::shared_ptr<typename InvertedIndex<IndexKeyT, KeyT, ValueT>::ListT> 
    InvertedIndex<IndexKeyT, KeyT, ValueT>::tryGetExistingInvertedList(IndexKeyT key) const
    {
		progressive_mutex::scoped_lock lock(m_mutex);
		for (;;) {
			lock.lock();
			MapItemT item(key);
			auto it_list = super_t::find(item);
            if (it_list == super_t::end()) {
                return nullptr;
            }
			
			if (!lock.upgradeToUniqueLock()) {
				continue;
			}
			// pull through cache
            return m_list_function(m_cache, (*it_list).value);
        }
    }
    
}