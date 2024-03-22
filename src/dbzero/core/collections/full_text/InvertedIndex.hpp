#pragma once

#include <vector>
#include <unordered_map>
#include "key_value.hpp"
#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>

namespace db0

{
    
    template <typename KeyT = std::uint64_t, typename ValueT = std::uint64_t> 
    ValueT addressOfMBIndex(const db0::MorphingBIndex<KeyT> &mb_index)
    {
        // use high 4-bits for index type
        return mb_index.getAddress() | (static_cast<std::uint64_t>(mb_index.getIndexType()) << 60);
    }
    
    template <typename KeyT = std::uint64_t, typename ValueT = std::uint64_t>
    db0::MorphingBIndex<KeyT> indexFromAddress(Memspace &memspace, ValueT address)
    {
        // use high 4-bits for index type 
        auto index_type = static_cast<db0::bindex::type>(address >> 60);
        return db0::MorphingBIndex<KeyT>(memspace, address & 0x0FFFFFFFFFFFFFFF, index_type);
    }
    
    template <typename KeyT = std::uint64_t, typename ValueT = std::uint64_t>
    class InvertedIndex : public db0::v_bindex<key_value<KeyT, ValueT>, std::uint64_t>
    {
        mutable progressive_mutex m_mutex;

    public :
        using ListT = db0::MorphingBIndex<KeyT>;
        using MapItemT = key_value<KeyT, ValueT>;
        using super_t = db0::v_bindex<MapItemT, std::uint64_t>;
        // convert inverted list to value
        using ValueFunctionT = std::function<ValueT(const ListT &)>;
        // construct inverted list from value
        using ListFunctionT = std::function<ListT(Memspace &, ValueT)>;
        using iterator = typename super_t::iterator;

        /**
         * Construct as null / invalid
         */
        InvertedIndex() = default;

        InvertedIndex(Memspace &memspace, ValueFunctionT = addressOfMBIndex<KeyT, ValueT>, 
            ListFunctionT = indexFromAddress<KeyT, ValueT>);

        InvertedIndex(mptr ptr, ValueFunctionT = addressOfMBIndex<KeyT, ValueT>, 
            ListFunctionT = indexFromAddress<KeyT, ValueT>);

        /**
         * Pull existing or create new key inverted list
         * @param key key to retrieve/create the inverted list by
         * @return the inverted list object
         */
        ListT getInvertedList(KeyT key);
        
        /**
         * Similar as getObjectIndex but performed in a bulk operation for all provided keys
         * @param keys ***MUST BE SORTED*** ascending
         * NOTICE: result iterator may point at null (not initialized list and must be initialized)
         */
        template<typename InputIterator>
        std::vector<iterator> bulkGetInvertedList(
            InputIterator first_key, InputIterator last_key, ProcessTimer *timer = nullptr
        )
        {
            std::vector<iterator> result;
            using InputIteratorCategory = typename std::iterator_traits<InputIterator>::iterator_category;
            if constexpr(std::is_same_v<InputIteratorCategory, std::random_access_iterator_tag>) {
                // We only preallocate vector when number of keys can be computed easly
                result.reserve(std::distance(first_key, last_key));
            }
            // First pass will insert non existing items
            {
                ProcessTimer t("First pass (insert non-existing keys)", timer);
                this->bulkInsertUnique(first_key, last_key);
            }
            // Second pass is to pull results (can run on many threads when this makes sense)
            {
                ProcessTimer t("Second pass (pull results)", timer);
                auto it = this->beginJoin(1);
                std::for_each(first_key, last_key,
                [&](const KeyT &key) {
                    [[maybe_unused]] bool join_result = it.join(key, 1);
                    assert(join_result);
                    assert((*it).key == key);
                    result.emplace_back(it.getIterator());
                });
            }
            return result;
        }

        /**
         * Pull list by map item
         */
        ListT getInvertedList(const MapItemT &item) const;

        /**
         * Pull existing index from under iterator (or create new)
         */
        ListT getInvertedList(iterator &it);

        void setInvertedList(const MapItemT &item);

        /**
         * Pull through cache, throw if no such inverted index found
         */
        const ListT &getExistingInvertedList(KeyT) const;

        const ListT *tryGetExistingInvertedList(KeyT) const;

        /**
         * Pull existing object index for modifications (through cache)
         * @param ptr_item
         * @return nullptr if not found / no such exists
         */
        ListT *tryGetExistingInvertedList(KeyT);

        void removeFromCache(KeyT);

        void clear();

    private:
        mutable std::unordered_map<KeyT, ListT> m_cache;
        ValueFunctionT m_value_function;
        ListFunctionT m_list_function;
    };

    template <typename KeyT, typename ValueT> InvertedIndex<KeyT, ValueT>::InvertedIndex(Memspace &memspace, 
        ValueFunctionT value_function, ListFunctionT list_function)
        : super_t(memspace)
        , m_value_function(value_function)
        , m_list_function(list_function)
    {        
    }

    template <typename KeyT, typename ValueT> InvertedIndex<KeyT, ValueT>::InvertedIndex(mptr ptr,
        ValueFunctionT value_function, ListFunctionT list_function)    
        : super_t(ptr, ptr.getPageSize())
        , m_value_function(value_function)
        , m_list_function(list_function)
    {
    }
    
    template <typename KeyT, typename ValueT>
    typename InvertedIndex<KeyT, ValueT>::ListT InvertedIndex<KeyT, ValueT>::getInvertedList(KeyT key)
    {
		MapItemT item(key);
		auto it = super_t::find(item);
		if (it == super_t::end()) {
			// construct as empty
			ListT result(this->getMemspace());
			item.value =  m_value_function(result);
			super_t::insert(item);
			return result;
		} else {
			// pull DBZero existing
            auto &memspace = this->getMemspace();
            return m_list_function(memspace, it->value);			
		}
    }

    template <typename KeyT, typename ValueT> 
    typename InvertedIndex<KeyT, ValueT>::ListT InvertedIndex<KeyT, ValueT>::getInvertedList(const MapItemT &item) const
    {
        // pull DBZero existing        
        return m_list_function(this->getMemspace(), item.value);
    }

    template <typename KeyT, typename ValueT> 
    typename InvertedIndex<KeyT, ValueT>::ListT InvertedIndex<KeyT, ValueT>::getInvertedList(iterator &it)
    {
		if ((*it).value == ValueT()) {
            auto result = ListT(this->getMemspace());
            it.modifyItem().value = m_value_function(result);
            return result;
		} else {
			// pull existing
            auto &memspace = this->getMemspace();
            return m_list_function(memspace, (*it).value);
		}
    }

    template <typename KeyT, typename ValueT>
    void InvertedIndex<KeyT, ValueT>::setInvertedList(const MapItemT &item)
    {
		auto it = super_t::find(item);
		if (it == super_t::end()) {
			super_t::insert(item);
		} else {
			it.modifyItem().value = item.value;
		}
    }

    template <typename KeyT, typename ValueT>
    const typename InvertedIndex<KeyT, ValueT>::ListT &InvertedIndex<KeyT, ValueT>::getExistingInvertedList(KeyT key) const
    {
		auto result_ptr = tryGetExistingObjectIndex(key);
        if (!result_ptr) {
            THROWF(db0::InputException) << "Inverted list not found" << THROWF_END;
        }

		return *result_ptr;
    }

    template <typename KeyT, typename ValueT>
    const typename InvertedIndex<KeyT, ValueT>::ListT *InvertedIndex<KeyT, ValueT>::tryGetExistingInvertedList(KeyT key) const
    {
		progressive_mutex::scoped_lock lock(m_mutex);
		for (;;) {
			lock.lock();
			auto it = m_cache.find(key);
			if (it == m_cache.end()) {
				MapItemT item(key);
				auto it_list = super_t::find(item);
				if (it_list!=super_t::end()) {
					if (!lock.upgradeToUniqueLock()) {
						continue;
					}
					// add to cache					
					it = m_cache.emplace(
                        std::piecewise_construct ,
                        std::forward_as_tuple(key) ,
                        std::forward_as_tuple(m_list_function(this->getMemspace(), (*it_list).value))).first;
				}
                else
                {
					return nullptr;
				}
			}
			return &it->second;
		}
    }

    template <typename KeyT, typename ValueT>
    typename InvertedIndex<KeyT, ValueT>::ListT *InvertedIndex<KeyT, ValueT>::tryGetExistingInvertedList(KeyT key)
    {
        return const_cast<ListT*>(
                const_cast<const InvertedIndex*>(this)->tryGetExistingInvertedList(key)
        );
    }

    template <typename KeyT, typename ValueT>
    void InvertedIndex<KeyT, ValueT>::removeFromCache(KeyT key)
    {
		progressive_mutex::scoped_lock lock(m_mutex);
		for (;;) {
			lock.lock();
			auto it = m_cache.find(key);
			if (it != m_cache.end()) {
				if (!lock.upgradeToUniqueLock()) {
					continue;
				}
				m_cache.erase(it);
			}
			return;
		}
    }

    template <typename KeyT, typename ValueT>
    void InvertedIndex<KeyT, ValueT>::clear()
    {
        super_t::clear();
        m_cache.clear();
    }
    
}