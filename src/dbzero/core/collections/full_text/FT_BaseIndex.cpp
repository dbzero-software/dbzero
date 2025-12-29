// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FT_BaseIndex.hpp"
#include "FT_IndexIterator.hpp"
#include "ConverterIteratorAdapter.hpp"
#include "InvertedIndex.hpp"

namespace db0

{

    template <typename T> bool is_valid(const T &value) {
        return value;
    }
    
    // is_valid specialization for UniqueAddress
    template <> bool is_valid(const UniqueAddress &value) {
        return value.isValid();
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::FT_BaseIndex(Memspace & memspace, VObjectCache &cache)
        : super_t(memspace, cache)
    {
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::FT_BaseIndex(mptr ptr, VObjectCache &cache)
        : super_t(ptr, cache)
    {
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::FT_BaseIndex(FT_BaseIndex &&other)
        : super_t(std::move(other))        
    {    
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    std::unique_ptr<FT_Iterator<KeyT> > 
    FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::makeIterator(IndexKeyT key, int direction) const
    {
        using ListT = typename super_t::ListT;
        auto inverted_list_ptr = this->tryGetExistingInvertedList(key);
        if (!inverted_list_ptr) {
            return nullptr;
        }
        return std::unique_ptr<FT_Iterator<KeyT> >(
            new FT_IndexIterator<ListT, KeyT, IndexKeyT>(*inverted_list_ptr, direction, key)
        );
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    bool FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::addIterator(FT_IteratorFactory<KeyT> &factory, IndexKeyT key) const
    {
        using ListT = typename super_t::ListT;
        auto inverted_list_ptr = this->tryGetExistingInvertedList(key);
        if (!inverted_list_ptr) {
            return false;
        }
        
        // key inverted index
        factory.add(std::unique_ptr<FT_Iterator<KeyT> >(
            new FT_IndexIterator<ListT, KeyT, IndexKeyT>(*inverted_list_ptr, -1, key))
        );
        return true;
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
	std::shared_ptr<typename FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperation> 
    FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::getBatchOperation() const
    {
        return std::shared_ptr<BatchOperation>(new BatchOperation(*const_cast<self_t*>(this)));
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
	typename FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperationBuilder 
    FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::beginBatchUpdate() const
    {
		return getBatchOperation();
	}

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperationBuilder::BatchOperationBuilder(std::shared_ptr<BatchOperation> batch_operation)
        : m_batch_operation(batch_operation)
    {
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperation::BatchOperation(FT_BaseIndex<IndexKeyT, KeyT, IndexValueT> &base_index)
        : m_base_index_ptr(&base_index)        
    {
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
	FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperation::~BatchOperation() 
    {
        if (m_commit_called) {
            return;
        }

        assert(m_add_set.empty() && m_remove_set.empty() &&
            "Operation not completed properly/commit or rollback should be called");
	}
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
	void FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperation::clear()
    {
		std::unique_lock<std::recursive_mutex> lock(m_mutex);
		m_add_set.clear();
		m_remove_set.clear();
        m_commit_called = false;
	}

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    bool FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperation::empty() const
    {
        std::unique_lock<std::recursive_mutex> lock(m_mutex);
        return m_add_set.empty() && m_remove_set.empty();
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    bool FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperation::assureEmpty()
    {
        std::unique_lock<std::recursive_mutex> lock(m_mutex);
        return m_add_set.assureEmpty() && m_remove_set.assureEmpty();
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    typename FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::FlushStats 
    FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperation::flush(
        std::function<void(KeyT)> *insert_callback_ptr, 
        std::function<void(KeyT)> *erase_callback_ptr,
        std::function<void(IndexKeyT)> *index_insert_callback_ptr, 
        std::function<void(IndexKeyT)> *index_erase_callback_ptr)
    {
        using TagRangesVector = std::vector<typename TagValueList::iterator>;
        struct GetIteratorPairFirst {
            IndexKeyT operator()(typename TagValueList::iterator it) const {
                return it->first;
            }
        };

        using TagIterator = db0::ConverterIteratorAdapter<typename TagRangesVector::iterator, GetIteratorPairFirst>;
        struct GetPairSecond {
            KeyT operator()(typename TagValueList::reference value) const {
                return value.second;
            }
        };

        using ValueIterator = db0::ConverterIteratorAdapter<typename TagValueList::iterator, GetPairSecond>;
        auto add =
        [insert_callback_ptr, index_insert_callback_ptr](std::uint32_t &all_count, std::uint32_t &new_count) {
            return 
                [insert_callback_ptr, index_insert_callback_ptr, &all_count, &new_count]
                (TagValueList &buf, FT_BaseIndex &index) 
            {
                auto buf_begin = buf.begin(), buf_end = buf.end();
                if (buf_begin == buf_end) {
                    return;
                }
                // sort the tags list and remove duplicate elements
                std::sort(buf_begin, buf_end);
                buf_end = std::unique(buf_begin, buf_end);
                
                TagRangesVector tag_ranges;
                // Find ranges for all tags
                // This vector will also effectively contain all unique tags
                tag_ranges.emplace_back(buf_begin);
                auto last_tag = buf_begin->first;
                for (auto it = buf_begin + 1; it != buf_end; ++it) {
                    if (it->first != last_tag) {
                        tag_ranges.emplace_back(it);
                        last_tag = it->first;
                    }
                }
                                
                // Create inverted lists for tags and get corresponding iterators to them
                std::vector<typename FT_BaseIndex<IndexKeyT>::iterator> tag_index_its = index.bulkGetInvertedLists(
                    TagIterator(tag_ranges.begin()),
                    TagIterator(tag_ranges.end()),
                    index_insert_callback_ptr
                );
                assert(tag_index_its.size() == tag_ranges.size());
                // Add end iterator to avoid special case
                tag_ranges.emplace_back(buf_end);

                for (std::size_t i = 0, n = tag_ranges.size() - 1; i < n; ++i) {
                    auto range_first = tag_ranges[i], range_last = tag_ranges[i + 1];
                    // Either create new or pull existing inverted list
                    typename FT_BaseIndex<IndexKeyT>::iterator &tag_index_it = tag_index_its[i];
                    assert((*tag_index_it).key == range_first->first);
                    auto tag_index_ptr = index.getInvertedList(tag_index_it);
                    auto old_addr = tag_index_ptr->getAddress();
                    auto old_map_value = addressOfMBIndex(*tag_index_ptr);
                    // NOTICE: only unique items are retained in index
                    // callback notified about unique items (objects)
                    std::pair<std::uint32_t, std::uint32_t> stats = tag_index_ptr->bulkInsertUnique(
                        ValueIterator(range_first),
                        ValueIterator(range_last),
                        insert_callback_ptr
                    );

                    // This check is here  because tag_index's location may have been changed by insert
                    // We need to update pointer to tag_index (either address or type changed)
                    auto new_map_value = addressOfMBIndex(*tag_index_ptr);
                    if (old_map_value != new_map_value) {
                        // update the address
                        tag_index_it.modifyItem().value = new_map_value;
                        // remove from cache since this instance has been relocated
                        index.getVObjectCache().erase(old_addr);
                    }
                    all_count += stats.first;
                    new_count += stats.second;
                }
                buf.clear();
            };
        };

        auto remove = 
        [erase_callback_ptr, index_erase_callback_ptr](std::uint32_t& all_count, std::uint32_t& removed_count) {
            return 
                [erase_callback_ptr, index_erase_callback_ptr, &all_count, &removed_count]
                (TagValueList &buf, FT_BaseIndex &index) 
            {
                auto buf_begin = buf.begin(), buf_end = buf.end();
                if (buf_begin == buf_end) {
                    return;
                }
                
                // Sort list and remove duplicate elements
                std::sort(buf_begin, buf_end);
                buf_end = std::unique(buf_begin, buf_end);

                while (buf_begin != buf_end) {
                    typename TagValueList::const_reference first_item = *buf_begin;
                    auto range_end = std::find_if(buf_begin + 1, buf_end,
                    [&first_item](typename TagValueList::const_reference item) {
                        return first_item.first != item.first;
                    });
                    // instance collection by tag pointer
                    auto tag_index_ptr = index.tryGetExistingInvertedList(first_item.first);
                    if (tag_index_ptr) {
                        // we need to remember old type nd pointer because they may be modified by bulkErase operation
                        auto old_addr = tag_index_ptr->getAddress();
                        auto old_map_value = addressOfMBIndex(*tag_index_ptr);
                        std::size_t erased_count = tag_index_ptr->bulkErase(
                            ValueIterator(buf_begin),
                            ValueIterator(range_end),
                            erase_callback_ptr
                        );
                        auto new_map_value = addressOfMBIndex(*tag_index_ptr);
                        if (old_map_value != new_map_value) {
                            // Update list ptr in index
                            auto it = index.find(first_item.first);
                            if (tag_index_ptr->getIndexType() == db0::bindex::type::empty) {
                                // remove empty inverted list completely
                                index.erase(it);
                                // notify callback on index erased
                                if (index_erase_callback_ptr) {
                                    (*index_erase_callback_ptr)(first_item.first);
                                }
                            } else {
                                it.modifyItem().value = new_map_value;
                            }
                            // remove from cache since this instance has been relocated or removed
                            index.getVObjectCache().erase(old_addr);
                        }
                        all_count -= erased_count;
                        removed_count += erased_count;
                    }
                    buf_begin = range_end;
                }
                buf.clear();
            };
        };
        
		FlushStats stats;
		{
			// 1. lock whole object for write while performing commit
			progressive_mutex::scoped_unique_lock book_lock(m_base_index_ptr->m_mutex);
			// 2. lock this BatchOperation object
			std::unique_lock<std::recursive_mutex> lock(m_mutex);
            m_commit_called = true;
            // add tags
            {
                TagValueList add_set(std::move(m_add_set));
                add(stats.m_all_inverted_lists, stats.m_new_inverted_lists)(add_set, *m_base_index_ptr);
            }
            // remove tags
            {
                TagValueList remove_set(std::move(m_remove_set));
                remove(stats.m_all_inverted_lists, stats.m_removed_inverted_lists)(remove_set, *m_base_index_ptr);
            }
		}
		return stats;
	}
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    void FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperationBuilder::reset()
    {
        if (m_batch_operation) {
            m_batch_operation->clear();
        }
        m_batch_operation = nullptr;
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
	FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperationBuilder::operator bool() const {
		return (bool)m_batch_operation;
	}

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
	bool FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperationBuilder::operator!() const {
		return !((bool)m_batch_operation);
	}

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    bool FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperationBuilder::empty() const {
        return !m_batch_operation || m_batch_operation->empty();
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    bool FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperationBuilder::assureEmpty() {
        return !m_batch_operation || m_batch_operation->assureEmpty();
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    void FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::BatchOperationBuilder::clear()
    {
        if (m_batch_operation) {
            m_batch_operation->clear();            
        }
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    void FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::TagValueBuffer::append(IndexKeyT tag, ActiveValueT value)
    {    
        assert((is_valid(value.first) || value.second) && "Either a value or value reference must be provided");
        assert(!(is_valid(value.first) && value.second) && "Both value and value reference cannot be provided");
        if (is_valid(value.first)) {
            m_values.emplace(tag, value.first);
        } else {
            m_value_refs.emplace(tag, value.second);
        }
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    bool FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::TagValueBuffer::remove(IndexKeyT tag, ActiveValueT value)
    {
        assert((is_valid(value.first) || value.second) && "Either a value or value reference must be provided");
        assert(!(is_valid(value.first) && value.second) && "Both value and value reference cannot be provided");
        if (is_valid(value.first)) {
            auto it = m_values.find({tag, value.first});
            if (it != m_values.end()) {
                m_values.erase(it);
                return true;
            }
        } else {
            auto it = m_value_refs.find({tag, value.second});
            if (it != m_value_refs.end()) {
                m_value_refs.erase(it);
                return true;
            }
        }
        return false;
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    void FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::TagValueBuffer::revert(KeyT key) {
        m_reverted.insert(key);
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    void FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::TagValueBuffer::revert(ActiveValueT value)
    {
        if (is_valid(value.first)) {
            this->revert(value.first);
        } else if (value.second) {
            assert(value.second);
            this->revert(*value.second);
        }
    }

    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    bool FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::TagValueBuffer::empty() const
    {
        if (m_values.empty() && m_value_refs.empty()) {
            return true;
        }
        // we also need to check, maybe all operations have been reverted
        for (const auto &item : m_values) {
            if (m_reverted.find(item.second) == m_reverted.end()) {
                return false; 
            }
        }
        for (const auto &item : m_value_refs) {
            // NOTE: for defunct objects value_refs may not be valid
            if (!is_valid(*item.second) || m_reverted.find(*item.second) == m_reverted.end()) {
                return false;
            }
        }
        // all values have been reverted
        return true;
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    bool FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::TagValueBuffer::assureEmpty()
    {
        if (this->empty()) {
            m_values.clear();            
            m_value_refs.clear();            
            m_reverted.clear();            
            return true;
        }
        return false;
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    void FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::TagValueBuffer::clear()
    {
        m_values.clear();
        m_value_refs.clear();
        m_reverted.clear();
    }
    
    template <typename IndexKeyT, typename KeyT, typename IndexValueT>
    FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>::TagValueList::TagValueList(TagValueBuffer &&buf)    
    {        
        if (buf.m_reverted.empty()) {
            // optimized implementation when no values have been reverted
            for (auto &item : buf.m_values) {
                this->push_back(item);
            }
            for (auto &item : buf.m_value_refs) {
                // NOTE: the 0x0 references may come from the defunct objects
                // and therefore mutest be ignored
                if (is_valid(*item.second)) {
                    this->emplace_back(item.first, *item.second);
                }
            }
        } else {
            const auto &reverted = buf.m_reverted;
            for (auto &item : buf.m_values) {
                if (reverted.find(item.second) == reverted.end()) {
                    this->push_back(item);
                }
            }
            for (auto &item : buf.m_value_refs) {
                // NOTE: the 0x0 references may come from the defunct objects
                // and therefore mutest be ignored
                if (is_valid(*item.second)) {
                    if (reverted.find(*item.second) == reverted.end()) {
                        this->emplace_back(item.first, *item.second);
                    }
                }
            }
        }
        buf.clear();
    }
    
    template class FT_BaseIndex<std::uint64_t, UniqueAddress>;
    template class FT_BaseIndex<db0::LongTagT, UniqueAddress>;

    template class FT_BaseIndex<std::uint64_t, std::uint64_t>;
    template class FT_BaseIndex<db0::LongTagT, std::uint64_t>;
    
}
