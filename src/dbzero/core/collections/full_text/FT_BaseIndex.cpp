#include "FT_BaseIndex.hpp"
#include "FT_IndexIterator.hpp"
#include "ConverterIteratorAdapter.hpp"
#include "InvertedIndex.hpp"

namespace db0

{
    
    FT_BaseIndex::FT_BaseIndex(Memspace & memspace) 
        : super_t(memspace)
    {
    }

    FT_BaseIndex::FT_BaseIndex(mptr ptr)
        : super_t(ptr)
    {
    }

    std::unique_ptr<FT_Iterator<std::uint64_t> > FT_BaseIndex::makeIterator(std::uint64_t key) const 
    {
        auto inverted_list_ptr = this->tryGetExistingInvertedList(key);
        if (!inverted_list_ptr) {
            return nullptr;
        }
        return std::unique_ptr<FT_Iterator<std::uint64_t> >(
            new FT_IndexIterator<ListT, std::uint64_t>(*inverted_list_ptr, -1, key));
    }

    bool FT_BaseIndex::addIterator(FT_IteratorFactory<std::uint64_t> &factory, std::uint64_t key) const
    {
        auto *inverted_list_ptr = this->tryGetExistingInvertedList(key);
        if (!inverted_list_ptr) {
            return false;
        }
        
        // key inverted index
        factory.add(std::unique_ptr<FT_Iterator<std::uint64_t> >(
            new FT_IndexIterator<ListT, std::uint64_t>(*inverted_list_ptr, -1, key))
        );
        return true;
    }

	std::shared_ptr<FT_BaseIndex::BatchOperation> FT_BaseIndex::getBatchOperation()
    {
		// either pull existing or create new BatchOperation instance
		progressive_mutex::scoped_lock lock(mx);
		std::shared_ptr<BatchOperation> result;
		for (;;) {
			lock.lock();
			result = m_batch_operation.lock();
			if (result) {
				// share existing BatchOperation instance
				break;
			}
			if (!lock.isUniqueLocked()) {
				continue;
			}
			assert(m_batch_operation.expired());
			// start new transaction
			result = std::shared_ptr<BatchOperation>(new BatchOperation(*this));
			m_batch_operation = result;
			break;
		}
		lock.release();
		return result;
	}

	FT_BaseIndex::BatchOperationBuilder FT_BaseIndex::beginBatchUpdate()
    {
		return getBatchOperation();
	}

    FT_BaseIndex::BatchOperationBuilder::BatchOperationBuilder(std::shared_ptr<BatchOperation> batch_operation)
        : m_batch_operation(batch_operation)
    {
    }

    FT_BaseIndex::BatchOperation::BatchOperation(FT_BaseIndex &base_index)
        : m_base_index_ptr(&base_index)        
    {
    }

	FT_BaseIndex::BatchOperation::~BatchOperation() {
        if (m_commit_called) {
            return;
        }

        assert(m_add_set.empty() && m_remove_set.empty() &&
            "Operation not completed properly/commit or rollback should be called");
	}

	void FT_BaseIndex::BatchOperation::cancel() 
    {
		std::unique_lock<std::mutex> lock(m_mutex);
		m_add_set.clear();
		m_remove_set.clear();
	}
    
    bool FT_BaseIndex::BatchOperation::empty () const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_add_set.empty() && m_remove_set.empty();
    }

    FT_BaseIndex::FlushStats FT_BaseIndex::BatchOperation::flush(
        std::function<void(std::uint64_t)> *insert_callback_ptr, std::function<void(std::uint64_t)> *erase_callback_ptr,
        ProcessTimer *timer)
    {
        using TagRangesVector = std::vector<TagValueList::iterator>;
        struct GetIteratorPairFirst {
            std::uint64_t operator()(TagValueList::iterator it) const {
                return it->first;
            }
        };
        using TagIterator = db0::ConverterIteratorAdapter<TagRangesVector::iterator, GetIteratorPairFirst>;

        struct GetPairSecond {
            std::uint64_t operator()(TagValueList::reference value) const {
                return value.second;
            }
        };
        using ValueIterator = db0::ConverterIteratorAdapter<TagValueList::iterator, GetPairSecond>;

        auto add =
        [insert_callback_ptr, timer](std::uint32_t &all_count, std::uint32_t &new_count) {
            return [insert_callback_ptr, timer, &all_count, &new_count](TagValueList& buf, FT_BaseIndex &index) {
                auto buf_begin = buf.begin(), buf_end = buf.end();
                if (buf_begin == buf_end) {
                    return;
                }
                ProcessTimer add_timer("FT_BaseIndex::flush/add", timer);
                // Sort list and remove duplicate elements
                std::sort(buf_begin, buf_end);
                buf_end = std::unique(buf_begin, buf_end);

                TagRangesVector tag_ranges;
                // Find ranges for all tags
                // This vector will also effectively contain all unique tags
                tag_ranges.emplace_back(buf_begin);
                std::uint64_t last_tag = buf_begin->first;
                for (auto it = buf_begin + 1; it != buf_end; ++it) {
                    if (it->first != last_tag) {
                        tag_ranges.emplace_back(it);
                        last_tag = it->first;
                    }
                }

                // Create inverted lists for tags and get corresponding iterators to them
                ProcessTimer bulk_get_timer("index.bulkGetInverteList", add_timer);
                std::vector<FT_BaseIndex::iterator> tag_index_its = index.bulkGetInvertedList(
                    TagIterator(tag_ranges.begin()),
                    TagIterator(tag_ranges.end()), 
                    &bulk_get_timer
                );
                bulk_get_timer.pause();
                assert(tag_index_its.size() == tag_ranges.size());                
                // Add end iterator to avoid special case
                tag_ranges.emplace_back(buf_end);

                for (std::size_t i = 0, n = tag_ranges.size() - 1; i < n; ++i) {
                    auto range_first = tag_ranges[i], range_last = tag_ranges[i + 1];
                    // Either create new or pull existing inverted list
                    FT_BaseIndex::iterator &tag_index_it = tag_index_its[i];
                    assert((*tag_index_it).key == range_first->first);
                    ListT tag_index = index.getInvertedList(tag_index_it);
                    auto old_addr = addressOfMBIndex(tag_index);
                    // NOTICE: only unique items are retained in index
                    // callback notified about unique items (objects)
                    std::pair<std::uint32_t, std::uint32_t> stats = tag_index.bulkInsertUnique(
                        ValueIterator(range_first), 
                        ValueIterator(range_last),
                        insert_callback_ptr
                    );
                    
                    // This check is here  because tag_index's location may have been changed by insert
                    // We need to update pointer to tag_index (either address or type changed)
                    auto new_addr = addressOfMBIndex(tag_index);
                    if (old_addr != new_addr) {
                        // update the address
                        tag_index_it.modifyItem().value = new_addr;
                        // remove from cache since this instance has been relocated
                        index.removeFromCache(range_first->first);
                    }
                    all_count += stats.first;
                    new_count += stats.second;
                }
                buf.clear();
            };
        };

        auto remove = 
        [erase_callback_ptr, timer](std::uint32_t& all_count, std::uint32_t& removed_count) {
            return [erase_callback_ptr, timer, &all_count, &removed_count](TagValueList& buf, FT_BaseIndex &index) {
                auto buf_begin = buf.begin(), buf_end = buf.end();
                if (buf_begin == buf_end) {
                    return;
                }
                ProcessTimer remove_timer("FT_BaseIndex::flush/remove", timer);
                // Sort list and remove duplicate elements
                std::sort(buf_begin, buf_end);
                buf_end = std::unique(buf_begin, buf_end);

                while (buf_begin != buf_end) {
                    TagValueList::const_reference first_item = *buf_begin;
                    auto range_end = std::find_if(buf_begin + 1, buf_end,
                    [&first_item](TagValueList::const_reference item) {
                        return first_item.first != item.first;
                    });
                    // instance collection by tag pointer
                    ListT *tag_index_ptr = index.tryGetExistingInvertedList(first_item.first);
                    if (tag_index_ptr) {
                        // we need to remember old type and pointer because they may be modified by bulkErase operation
                        auto old_addr = addressOfMBIndex(*tag_index_ptr);                        
                        std::size_t erased_count = tag_index_ptr->bulkErase(
                            ValueIterator(buf_begin), 
                            ValueIterator(range_end),
                            erase_callback_ptr
                        );
                        auto new_addr = addressOfMBIndex(*tag_index_ptr);
                        if (old_addr != new_addr) {
                            // Update list ptr in index
                            auto it = index.find(first_item.first);
                            if (tag_index_ptr->getIndexType() == db0::bindex::empty) {
                                // remove empty inverted list completely
                                index.erase(it);
                            } else {
                                it.modifyItem().value = new_addr;
                            }
                            // remove from cache since this instance has been relocated or removed
                            index.removeFromCache(first_item.first);
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
			progressive_mutex::scoped_unique_lock book_lock(m_base_index_ptr->mx);
			// 2. lock this BatchOperation object
			std::unique_lock<std::mutex> lock(m_mutex);
            m_commit_called = true;
			add(stats.m_all_inverted_lists, stats.m_new_inverted_lists)(m_add_set, *m_base_index_ptr);
            remove(stats.m_all_inverted_lists, stats.m_removed_inverted_lists)(m_remove_set, *m_base_index_ptr);
		}
		return stats;
	}

    std::uint64_t FT_BaseIndex::BatchOperation::estimateSize() const
    {
        std::uint64_t size = 0;
        constexpr std::size_t elem_size = sizeof(typename TagValueList::value_type);
        size += m_add_set.size() * elem_size;
        size += m_remove_set.size() * elem_size;
        return size;
    }

    void FT_BaseIndex::BatchOperation::setActiveValue(std::uint64_t value) {
        m_active_value = value;
    }

    FT_BaseIndex::FlushStats FT_BaseIndex::BatchOperationBuilder::flush(
        std::function<void(std::uint64_t)> *insert_callback_ptr, 
        std::function<void(std::uint64_t)> *erase_callback_ptr,
        ProcessTimer *timer)
    {
        return m_batch_operation->flush(insert_callback_ptr, erase_callback_ptr, timer);
    }

    void FT_BaseIndex::BatchOperationBuilder::reset()
    {
        if (m_batch_operation) {
            m_batch_operation->cancel();
        }
        m_batch_operation = nullptr;
    }

	FT_BaseIndex::BatchOperationBuilder::operator bool() const {
		return (bool)m_batch_operation;
	}

	bool FT_BaseIndex::BatchOperationBuilder::operator!() const {
		return !((bool)m_batch_operation);
	}
    
    bool FT_BaseIndex::BatchOperationBuilder::empty() const
    {
        return !m_batch_operation || m_batch_operation->empty();
    }

}
