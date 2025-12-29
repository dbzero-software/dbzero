// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "InvertedIndex.hpp"
#include "FT_IndexIterator.hpp"
#include "FT_Iterator.hpp"
#include "FT_ANDIterator.hpp"
#include "FT_ORXIterator.hpp"
#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include "LongTag.hpp"

namespace db0

{

    // FT_BaseIndex provides common API for managing tag/type inverted lists
    // @tparam IndexKeyT the tag / element's key type
    template <typename IndexKeyT, typename KeyT = UniqueAddress, typename IndexValueT = Address>
    class FT_BaseIndex: public InvertedIndex<IndexKeyT, KeyT, IndexValueT>
    {
    public:
        using super_t = InvertedIndex<IndexKeyT, KeyT, IndexValueT>;
        using self_t = FT_BaseIndex<IndexKeyT, KeyT, IndexValueT>;
                
        FT_BaseIndex() = default;
        FT_BaseIndex(Memspace &, VObjectCache &);
        FT_BaseIndex(mptr, VObjectCache &);
        FT_BaseIndex(FT_BaseIndex &&);
        
        virtual ~FT_BaseIndex() = default;
        
        /**
         * Collect iterator associated with a specific key (e.g. tag/type)
         * @return false if no iterator collected (e.g. no such key)
        */
        bool addIterator(FT_IteratorFactory<KeyT> &, IndexKeyT key) const;

        /**
         * @param key either tag or class identifier        
        */
        std::unique_ptr<FT_Iterator<KeyT> > makeIterator(IndexKeyT key, int direction = -1) const;
        
        /**
         * Match all elements from the user provided sequence
         * @param key_sequence sequence of keys (e.g. tags/types)
         * @param factory object to receive iterators
         * @param all to distinguish between all / any requirement
        */
        template <typename SequenceT>
        bool beginFind(const SequenceT &key_sequence, FT_IteratorFactory<KeyT> &factory, bool all) const
        {
            using ListT = typename super_t::ListT;
            bool result = false;
            std::vector<std::pair<IndexKeyT, const ListT*> > inverted_lists;
            for (auto key: key_sequence) {
                auto inverted_list_ptr = this->tryGetExistingInvertedList(key);
                if (inverted_list_ptr) {
                    result = true;
                    inverted_lists.emplace_back(key, inverted_list_ptr);
                } else {
                    if (all) {
                        return false;
                    }
                }
            }
            
            // build query tree
            for (const auto &inverted_list: inverted_lists) {
                // key inverted index
                factory.add(std::unique_ptr<FT_Iterator<KeyT> >(
                    new FT_IndexIterator<ListT, KeyT>(*inverted_list.second, -1, inverted_list.first))
                );
            }
            return result;
        }
        
        struct TimeStats
        {
            /// total commit time
            double m_total_time = 0;
            /// bulkInsert operation time (included in total time)
            double m_insert_time = 0;

            TimeStats &operator+=(const TimeStats &);
        };
        
        struct FlushStats
        {
            /// total number of inverted lists
            std::uint32_t m_all_inverted_lists = 0;
            /// new (unique) inverted lists added
            std::uint32_t m_new_inverted_lists = 0;
            std::uint32_t m_removed_inverted_lists = 0;
            TimeStats m_time_stats;                        
        };
        
        // either of the elements is non-null
        using ActiveValueT = std::pair<KeyT, const KeyT *>;
        
        // NOTE: values can be optionally stored as pointers due to lazy address evaluation
        // (may point to a placeholder where the actual value will be populated on flush)
        class TagValueBuffer
        {
        public:
            using ValueT = std::pair<IndexKeyT, KeyT>;
            using ValueRefT = std::pair<IndexKeyT, const KeyT *>;

            // hash specializations
            template <typename T> struct ValueHash
            {
                std::size_t operator()(const std::pair<IndexKeyT, T> &value) const {
                    return std::hash<IndexKeyT>()(value.first) ^ std::hash<T>()(value.second);
                }
            };

            std::unordered_set<std::pair<IndexKeyT, KeyT>, ValueHash<KeyT> > m_values;
            std::unordered_set<std::pair<IndexKeyT, const KeyT *>, ValueHash<const KeyT *> > m_value_refs;
            // a set of keys for which all operations should be reverted / ignored
            std::unordered_set<KeyT> m_reverted;

            void append(IndexKeyT key, ActiveValueT value);

            // @return false if not removed
            bool remove(IndexKeyT key, ActiveValueT value);
            
            void revert(KeyT);
            void revert(ActiveValueT);
            
            bool empty() const;
            bool assureEmpty();
            
            void clear();
        };
        
        class TagValueList: public std::vector<std::pair<IndexKeyT, KeyT> >
        {
        public:
            TagValueList(TagValueBuffer &&);
        };
        
        /**
         * Batch operation builder should be used for bulk-loads and optimal performance
         */
        class BatchOperation
        {
        protected :
            friend FT_BaseIndex;
            
            mutable std::recursive_mutex m_mutex;
            FT_BaseIndex *m_base_index_ptr;         
            TagValueBuffer m_add_set;
            TagValueBuffer m_remove_set;
            bool m_commit_called = false;
            
            /**
             * Constructor is protected because BatchOperation object can only be created
             * via beginTransaction call
             */
            BatchOperation(FT_BaseIndex &base_index);

        public:
            virtual ~BatchOperation();
            
            /**
             * Assign tags or types to a value (e.g. object instance)
             */
            template <typename SequenceT> void addTags(ActiveValueT value, const SequenceT &tags_or_types)
            {
                std::lock_guard<std::recursive_mutex> lock(m_mutex);
                m_commit_called = false;
                for (auto key: tags_or_types) {
                    _addTag(value, key);
                }
            }
            
            // Add a single tag
            void addTag(ActiveValueT value, IndexKeyT tag)
            {
                std::lock_guard<std::recursive_mutex> lock(m_mutex);
                m_commit_called = false;
                _addTag(value, tag);                
            }

            void removeTag(ActiveValueT value, IndexKeyT tag)
            {
                std::lock_guard<std::recursive_mutex> lock(m_mutex);
                m_commit_called = false;
                _removeTag(value, tag);                
            }

            template <typename SequenceT>
            void removeTags(ActiveValueT value, const SequenceT &tags_or_types)
            {
                std::lock_guard<std::recursive_mutex> lock(m_mutex);
                m_commit_called = false;
                for (auto key: tags_or_types) {
                    _removeTag(value, key);
                }
            }

            /**
             * Flush all updates into actual object inverted indexes
             * @param insert_callback_ptr optional callback to be called for each added object
             * @param erase_callback_ptr optional callback to be called for each removed object
             * @param index_insert_callback_ptr optional callback to be called for each new inverted list
             * @param index_erase_callback_ptr optional callback to be called for each removed inverted list
             */
            using CallbackT = std::function<void(KeyT)>;
            using IndexCallbackT = std::function<void(IndexKeyT)>;

            FlushStats flush(CallbackT *insert_callback_ptr = nullptr, 
                CallbackT *erase_callback_ptr = nullptr,
                IndexCallbackT *index_insert_callback_ptr = nullptr, 
                IndexCallbackT *index_erase_callback_ptr = nullptr);
            
            /**
             * Check if there're any operations queued for commit
             * @return
             */
            bool empty () const;
            // Check and if empty, clear all internal buffers (e.g. revert-ops)
            bool assureEmpty();
            
            // Revert ALL operations associated with a specific key
            void revert(KeyT key)
            {
                std::lock_guard<std::recursive_mutex> lock(m_mutex);
                m_commit_called = false;
                m_add_set.revert(key);
                m_remove_set.revert(key);
            }
            
            // Cancels all modifications
            void clear();

        private:
            void _addTag(ActiveValueT value, IndexKeyT tag)
            {
                // try un-removing the tag first
                if (!m_remove_set.remove(tag, value)) {
                    m_add_set.append(tag, value);
                }
            }

            void _removeTag(ActiveValueT value, IndexKeyT tag) {
                // try un-adding the tag first
                if (!m_add_set.remove(tag, value)) {
                    m_remove_set.append(tag, value);
                }
            }         
        };

        class BatchOperationBuilder
        {
            std::shared_ptr<BatchOperation> m_batch_operation;

        public :
            BatchOperationBuilder() = default;
            BatchOperationBuilder(std::shared_ptr<BatchOperation>);

            virtual ~BatchOperationBuilder() = default;
            
            BatchOperation *operator->()
            {
                assert(m_batch_operation);
                return m_batch_operation.get();
            }

            void clear();
            
            /**
             * Clear operation builder / render invalid
             */
            void reset();

            explicit operator bool() const;

            bool operator!() const;

            bool empty() const;
            bool assureEmpty();
        };

        std::shared_ptr<BatchOperation> getBatchOperation() const;

        /**
         * Initiate batch operation based update
        */
        BatchOperationBuilder beginBatchUpdate() const;
        
    protected:
        mutable progressive_mutex m_mutex;
    };
    
    extern template class FT_BaseIndex<std::uint64_t, UniqueAddress>;
    extern template class FT_BaseIndex<db0::LongTagT, UniqueAddress>;
    
    extern template class FT_BaseIndex<std::uint64_t, std::uint64_t>;
    extern template class FT_BaseIndex<db0::LongTagT, std::uint64_t>;

} 
