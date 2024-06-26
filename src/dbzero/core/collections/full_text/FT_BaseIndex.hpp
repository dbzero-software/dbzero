#pragma once

#include "InvertedIndex.hpp"
#include "FT_IndexIterator.hpp"
#include "FT_Iterator.hpp"
#include "FT_ANDIterator.hpp"
#include "FT_ORXIterator.hpp"
#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include <dbzero/core/utils/num_pack.hpp>

namespace db0

{

    // FT_BaseIndex provides common API for managing tag/type inverted lists
    // @tparam KeyT the tag / element's key type
    template <typename IndexKeyT>
    class FT_BaseIndex: public InvertedIndex<IndexKeyT, std::uint64_t, std::uint64_t>
    {
    public:
        using super_t = InvertedIndex<IndexKeyT, std::uint64_t, std::uint64_t>;

        FT_BaseIndex() = default;
        FT_BaseIndex(Memspace &, VObjectCache &);
        FT_BaseIndex(mptr, VObjectCache &);
        FT_BaseIndex(FT_BaseIndex &&);
        
        virtual ~FT_BaseIndex() = default;
        
        /**
         * Collect iterator associated with a specific key (e.g. tag/type)
         * @return false if no iterator collected (e.g. no such key)
        */
        bool addIterator(FT_IteratorFactory<std::uint64_t> &, IndexKeyT key) const;

        /**
         * @param key either tag or class identifier        
        */
        std::unique_ptr<FT_Iterator<std::uint64_t> > makeIterator(IndexKeyT key, int direction = -1) const;
        
        /**
         * Match all elements from the user provided sequence
         * @param key_sequence sequence of keys (e.g. tags/types)
         * @param factory object to receive iterators
         * @param all to distinguish between all / any requirement
        */
        template <typename SequenceT>
        bool beginFind(const SequenceT &key_sequence, FT_IteratorFactory<std::uint64_t> &factory, bool all) const
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
                factory.add(std::unique_ptr<FT_Iterator<std::uint64_t> >(
                    new FT_IndexIterator<ListT, std::uint64_t>(*inverted_list.second, -1, inverted_list.first))
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
        
        /**
         * Batch operation builder should be used for bulk-loads and optimal performance
         */
        class BatchOperation
        {
        protected :
            friend FT_BaseIndex;
            using TagValueList = std::vector<std::pair<IndexKeyT, std::uint64_t> >;
            
            mutable std::mutex m_mutex;
            FT_BaseIndex *m_base_index_ptr;            
            TagValueList m_add_set;
            TagValueList m_remove_set;
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
            template <typename SequenceT> void addTags(std::uint64_t value, const SequenceT &tags_or_types)
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_commit_called = false;
                for (auto tag_or_type: tags_or_types) {
                    m_add_set.emplace_back(tag_or_type, value);
                }
            }
            
            // Add a single tag
            void addTag(std::uint64_t value, IndexKeyT tag)
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_commit_called = false;
                m_add_set.emplace_back(tag, value);
            }

            // Add tags using the active value
            template <typename SequenceT> void addTags(const SequenceT &tags_or_types) 
            {
                assert(m_active_value);
                addTags(m_active_value, tags_or_types);
            }

            // Add a single tag using the active value
            void addTag(IndexKeyT tag) 
            {
                assert(m_active_value);
                addTag(m_active_value, tag);
            }

            template <typename SequenceT>
            void removeTags(std::uint64_t value, const SequenceT &tags_or_types)
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_commit_called = false;
                for (auto tag_or_type: tags_or_types) {
                    m_remove_set.emplace_back(tag_or_type, value);
                }
            }

            void removeTag(std::uint64_t value, IndexKeyT tag)
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_commit_called = false;                
                m_remove_set.emplace_back(tag, value);                
            }

            // Remove tags using the active value
            template <typename SequenceT> void removeTags(const SequenceT &tags_or_types) 
            {
                assert(m_active_value);
                removeTags(m_active_value, tags_or_types);
            }

            void removeTag(IndexKeyT tag) 
            {
                assert(m_active_value);
                removeTag(m_active_value, tag);
            }

            /**
             * Flush all updates into actual object inverted indexes
             */
            using CallbackT = std::function<void(std::uint64_t)>;
            FlushStats flush(CallbackT *insert_callback_ptr = nullptr, CallbackT *erase_callback_ptr = nullptr);

            /**
             * Cancel all modifications
             */
            void cancel();

            /**
             * Check if there're any operations queued for commit
             * @return
             */
            bool empty () const;

            void setActiveValue(std::uint64_t value);

        private:
            std::uint64_t m_active_value = 0;
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

            /**
             * Flush all updates into actual object inverted indexes
             */
            using CallbackT = std::function<void(std::uint64_t)>;
            FlushStats flush(CallbackT *insert_callback_ptr = nullptr, CallbackT *erase_callback_ptr = nullptr);

            /**
             * Clear operation builder / render invalid
             */
            void reset();

            explicit operator bool() const;

            bool operator!() const;

            bool empty() const;        
        };

        std::shared_ptr<BatchOperation> getBatchOperation();

        /**
         * Initiate batch operation based update
        */
        BatchOperationBuilder beginBatchUpdate();
        
    protected:
        mutable progressive_mutex mx;
        // currently pending batch operation (if any)
        std::weak_ptr<BatchOperation> m_batch_operation;
    };

    extern template class FT_BaseIndex<std::uint64_t>;
    extern template class FT_BaseIndex<db0::num_pack<std::uint64_t, 2u>>;

} 
