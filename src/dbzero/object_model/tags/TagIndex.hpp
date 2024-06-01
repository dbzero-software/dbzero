#pragma once

#include <unordered_map>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/collections/full_text/FT_BaseIndex.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/core/utils/num_pack.hpp>
#include "QueryObserver.hpp"

namespace db0::object_model

{
    
    using Object = db0::object_model::Object;
    using RC_LimitedStringPool = db0::pools::RC_LimitedStringPool;
    
    /**
     * A class to represent a full-text (tag) index and the corresponding batch-update buffer
     * typically the TagIndex instance is associated with the Class object
    */
    class TagIndex
    {
    public:
        using LangToolkit = typename Object::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        // full-text query iterator
        using QueryIterator = FT_Iterator<std::uint64_t>;
        // string tokens and classes are represented as short tags
        using ShortTagT = std::uint64_t;
        // field-level tags are represented as long tags
        using LongTagT = db0::num_pack<std::uint64_t, 2u>;
        
        TagIndex(const ClassFactory &, RC_LimitedStringPool &, db0::FT_BaseIndex<ShortTagT> &, 
            db0::FT_BaseIndex<LongTagT> &);
        
        virtual ~TagIndex();
        
        void addTag(ObjectPtr memo_ptr, ShortTagT tag_addr);

        // add a tag using long identifier
        void addTag(ObjectPtr memo_ptr, LongTagT tag_addr);

        void addTags(ObjectPtr memo_ptr, ObjectPtr const *lang_args, std::size_t nargs);

        void removeTags(ObjectPtr memo_ptr, ObjectPtr const *lang_args, std::size_t nargs);
        
        /**
         * Construct query result iterator (resolve and execute language specific query)
         * args - will be AND-combined
         * @param type additional return value (if type was matched)
         * @param observer buffer to receive query observers (possibly inherited from inner queries)
         */
        std::unique_ptr<QueryIterator> find(ObjectPtr const *args, std::size_t nargs,
            std::shared_ptr<Class> &type, std::vector<std::unique_ptr<QueryObserver> > &observers) const;
        
        /**
         * Split query by all values from a specific tags_list (can be either short or long tag definitions)
         * @param lang_arg must represent a list of tags as language specific types (e.g. string / enum value etc.)
         * @return updated query iterator + observer to retrieve the active value
        */
        std::pair<std::unique_ptr<QueryIterator>, std::unique_ptr<QueryObserver> > 
        splitBy(ObjectPtr lang_arg, std::unique_ptr<QueryIterator> &&query) const;
                
        // Clears the entire contents
        void clear();

        // Flush any pending updates from the internal buffers
        void flush() const;
        
        // Close tag index without flushing any pending updates
        void close();

    private:
        using TypeId = db0::bindings::TypeId;

        const ClassFactory &m_class_factory;
        RC_LimitedStringPool &m_string_pool;
        db0::FT_BaseIndex<ShortTagT> &m_base_index_short;
        db0::FT_BaseIndex<LongTagT> &m_base_index_long;
        // Current batch-operation buffer (may not be initialized)
        mutable db0::FT_BaseIndex<ShortTagT>::BatchOperationBuilder m_batch_operation_short;
        mutable db0::FT_BaseIndex<LongTagT>::BatchOperationBuilder m_batch_operation_long;
        // A cache of language objects held until flush/close is called
        // it's required to prevent unreferenced objects from being collected by GC
        // and to handle callbacks from the full-text index
        mutable std::unordered_map<std::uint64_t, ObjectSharedPtr> m_object_cache;
                
        template <typename BaseIndexT, typename BatchOperationT> 
        BatchOperationT &getBatchOperation(ObjectPtr, BaseIndexT &, BatchOperationT &);

        db0::FT_BaseIndex<ShortTagT>::BatchOperationBuilder &getBatchOperationShort(ObjectPtr);

        db0::FT_BaseIndex<LongTagT>::BatchOperationBuilder &getBatchOperationLong(ObjectPtr);

        /**
         * Make a tag from the provided argument (can be a string, type or a memo instance)        
        */        
        ShortTagT makeShortTag(ObjectPtr) const;
        ShortTagT makeShortTag(ObjectSharedPtr) const;
        ShortTagT makeShortTag(TypeId, ObjectPtr) const;
        ShortTagT makeShortTagFromString(ObjectPtr) const;
        ShortTagT makeShortTagFromMemo(ObjectPtr) const;
        ShortTagT makeShortTagFromEnumValue(ObjectPtr) const;
        ShortTagT makeShortTagFromFieldDef(ObjectPtr) const;

        bool addIterator(ObjectPtr, db0::FT_IteratorFactory<std::uint64_t> &factory,
            std::vector<std::unique_ptr<QueryIterator> > &neg_iterators, 
            std::vector<std::unique_ptr<QueryObserver> > &query_observers) const;
        
        bool isShortTag(ObjectPtr) const;
        bool isShortTag(ObjectSharedPtr) const;

        bool isLongTag(ObjectPtr) const;
        bool isLongTag(ObjectSharedPtr) const;
        
        LongTagT makeLongTag(ObjectPtr) const;
        LongTagT makeLongTag(ObjectSharedPtr) const;

        // Check if the sequence represents a long tag (i.e. scope + short tag)
        template <typename IteratorT> bool isLongTag(IteratorT begin, IteratorT end) const;

        template <typename SequenceT> LongTagT makeLongTagFromSequence(const SequenceT &) const;

        // Check if a specific parameter can be used as the scope identifieg (e.g. FieldDef)
        bool isScopeIdentifier(ObjectPtr) const;
        bool isScopeIdentifier(ObjectSharedPtr) const;
    };

    template <typename BaseIndexT, typename BatchOperationT>
    BatchOperationT &TagIndex::getBatchOperation(ObjectPtr memo_ptr, BaseIndexT &base_index, BatchOperationT &batch_op)
    {
        auto &memo = LangToolkit::getTypeManager().extractObject(memo_ptr);
        auto object_addr = memo.getAddress();
        // cache object locally
        if (m_object_cache.find(object_addr) == m_object_cache.end()) {
            m_object_cache.emplace(object_addr, memo_ptr);
        }

        if (!batch_op) {
            batch_op = base_index.beginBatchUpdate();
        }
        batch_op->setActiveValue(object_addr);
        return batch_op;
    }
    
    template <typename IteratorT>
    bool TagIndex::isLongTag(IteratorT begin, IteratorT end) const
    {
        unsigned int index = 0;
        for (; begin != end; ++begin, ++index) {
            // first item must be the scope identifier
            if (index == 0 && !isScopeIdentifier(*begin)) {
                return false;
            }
            // second item must be a short tag
            if (index == 1 && !isShortTag(*begin)) {
                return false;
            }
            if (index > 1) {
                return false;
            }
        }
        return index == 2;
    }
    
    template <typename SequenceT>
    TagIndex::LongTagT TagIndex::makeLongTagFromSequence(const SequenceT &sequence) const
    {
        auto it = sequence.begin();
        auto first = *it;
        ++it;
        assert(it != sequence.end());
        return { first, *it }; 
    }

}
