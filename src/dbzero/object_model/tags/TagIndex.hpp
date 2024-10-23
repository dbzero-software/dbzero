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
    using LongTagT = db0::LongTagT;
    
    struct [[gnu::packed]] o_tag_index: public o_fixed<o_tag_index>
    {
        std::uint64_t m_base_index_short_ptr = 0;
        std::uint64_t m_base_index_long_ptr = 0;
        std::uint64_t m_reserved[4] = { 0, 0, 0, 0 };
    };
    
    /**
     * A class to represent a full-text (tag) index and the corresponding batch-update buffer
     * typically the TagIndex instance is associated with the Class object
    */
    class TagIndex: public db0::v_object<o_tag_index>
    {
    public:
        using LangToolkit = typename Object::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        // full-text query iterator
        using QueryIterator = FT_Iterator<std::uint64_t>;
        // string tokens and classes are represented as short tags
        using ShortTagT = std::uint64_t;
        
        TagIndex(Memspace &memspace, const ClassFactory &, RC_LimitedStringPool &, VObjectCache &);
        TagIndex(mptr, const ClassFactory &, RC_LimitedStringPool &, VObjectCache &);
        
        virtual ~TagIndex();
        
        void addTag(ObjectPtr memo_ptr, ShortTagT tag_addr);

        // add a tag using long identifier
        void addTag(ObjectPtr memo_ptr, LongTagT tag_addr);

        void addTags(ObjectPtr memo_ptr, ObjectPtr const *lang_args, std::size_t nargs);

        void removeTags(ObjectPtr memo_ptr, ObjectPtr const *lang_args, std::size_t nargs);
        
        /**
         * Construct query result iterator (resolve and execute language specific query)
         * args - will be AND-combined
         * @param type optional type to match by
         * @param observer buffer to receive query observers (possibly inherited from inner queries)
         * @param no_result flag indicating if an empty query iterator should be returned
         */
        std::unique_ptr<QueryIterator> find(ObjectPtr const *args, std::size_t nargs,
            std::shared_ptr<const Class> type, std::vector<std::unique_ptr<QueryObserver> > &observers, 
            bool no_result = false) const;
        
        /**
         * Split query by all values from a specific tags_list (can be either short or long tag definitions)
         * @param lang_arg must represent a list of tags as language specific types (e.g. string / enum value etc.)
         * @return updated query iterator + observer to retrieve the active value
        */
        std::pair<std::unique_ptr<QueryIterator>, std::unique_ptr<QueryObserver> > 
        splitBy(ObjectPtr lang_arg, std::unique_ptr<QueryIterator> &&query) const;

        // Clears the uncommited contents (rollback)
        void rollback();

        // Flush any pending updates from the internal buffers
        void flush() const;
        
        // Close tag index without flushing any pending updates
        void close();

        void commit() const;
        
        void detach() const;

        db0::FT_BaseIndex<ShortTagT> &getBaseIndexShort();
        const db0::FT_BaseIndex<ShortTagT> &getBaseIndexShort() const;
        const db0::FT_BaseIndex<LongTagT> &getBaseIndexLong() const;

        void clear();
        
    private:
        using TypeId = db0::bindings::TypeId;
        using ActiveValueT = typename db0::FT_BaseIndex<ShortTagT>::ActiveValueT;
                
        RC_LimitedStringPool &m_string_pool;
        db0::FT_BaseIndex<ShortTagT> m_base_index_short;
        db0::FT_BaseIndex<LongTagT> m_base_index_long;
        // Current batch-operation buffer (may not be initialized)
        mutable db0::FT_BaseIndex<ShortTagT>::BatchOperationBuilder m_batch_operation_short;
        mutable db0::FT_BaseIndex<LongTagT>::BatchOperationBuilder m_batch_operation_long;
        // the set of tags to which the ref-count has been increased when they were first created
        mutable std::unordered_set<std::uint64_t> m_inc_refed_tags;
        // A cache of language objects held until flush/close is called
        // it's required to prevent unreferenced objects from being collected by GC
        // and to handle callbacks from the full-text index
        mutable std::unordered_map<std::uint64_t, ObjectSharedPtr> m_object_cache;
        // A cache for incomplete objects (not yet fully initialized)
        mutable std::list<std::pair<ObjectSharedPtr, std::uint64_t> > m_active_cache;
        
        template <typename BaseIndexT, typename BatchOperationT>
        BatchOperationT &getBatchOperation(ObjectPtr, BaseIndexT &, BatchOperationT &, ActiveValueT &result);
        
        db0::FT_BaseIndex<ShortTagT>::BatchOperationBuilder &getBatchOperationShort(ObjectPtr, 
            ActiveValueT &result);

        db0::FT_BaseIndex<LongTagT>::BatchOperationBuilder &getBatchOperationLong(ObjectPtr,
            ActiveValueT &result);
        
        /**
         * Make a tag from the provided argument (can be a string, type or a memo instance)
         * @return 0x0 if the tag does not exist
        */        
        ShortTagT getShortTag(ObjectPtr) const;
        ShortTagT getShortTag(ObjectSharedPtr) const;
        ShortTagT getShortTag(TypeId, ObjectPtr) const;
        ShortTagT getShortTagFromString(ObjectPtr) const;
        ShortTagT getShortTagFromMemo(ObjectPtr) const;
        ShortTagT getShortTagFromEnumValue(ObjectPtr) const;
        ShortTagT getShortTagFromFieldDef(ObjectPtr) const;
        ShortTagT getShortTagFromClass(ObjectPtr) const;

        /**
         * Adds a new object or increase ref-count of the existing element
         * @param inc_ref - whether to increase ref-count of the existing element, note that for
         * newly created elements ref-count is always set to 1 (in such case inc_ref fill be flipped from false to true)
        */
        ShortTagT addShortTag(ObjectPtr, bool &inc_ref) const;
        ShortTagT addShortTag(ObjectSharedPtr, bool &inc_ref) const;
        ShortTagT addShortTag(TypeId, ObjectPtr, bool &inc_ref) const;
        ShortTagT addShortTagFromString(ObjectPtr, bool &inc_ref) const;
        ShortTagT addShortTagFromMemo(ObjectPtr) const;

        bool addIterator(ObjectPtr, db0::FT_IteratorFactory<std::uint64_t> &factory,
            std::vector<std::unique_ptr<QueryIterator> > &neg_iterators, 
            std::vector<std::unique_ptr<QueryObserver> > &query_observers) const;
        
        bool isShortTag(ObjectPtr) const;
        bool isShortTag(ObjectSharedPtr) const;

        bool isLongTag(ObjectPtr) const;
        bool isLongTag(ObjectSharedPtr) const;
        
        LongTagT getLongTag(ObjectPtr) const;
        LongTagT getLongTag(ObjectSharedPtr) const;        

        LongTagT addLongTag(ObjectPtr, bool &inc_ref) const;
        LongTagT addLongTag(ObjectSharedPtr, bool &inc_ref) const;
        template <typename SequenceT> LongTagT makeLongTagFromSequence(const SequenceT &) const;
        
        // Check if the sequence represents a long tag (i.e. scope + short tag)
        template <typename IteratorT> bool isLongTag(IteratorT begin, IteratorT end) const;
        
        // Check if a specific parameter can be used as the scope identifieg (e.g. FieldDef)
        bool isScopeIdentifier(ObjectPtr) const;
        bool isScopeIdentifier(ObjectSharedPtr) const;

        void buildActiveValues() const;

        // adds reference to tags (string pool tokens)
        // unless such reference has already been added when the tag was first created
        void tryTagIncRef(ShortTagT tag_addr) const;
        void tryTagDecRef(ShortTagT tag_addr) const;
    };
    
    template <typename BaseIndexT, typename BatchOperationT>
    BatchOperationT &TagIndex::getBatchOperation(ObjectPtr memo_ptr, BaseIndexT &base_index, 
        BatchOperationT &batch_op, ActiveValueT &result)
    {
        if (!batch_op) {
            batch_op = base_index.beginBatchUpdate();
        }
        
        // prepare the active value only if it's not yet initialized
        if (!result.first && !result.second) {
            auto &memo = LangToolkit::getTypeManager().extractObject(memo_ptr);
            auto object_addr = memo.getAddress();
            // NOTE: that memo object may not have address before fully initialized (before postInit)
            if (object_addr) {
                // cache object locally
                if (m_object_cache.find(object_addr) == m_object_cache.end()) {
                    m_object_cache.emplace(object_addr, memo_ptr);
                }
                result = ActiveValueT(object_addr, nullptr);
            } else {
                m_active_cache.emplace_back(memo_ptr, 0);
                // use the address placeholder for an active value
                result = ActiveValueT(0, &m_active_cache.back().second);
            }
        }

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
    LongTagT TagIndex::makeLongTagFromSequence(const SequenceT &sequence) const
    {
        auto it = sequence.begin();
        auto first = *it;
        ++it;
        assert(it != sequence.end());
        return { first, *it }; 
    }

    // Get type / enum / iterable associated fixture UUID (or 0 if not prefix bound)
    std::uint64_t getFindFixtureUUID(TagIndex::ObjectPtr);

    /**
     * Resolve find parameters from user supplied arguments
     * @param args arguments passed to the find method
     * @param nargs number of arguments
     * @param find_args the resulting find arguments
     * @param type the find type (if specified). Note that type can only be specified as the 1st argument
     * @param lang_type the associated language specific type object (only returned with type), can be of a base type (e.g. MemoBase)
     * @param no_result flag to indicate that the query yields no result
     * @return the find associated fixture (or exception raised if could not be determined)
     */
    db0::swine_ptr<Fixture> getFindParams(db0::Snapshot &, TagIndex::ObjectPtr const *args, std::size_t nargs,
        std::vector<TagIndex::ObjectPtr> &find_args, std::shared_ptr<Class> &type, TagIndex::TypeObjectPtr &lang_type,
        bool &no_result);

}
