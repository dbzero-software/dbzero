// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_map>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/collections/full_text/FT_BaseIndex.hpp>
#include <dbzero/object_model/object/ObjectAnyImpl.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/core/collections/full_text/TagProduct.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/core/utils/num_pack.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include "QueryObserver.hpp"

namespace db0::object_model

{
    
    using ObjectAnyImpl = db0::object_model::ObjectAnyImpl;
    using RC_LimitedStringPool = db0::pools::RC_LimitedStringPool;
    using LongTagT = db0::LongTagT;
    class EnumFactory;

DB0_PACKED_BEGIN    
    struct DB0_PACKED_ATTR o_tag_index: public o_fixed_versioned<o_tag_index>
    {
        Address m_base_index_short_ptr = {};
        Address m_base_index_long_ptr = {};
        std::array<std::uint64_t, 4> m_reserved = { 0, 0, 0, 0 };
    };
DB0_PACKED_END

    /**
     * A class to represent a full-text (tag) index and the corresponding batch-update buffer
     * typically the TagIndex instance is associated with the Class object
    */
    class TagIndex: public db0::v_object<o_tag_index>
    {
    public:
        using super_t = db0::v_object<o_tag_index>;
        using LangToolkit = typename ObjectAnyImpl::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using ObjectSharedExtPtr = typename LangToolkit::ObjectSharedExtPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        // full-text query iterator
        using QueryIterator = FT_Iterator<UniqueAddress>;
        using TP_Iterator = TagProduct<UniqueAddress>;
        // string tokens and classes are represented as short tags
        using ShortTagT = std::uint64_t;
        
        TagIndex(Memspace &memspace, ClassFactory &, EnumFactory &, RC_LimitedStringPool &, VObjectCache &,
            std::shared_ptr<MutationLog> mutation_log);
        TagIndex(mptr, ClassFactory &, EnumFactory &, RC_LimitedStringPool &, VObjectCache &,
            std::shared_ptr<MutationLog> mutation_log);
        
        virtual ~TagIndex();
        
        // @param is_type true for implicilty assigned type tags
        void addTag(ObjectPtr memo_ptr, ShortTagT tag_addr, bool is_type);
        void addTag(ObjectPtr memo_ptr, Address tag_addr, bool is_type);
        
        // add a tag using long identifier
        void addTag(ObjectPtr memo_ptr, LongTagT tag_addr);

        void addTags(ObjectPtr memo_ptr, ObjectPtr const *lang_args, std::size_t nargs);
        
        // NOTE: type tags are removed when dropping the object, therefore lang instances are not required
        void removeTypeTag(UniqueAddress obj_addr, Address tag_addr);
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
         * @param exclusive if false, then duplicate objects will be returned (the ones tagged with multiple of split-by tags)
         * @return updated query iterator + observer to retrieve the active value
        */
        std::pair<std::unique_ptr<QueryIterator>, std::unique_ptr<QueryObserver> >
        splitBy(ObjectPtr lang_arg, std::unique_ptr<QueryIterator> &&query, bool exclusive) const;
        
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
        
        // add a defunct object (failed on __init__)
        void addDefunct(ObjectPtr memo_ptr) const;
        
        void clear();
        
        // Check if there's any queued update for the provided address
        // which may affect a future state of the object (e.g. add tags or drop)
        bool isPendingUpdate(UniqueAddress) const;

        bool empty() const;
        
        // Create a join query iterator (aka TagProduct)
        std::unique_ptr<TP_Iterator> makeTagProduct(
            const std::vector<const ObjectIterable*> &object_iterables, const ObjectIterable* tag_iterable) const;
        
        // Create a query iterator for a specific tag (e.g. a type)
        std::unique_ptr<QueryIterator> makeIterator(ObjectPtr) const;
        // Create a query from type
        std::unique_ptr<QueryIterator> makeIterator(const TagDef &) const;
        std::unique_ptr<QueryIterator> makeIterator(const Class &) const;
        std::unique_ptr<QueryIterator> makeIterator(ShortTagT) const;
        
    private:
        using TypeId = db0::bindings::TypeId;
        using ActiveValueT = typename db0::FT_BaseIndex<ShortTagT>::ActiveValueT;
        
        RC_LimitedStringPool &m_string_pool;
        ClassFactory &m_class_factory;
        EnumFactory &m_enum_factory;
        db0::FT_BaseIndex<ShortTagT> m_base_index_short;
        db0::FT_BaseIndex<LongTagT> m_base_index_long;
        // Current batch-operation buffer (may not be initialized)
        mutable db0::FT_BaseIndex<ShortTagT>::BatchOperationBuilder m_batch_op_short;
        mutable db0::FT_BaseIndex<LongTagT>::BatchOperationBuilder m_batch_op_long;
        // batch operation associated with type-tags only (auto-assigned)
        mutable db0::FT_BaseIndex<ShortTagT>::BatchOperationBuilder m_batch_op_types;
        // the set of tags to which the ref-count has been increased when they were first created
        mutable std::unordered_set<std::uint64_t> m_inc_refed_tags;
        // A cache of language objects held until flush/close is called
        // it's required to prevent unreferenced objects from being collected by GC
        // and to handle callbacks from the full-text index
        // NOTE: cache must hold "shared external" references to the objects
        mutable std::unordered_map<UniqueAddress, ObjectSharedExtPtr> m_object_cache;
        // A cache for incomplete objects (not yet fully initialized)        
        mutable std::unordered_map<ObjectPtr, UniqueAddress> m_active_cache;
        // Additional buffer to preserve / release ownership for active-cache objects
        mutable std::unordered_set<ObjectSharedExtPtr> m_active_pre_cache;
        db0::weak_swine_ptr<Fixture> m_fixture;
        // the associated fixture UUID (for validation purposes)
        const std::uint64_t m_fixture_uuid;
        mutable std::shared_ptr<MutationLog> m_mutation_log;
        
        template <typename BaseIndexT, typename BatchOperationT>
        BatchOperationT &getBatchOperation(BaseIndexT &, BatchOperationT &) const;
        
        template <typename BaseIndexT, typename BatchOperationT>
        BatchOperationT &getBatchOperation(ObjectPtr, BaseIndexT &, BatchOperationT &, ActiveValueT &result) const;
        
        db0::FT_BaseIndex<ShortTagT>::BatchOperationBuilder &getBatchOperationShort(ObjectPtr,
            ActiveValueT &result, bool is_type) const;
                
        db0::FT_BaseIndex<LongTagT>::BatchOperationBuilder &getBatchOperationLong(ObjectPtr,
            ActiveValueT &result) const;
        
        /**
         * Make a tag from the provided argument (can be a string, type or a memo instance)
         * @param alt_repr optional buffer to hold the alternative tag representation if such exists
         * this is useful for capturing the conversion result of EnumValueRepr -> EnumValue
         * @return 0x0 if the tag does not exist
        */
        ShortTagT getShortTag(ObjectPtr, ObjectSharedPtr *alt_repr = nullptr) const;
        ShortTagT getShortTag(ObjectSharedPtr, ObjectSharedPtr *alt_repr = nullptr) const;
        ShortTagT getShortTag(TypeId, ObjectPtr, ObjectSharedPtr *alt_repr = nullptr) const;
        ShortTagT getShortTagFromString(ObjectPtr) const;
        ShortTagT getShortTagFromTag(ObjectPtr) const;
        ShortTagT getShortTagFromTag(const TagDef &) const;
        ShortTagT getShortTagFromEnumValue(const EnumValue &, ObjectSharedPtr *alt_repr = nullptr) const;
        ShortTagT getShortTagFromEnumValue(ObjectPtr, ObjectSharedPtr *alt_repr = nullptr) const;
        ShortTagT getShortTagFromEnumValueRepr(ObjectPtr, ObjectSharedPtr *alt_repr = nullptr) const;
        ShortTagT getShortTagFromFieldDef(ObjectPtr) const;
        ShortTagT getShortTagFromClass(ObjectPtr) const;
        ShortTagT getShortTagFromClass(const Class &) const;
        
        /**
         * Adds a new object or increase ref-count of the existing element
         * @param inc_ref - whether to increase ref-count of the existing element, note that for
         * newly created elements ref-count is always set to 1 (in such case inc_ref fill be flipped from false to true)
         * @return nullopt if element cannot be added as short tag (must use long-tag instead)
        */
        std::optional<ShortTagT> tryAddShortTag(TypeId, ObjectPtr, bool &inc_ref) const;
        std::optional<ShortTagT> tryAddShortTag(ObjectPtr, bool &inc_ref) const;
        std::optional<ShortTagT> tryAddShortTag(ObjectSharedPtr, bool &inc_ref) const;
        ShortTagT addShortTagFromString(ObjectPtr, bool &inc_ref) const;
        // return 0x0 if object is from a different prefix (must be added as long tag)
        std::optional<ShortTagT> tryAddShortTagFromTag(ObjectPtr) const;        
        std::optional<ShortTagT> tryAddShortTagFromMemo(ObjectPtr) const;
        
        bool addIterator(ObjectPtr, db0::FT_IteratorFactory<UniqueAddress> &factory,
            std::vector<std::unique_ptr<QueryIterator> > &neg_iterators, 
            std::vector<std::unique_ptr<QueryObserver> > &query_observers) const;
        
        bool isShortTag(ObjectPtr) const;
        bool isShortTag(ObjectSharedPtr) const;
        
        bool isLongTag(ObjectPtr) const;
        bool isLongTag(ObjectSharedPtr) const;
        bool isLongTag(TypeId, ObjectPtr) const;
        
        LongTagT getLongTag(ObjectPtr) const;
        LongTagT getLongTag(TypeId, ObjectPtr) const;
        LongTagT getLongTag(ObjectSharedPtr) const;
        LongTagT getLongTagFromTag(ObjectPtr) const;
        LongTagT getLongTagFromMemo(ObjectPtr) const;
        
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
        
        // revert all pending operations associated with a specific object
        void revert(ObjectPtr) const;
        // check and if empty, clear all internal buffers (e.g. revert-ops)
        bool assureEmpty() const;                
    };
    
    template <typename BaseIndexT, typename BatchOperationT>
    BatchOperationT &TagIndex::getBatchOperation(BaseIndexT &base_index, BatchOperationT &batch_op) const
    {
        if (!batch_op) {
            batch_op = base_index.beginBatchUpdate();
        }
        return batch_op;
    }
    
    template <typename BaseIndexT, typename BatchOperationT>
    BatchOperationT &TagIndex::getBatchOperation(ObjectPtr memo_ptr, BaseIndexT &base_index, 
        BatchOperationT &batch_op, ActiveValueT &result) const
    {
        // prepare the active value only if it's not yet initialized
        if (!result.first.isValid() && !result.second) {
            auto &memo = LangToolkit::getTypeManager().extractAnyObject(memo_ptr);
            // NOTE: that memo object may not have address before fully initialized (before postInit)
            if (memo.hasInstance()) {
                auto object_addr = memo.getUniqueAddress();
                // cache object locally
                if (m_object_cache.find(object_addr) == m_object_cache.end()) {
                    m_object_cache.emplace(object_addr, memo_ptr);
                }
                result = ActiveValueT(object_addr, nullptr);
            } else {
                m_active_pre_cache.insert(memo_ptr);
                auto it = m_active_cache.emplace(memo_ptr, UniqueAddress());
                // use the address placeholder for an active value
                result = ActiveValueT(UniqueAddress(), &(it.first->second));
            }
        }
        
        return getBatchOperation(base_index, batch_op);        
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

    db0::swine_ptr<Fixture> getFindScope(db0::Snapshot &workspace, TagIndex::ObjectPtr const *args,
        std::size_t nargs, const char *prefix_name = nullptr);
    
    /**
     * Resolve find parameters from user supplied arguments
     * @param args arguments passed to the find method
     * @param nargs number of arguments
     * @param find_args the resulting find arguments
     * @param type the find type (if specified). Note that type can only be specified as the 1st argument
     * @param lang_type the associated language specific type object (only returned with type), can be of a base type (e.g. MemoBase)
     * @param no_result flag to indicate that the query yields no result
     * @param prefix_name explicitly requested scope (fixture) to use, if not provided then the scope will be determined from the arguments
     * @return the find associated fixture (or exception raised if could not be determined)
     */
    db0::swine_ptr<Fixture> getFindParams(db0::Snapshot &, TagIndex::ObjectPtr const *args, std::size_t nargs,
        std::vector<TagIndex::ObjectPtr> &find_args, std::shared_ptr<Class> &type, TagIndex::TypeObjectPtr &lang_type,
        bool &no_result, const char *prefix_name = nullptr);
    
    // Check if the object is pending update in the TagIndex withih a specific fixture
    bool isObjectPendingUpdate(db0::swine_ptr<Fixture> &fixture, UniqueAddress);
    
}
