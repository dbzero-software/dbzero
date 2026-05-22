// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "TagIndex.hpp"
#include "ObjectIterator.hpp"
#include "OR_QueryObserver.hpp"
#include "CompositeTagDef.hpp"
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/iterators.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/class/ClassFields.hpp>
#include <dbzero/core/collections/full_text/FT_ORXIterator.hpp>
#include <dbzero/core/collections/full_text/FT_ANDIterator.hpp>
#include <dbzero/core/collections/full_text/FT_ANDNOTIterator.hpp>
#include <dbzero/core/collections/full_text/FT_FixedKeyIterator.hpp>
#include <dbzero/object_model/tags/TagSet.hpp>
#include <dbzero/object_model/tags/TagDef.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>

namespace db0::object_model

{

    template <typename IteratorT, typename PtrT = TagIndex::ObjectPtr> class TagMakerSequence
    {
    public:
        using LangToolkit = TagIndex::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using TagMakerFunction = std::function<std::uint64_t(PtrT) >;

        TagMakerSequence(IteratorT begin, IteratorT end, TagMakerFunction tag_maker)
            : m_begin(begin)
            , m_end(end)
            , m_tag_maker(tag_maker)
        {
        }
        
        struct TagIterator
        {
            IteratorT m_value;
            const TagMakerFunction &m_tag_maker;

            TagIterator(IteratorT value, const TagMakerFunction &tag_maker)
                : m_value(value)                
                , m_tag_maker(tag_maker)
            {
            }

            bool operator!=(const TagIterator &other) const {
                return m_value != other.m_value;
            }

            void operator++() {
                ++m_value;
            }

            std::uint64_t operator*() const {
                return m_tag_maker(*m_value);
            }
        };

        TagIterator begin() const {
            return { m_begin, m_tag_maker };
        }
        
        TagIterator end() const {
            return { m_end, m_tag_maker };
        }

    private:
        IteratorT m_begin;
        IteratorT m_end;
        TagMakerFunction m_tag_maker;
    };
    
    template <typename TagT> class TagPtrSequence
    {
    public:
        TagPtrSequence(const TagT *begin, const TagT *end)
            : m_begin(begin)
            , m_end(end)
        {
        }

        const TagT *begin() const {
            return m_begin;
        }

        const TagT *end() const {
            return m_end;
        }

    private:
        const TagT *m_begin;
        const TagT *m_end;
    };
    
    class NegFactory: public FT_IteratorFactory<UniqueAddress>
    {
    public:
        using QueryIterator = FT_Iterator<UniqueAddress>;
        NegFactory(std::vector<std::unique_ptr<QueryIterator> > &neg_iterators)
            : m_neg_iterators(neg_iterators)
        {
        }
        
        void add(std::unique_ptr<QueryIterator> &&query_iterator) override
        {
            if (m_neg_iterators.empty()) {
                // add a placeholder to hold the positive query part
                m_neg_iterators.emplace_back();
            }
            m_neg_iterators.push_back(std::move(query_iterator));
        }

        std::unique_ptr<QueryIterator> release(int direction, bool lazy_init = false) override {
            THROWF(db0::InternalException) << "Invalid operation" << THROWF_END;
        }

        void clear() override {
            m_neg_iterators.clear();
        }

    private:
        std::vector<std::unique_ptr<QueryIterator> > &m_neg_iterators;
    };

    TagIndex::TagIndex(Memspace &memspace, ClassFactory &class_factory, EnumFactory &enum_factory,
        RC_LimitedStringPool &string_pool, VObjectCache &cache, std::shared_ptr<MutationLog> mutation_log)
        : db0::v_object<o_tag_index>(memspace)        
        , m_string_pool(string_pool)
        , m_class_factory(class_factory)
        , m_enum_factory(enum_factory)
        , m_cache(cache)
        , m_base_index_short(memspace, cache)
        , m_base_index_long(memspace, cache)
        , m_short_tag_index_map(std::make_unique<ShortTagIndexMap>(memspace, cache))
        , m_fixture(enum_factory.getFixture())
        , m_fixture_uuid(enum_factory.getFixture()->getUUID())
        , m_mutation_log(mutation_log)
    {
        assert(mutation_log);
        modify().m_base_index_short_ptr = m_base_index_short.getAddress();
        modify().m_base_index_long_ptr = m_base_index_long.getAddress();
        modify().m_reserved[0] = m_short_tag_index_map->getAddress().getOffset();
    }
    
    TagIndex::TagIndex(mptr ptr, ClassFactory &class_factory, EnumFactory &enum_factory,
        RC_LimitedStringPool &string_pool, VObjectCache &cache, std::shared_ptr<MutationLog> mutation_log)
        : db0::v_object<o_tag_index>(ptr)        
        , m_string_pool(string_pool)
        , m_class_factory(class_factory)
        , m_enum_factory(enum_factory)
        , m_cache(cache)
        , m_base_index_short(myPtr((*this)->m_base_index_short_ptr), cache)
        , m_base_index_long(myPtr((*this)->m_base_index_long_ptr), cache)
        , m_fixture(enum_factory.getFixture())
        , m_fixture_uuid(enum_factory.getFixture()->getUUID())
        , m_mutation_log(mutation_log)
    {
        assert(mutation_log);
        if ((*this)->m_reserved[0] != 0) {
            m_short_tag_index_map = std::make_unique<ShortTagIndexMap>(
                myPtr(Address::fromOffset((*this)->m_reserved[0])),
                cache
            );
        }
    }
    
    TagIndex::~TagIndex()
    {
        assert(
            m_batch_op_short.empty() && 
            m_batch_op_long.empty() && 
            m_batch_op_types.empty() && 
            "TagIndex::flush() or close() must be called before destruction");
    }
    
    void TagIndex::addTags(ObjectPtr memo_ptr, ObjectPtr const *args, std::size_t nargs)
    {       
        using TypeId = db0::bindings::TypeId;
        if (nargs == 0) {
            return;
        }

        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        ActiveValueT active_key = { UniqueAddress(), nullptr };
        auto &batch_op_short = getBatchOperationShort(memo_ptr, active_key, false);
        // since it's less common, defer initialization until first occurence
        db0::FT_BaseIndex<LongTagT>::BatchOperationBuilder *batch_op_long_ptr = nullptr;
        auto &type_manager = LangToolkit::getTypeManager();
        for (std::size_t i = 0; i < nargs; ++i) {
            ObjectPtr arg = args[i];
            // must check for string since it's an iterable as well
            if (!LangToolkit::isString(arg) && LangToolkit::isIterable(arg)) {
                auto tag_sequence = IterableSequence(LangToolkit::getIterator(arg), ForwardIterator::end(), [this](ObjectSharedPtr arg) {
                    bool inc_ref = false;
                    auto result = tryAddShortTag(arg.get(), inc_ref);
                    if (!result) {
                        // FIXME: implement
                        THROWF(db0::InputException) << "Unable to add foreign tag";
                    }
                    if (inc_ref) {
                        m_inc_refed_tags.insert(*result);
                    }
                    return *result;
                });
                // sequence (pair) may represent a single long tag
                if (isLongTag(arg)) {
                    if (!batch_op_long_ptr) {
                        batch_op_long_ptr = &getBatchOperationLong(memo_ptr, active_key);
                    }
                    auto tag = makeLongTagFromSequence(tag_sequence);
                    (*batch_op_long_ptr)->addTags(active_key, TagPtrSequence(&tag, &tag + 1));
                } else {
                    batch_op_short->addTags(active_key, tag_sequence);
                }
            } else {
                auto type_id = type_manager.getTypeId(arg);
                bool inc_ref = false;
                auto tag_addr = tryAddShortTag(type_id, arg, inc_ref);
                if (tag_addr) {
                    batch_op_short->addTag(active_key, *tag_addr);
                    if (inc_ref) {
                        m_inc_refed_tags.insert(*tag_addr);
                    }
                } else {
                    // must try adding as a long tag (item from a foreign scope)
                    if (!batch_op_long_ptr) {
                        batch_op_long_ptr = &getBatchOperationLong(memo_ptr, active_key);
                    }
                    auto long_tag = getLongTag(arg);
                    (*batch_op_long_ptr)->addTag(active_key, long_tag);
                }
            }            
            m_mutation_log->onDirty();            
        }
    }
    
    FT_BaseIndex<TagIndex::ShortTagT>::BatchOperationBuilder &
    TagIndex::getBatchOperationShort(ObjectPtr memo_ptr, ActiveValueT &result, bool is_type) const
    {
        if (is_type) {
            return getBatchOperation(
                memo_ptr, m_base_index_short, m_batch_op_types, result
            );
        } else {
            return getBatchOperation(
                memo_ptr, m_base_index_short, m_batch_op_short, result
            );
        }
    }

    db0::FT_BaseIndex<LongTagT>::BatchOperationBuilder &
    TagIndex::getBatchOperationLong(ObjectPtr memo_ptr, ActiveValueT &result) const
    {
        return getBatchOperation(memo_ptr, m_base_index_long, m_batch_op_long, result);
    }
    
    void TagIndex::addTag(ObjectPtr memo_ptr, Address tag_addr, bool is_type) {
        addTag(memo_ptr, tag_addr.getOffset(), is_type);
    }
    
    void TagIndex::addTag(ObjectPtr memo_ptr, ShortTagT tag, bool is_type)
    {
        ActiveValueT active_key = { UniqueAddress(), nullptr };
        auto &batch_operation = getBatchOperationShort(memo_ptr, active_key, is_type);
        batch_operation->addTags(active_key, TagPtrSequence(&tag, &tag + 1));        
        m_mutation_log->onDirty();
    }    
    
    void TagIndex::addTag(ObjectPtr memo_ptr, LongTagT tag)
    {
        ActiveValueT active_key = { UniqueAddress(), nullptr };
        auto &batch_operation = getBatchOperationLong(memo_ptr, active_key);
        batch_operation->addTags(active_key, TagPtrSequence(&tag, &tag + 1));        
        m_mutation_log->onDirty();        
    }

    std::shared_ptr<TagIndex> TagIndex::addComposite(ObjectPtr, ShortTagT tag)
    {
        return getShortTagIndexMap().findOrCreate(
            tag,
            m_class_factory,
            m_enum_factory,
            m_string_pool,
            m_cache,
            m_mutation_log
        );
    }

    std::shared_ptr<TagIndex> TagIndex::tryUpdateComposite(ObjectPtr, ShortTagT tag)
    {
        if (!m_short_tag_index_map) {
            return nullptr;
        }

        return m_short_tag_index_map->tryGet(
            tag,
            m_class_factory,
            m_enum_factory,
            m_string_pool,
            m_cache,
            m_mutation_log
        );
    }

    TagIndex::ShortTagT TagIndex::addCompositeKey(ObjectPtr arg)
    {
        bool inc_ref = false;
        auto result = tryAddShortTag(arg, inc_ref);
        if (!result) {
            THROWF(db0::InputException) << "Unable to add foreign composite tag";
        }
        if (inc_ref) {
            m_inc_refed_tags.insert(*result);
        }
        return *result;
    }

    TagIndex::ShortTagT TagIndex::getCompositeKey(ObjectPtr arg) const
    {
        auto result = tryGetCompositeKey(arg);
        if (!result) {
            THROWF(db0::InputException) << "Unable to resolve foreign composite tag key" << THROWF_END;
        }
        return *result;
    }
    
    void TagIndex::removeTypeTag(UniqueAddress obj_addr, Address tag_addr)
    {
        auto &batch_operation = getBatchOperation(m_base_index_short, m_batch_op_types);
        batch_operation->removeTag({ obj_addr, nullptr }, tag_addr.getOffset());
        m_mutation_log->onDirty();
    }
    
    void TagIndex::removeTags(ObjectPtr memo_ptr, ObjectPtr const *args, std::size_t nargs)
    {
        if (nargs == 0) {
            return;
        }

        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        ActiveValueT active_key = { UniqueAddress(), nullptr };
        auto &batch_operation = getBatchOperationShort(memo_ptr, active_key, false);
        for (std::size_t i = 0; i < nargs; ++i) {
            auto type_id = LangToolkit::getTypeManager().getTypeId(args[i]);
            // must check for string since it's an iterable as well
            if (type_id != TypeId::STRING && LangToolkit::isIterable(args[i])) {
                batch_operation->removeTags(active_key,
                    IterableSequence(LangToolkit::getIterator(args[i]), ForwardIterator::end(), [&](ObjectSharedPtr arg) {
                        return getCompositeKey(arg.get());
                    })
                );
                m_mutation_log->onDirty();
            } else {
                batch_operation->removeTag(active_key, getCompositeKey(args[i]));
                m_mutation_log->onDirty();
            }
        }
    }
    
    void TagIndex::rollback()
    {
        if (m_short_tag_index_map) {
            m_short_tag_index_map->forEachActive([](TagIndex &tag_index) {
                tag_index.rollback();
            });
        }

        // Reject any pending updates
        if (m_batch_op_short) {
            m_batch_op_short.reset();
        }
        if (m_batch_op_long) {
            m_batch_op_long.reset();
        }
        if (m_batch_op_types) {
            m_batch_op_types.reset();
        }
        // undo inc-ref
        for (auto tag_addr: m_inc_refed_tags) {
            m_string_pool.unRefByAddr(tag_addr);
        }
        // NOTE: also need to clear buffers which may contain incomplete objects
        m_object_cache.clear();
        m_active_cache.clear();
        m_active_pre_cache.clear();
    }

    void TagIndex::clear()
    {
        rollback();
        m_base_index_short.clear();
        m_base_index_long.clear();        
    }
    
    void TagIndex::close()
    {
        if (m_short_tag_index_map) {
            m_short_tag_index_map->forEachActive([](TagIndex &tag_index) {
                tag_index.close();
            });
        }

        if (m_batch_op_short) {
            m_batch_op_short.reset();
        }
        if (m_batch_op_long) {
            m_batch_op_long.reset();
        }
        if (m_batch_op_types) {
            m_batch_op_types.reset();
        }
        m_object_cache.clear();
        m_active_cache.clear();
        m_active_pre_cache.clear();
        m_inc_refed_tags.clear();
    }
    
    void TagIndex::tryTagIncRef(ShortTagT tag_addr) const
    {
        if (m_string_pool.isTokenAddr(Address::fromOffset(tag_addr)) && 
            m_inc_refed_tags.find(tag_addr) == m_inc_refed_tags.end()) 
        {
            m_string_pool.addRefByAddr(tag_addr);
        }
    }
    
    void TagIndex::tryTagDecRef(ShortTagT tag_addr) const
    {
        if (m_string_pool.isTokenAddr(Address::fromOffset(tag_addr))) {
            m_string_pool.unRefByAddr(tag_addr);
        }
    }
    
    bool TagIndex::flush() const
    {
        using ShortBatchOperationBulder = db0::FT_BaseIndex<ShortTagT>::BatchOperationBuilder;

        bool composite_indexes_contain_values = false;
        auto hasPersistedValues = [&]() {
            return !m_base_index_short.empty() || !m_base_index_long.empty() || composite_indexes_contain_values;
        };

        if (m_short_tag_index_map) {
            std::vector<ShortTagT> emptyCompositeTags;
            m_short_tag_index_map->forEachActive([this, &composite_indexes_contain_values, &emptyCompositeTags](TagIndex &tag_index) {
                if (tag_index.flush()) {
                    composite_indexes_contain_values = true;
                    return;
                }

                auto tag_index_address = tag_index.getAddress();
                for (auto it = m_short_tag_index_map->begin(); it != m_short_tag_index_map->end(); ++it) {
                    if ((*it).value == tag_index_address) {
                        emptyCompositeTags.push_back((*it).key);
                        return;
                    }
                }
            });
            for (auto tag: emptyCompositeTags) {
                m_short_tag_index_map->erase(
                    tag,
                    m_class_factory,
                    m_enum_factory,
                    m_string_pool,
                    m_cache,
                    m_mutation_log
                );
            }
        }
        
        if (empty()) {
            return hasPersistedValues();
        }
                
        // this is to resolve addresses of incomplete objects (must be done before flushing)
        buildActiveValues();
        // NOTE: some object might've been dropped in the meantime, need to be reverted from batch operations        
        for (const auto &item: m_object_cache) {
            auto obj_ptr = item.second.get();
            if (LangToolkit::isMemoDead(obj_ptr)) {
                revert(obj_ptr);
            }
        }
        
        // might be empty after clean-ups, check again
        if (!assureEmpty()) {
            // the purpose of callback is to incRef objects when a new tag is assigned
            std::function<void(UniqueAddress)> add_tag_callback = [&](UniqueAddress obj_addr) {
                auto it = m_object_cache.find(obj_addr);
                assert(it != m_object_cache.end());
                // NOTE: inc-ref as tag
                LangToolkit::incRefMemo(true, it->second.get());
            };
            
            // add_index_callback adds reference to tags (string pool tokens)
            // unless such reference has already been added when the tag was first created
            std::function<void(ShortTagT)> add_index_callback = [&](ShortTagT tag_addr) {
                tryTagIncRef(tag_addr);
            };
            
            auto &batch_op_types = getBatchOperation(m_base_index_short, m_batch_op_types);
            std::function<void(UniqueAddress)> remove_tag_callback = [&](UniqueAddress obj_addr) {
                auto it = m_object_cache.find(obj_addr);
                // object may not exist if tags are removed post-deletion
                if (it != m_object_cache.end()) {
                    auto obj_ptr = it->second.get();
                    // NOTE: we check for acutal language references (excluding LangCache + TagIndex)
                    if (LangToolkit::decRefMemo(true, obj_ptr) && !LangToolkit::hasAnyLangRefs(obj_ptr, 2)) {
                        // if object is pending deletion, remove all type tags as well
                        // we might skip this operation and leave it to Object's dropTags function
                        // but it will be more efficient to do it here
                        const Class *type_ptr = &LangToolkit::getMemoType(obj_ptr);
                        while (type_ptr) {
                            batch_op_types->removeTag({ obj_addr, nullptr }, type_ptr->getAddress().getOffset());
                            type_ptr = type_ptr->getBaseClassPtr();
                        }
                    }
                }
            };
            
            std::function<void(ShortTagT)> erase_index_callback = [&](ShortTagT tag_addr) {
                tryTagDecRef(tag_addr);
            };
            
            // flush all short tags' updates
            if (!m_batch_op_short.assureEmpty()) {
                m_batch_op_short->flush(&add_tag_callback, &remove_tag_callback,
                    &add_index_callback, &erase_index_callback);
            }
            
            std::function<void(LongTagT)> add_long_index_callback = [&](LongTagT long_tag_addr) {
                tryTagIncRef(long_tag_addr[0]);
                tryTagIncRef(long_tag_addr[1]);
            };

            std::function<void(LongTagT)> erase_long_index_callback = [&](LongTagT long_tag_addr) {
                tryTagDecRef(long_tag_addr[0]);
                tryTagDecRef(long_tag_addr[1]);
            };
            
            // flush all long tags' updates
            if (!m_batch_op_long.assureEmpty()) {
                m_batch_op_long->flush(&add_tag_callback, &remove_tag_callback,
                    &add_long_index_callback, &erase_long_index_callback);
            }
            
            if (!m_batch_op_types.assureEmpty()) {
                // Scan fully-initialized objects and revert type tags for those with no remaining refs.
                // Mid-init objects are in m_active_pre_cache, not m_object_cache, so they are unaffected.
                for (const auto &item: m_object_cache) {
                    auto obj_ptr = item.second.get();
                    // NOTE: dropped instances should've already been reverted by now
                    // NOTE: we check for actual language references (excluding LangCache + TagIndex)
                    if (!LangToolkit::isMemoDropped(obj_ptr) && !LangToolkit::hasMemoAnyRefs(obj_ptr)
                        && !LangToolkit::hasAnyLangRefs(obj_ptr, 2)) {
                        m_batch_op_types->revert(LangToolkit::getMemoUniqueAddress(obj_ptr));
                    }
                }
                // flush all type-tag updates
                if (!m_batch_op_types.assureEmpty()) {
                    // NOTE: we don't pass any remove_tag_callback since type tags are only removed when objects are dropped
                    m_batch_op_types->flush(&add_tag_callback, nullptr, &add_index_callback, &erase_index_callback);
                }
            }
        }

        m_object_cache.clear();
        // Keep mid-init entries (zero-addr placeholder) for the next flush cycle;
        // erase only entries that have been resolved to a real address.
        // Pre-cache ref was born in getBatchOperation; drop it here at resolution time.
        for (auto it = m_active_cache.begin(); it != m_active_cache.end(); ) {
            if (it->second.isValid()) {
                m_active_pre_cache.erase(ObjectSharedExtPtr(it->first));
                it = m_active_cache.erase(it);
            } else {
                ++it;
            }
        }
        m_inc_refed_tags.clear();
        return hasPersistedValues();
    }
    
    void TagIndex::buildActiveValues() const
    {
        for (auto &item: m_active_cache) {
            // NOTE: defunct objects and mid-init objects (still in __init__, no address yet)
            // must be skipped — their placeholder stays zero and is preserved for the next flush.
            if (!LangToolkit::isMemoDead(item.first) && LangToolkit::hasMemoInstance(item.first)) {
                auto object_addr = LangToolkit::getMemoUniqueAddress(item.first);
                assert(object_addr.isValid());
                // initialize active value with the actual object address
                item.second = object_addr;
                // add object to cache
                if (m_object_cache.find(object_addr) == m_object_cache.end()) {
                    m_object_cache.emplace(object_addr, item.first);
                }
            }
        }        
    }
    
    std::unique_ptr<TagIndex::QueryIterator> TagIndex::find(ObjectPtr const *args, std::size_t nargs,
        std::shared_ptr<const Class> type, std::vector<std::unique_ptr<QueryObserver> > &observers, bool no_result) const
    {
        db0::FT_ANDIteratorFactory<UniqueAddress> factory;
        // the negated root-level query components
        std::vector<std::unique_ptr<QueryIterator> > neg_iterators;
        if (nargs > 0 || type) {
            // flush pending updates before querying
            flush();
            // if the 1st argument is a type then resolve as a typed ObjectIterable
            std::size_t offset = 0;
            bool result = !no_result;
            // apply type filter if provided (unless type is a MemoBase)
            if (type) {
                result &= m_base_index_short.addIterator(factory, type->getAddress().getOffset());
            }
            
            while (result && (offset < nargs)) {
                result &= addIterator(args[offset], factory, neg_iterators, observers);
                ++offset;
            }
            if (!result) {
                // invalidate factory since no matching results exist
                factory.clear();
            }
        }
        
        auto query_iterator = factory.release();
        // handle negated query components
        if (neg_iterators.empty()) {
            return query_iterator;
        } else {
            if (!query_iterator) {
                THROWF(db0::InputException) << "Negated query components are not supported without positive components" << THROWF_END;
            }
            // put query iterator in the first position (for which the placeholder was added)
            neg_iterators[0] = std::move(query_iterator);
            // construct AND-not query iterator
            return std::make_unique<FT_ANDNOTIterator<UniqueAddress> >(std::move(neg_iterators), -1);
        }
    }
    
    bool TagIndex::addIterator(ObjectPtr arg, db0::FT_IteratorFactory<UniqueAddress> &factory,
        std::vector<std::unique_ptr<QueryIterator> > &neg_iterators, std::vector<std::unique_ptr<QueryObserver> > &query_observers) const
    {
        using TypeId = db0::bindings::TypeId;
        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        
        auto type_id = LangToolkit::getTypeManager().getTypeId(arg);
        if (type_id == TypeId::DB0_COMPOSITE_TAG) {
            return addCompositeIterator(LangToolkit::getTypeManager().extractCompositeTag(arg), factory, query_observers);
        }

        // simple tag-convertible type
        if (type_id == TypeId::STRING || type_id == TypeId::DB0_TAG || type_id == TypeId::DB0_ENUM_VALUE || 
            type_id == TypeId::DB0_CLASS)
        {
            if (isLongTag(type_id, arg)) {
                // query as the long-tag
                return m_base_index_long.addIterator(factory, getLongTag(type_id, arg));
            } else {
                return m_base_index_short.addIterator(factory, getShortTag(type_id, arg));
            }
        }
        
        // Memo instance is directly fed into the FT_FixedKeyIterator
        if (type_id == TypeId::MEMO_OBJECT || type_id == TypeId::MEMO_IMMUTABLE_OBJECT) {
            auto addr = LangToolkit::getMemoUniqueAddress(arg);
            factory.add(std::make_unique<FT_FixedKeyIterator<UniqueAddress> >(&addr, &addr + 1));
            return true;
        }
        
        // a python iterable
        if (type_id == TypeId::LIST || type_id == TypeId::TUPLE) {
            // check if an iterable can be converted into a long tag and attach to query if yes
            if (isLongTag<ForwardIterator>(LangToolkit::getIterator(arg), ForwardIterator::end())) {
                IterableSequence sequence(LangToolkit::getIterator(arg), ForwardIterator::end(), [&](ObjectSharedPtr arg) {
                    bool inc_ref = false;
                    auto result = tryAddShortTag(arg.get(), inc_ref);
                    if (!result) {
                        THROWF(db0::InputException) << "Unable to add foreign tag";
                    }
                    if (inc_ref) {
                        m_inc_refed_tags.insert(*result);                        
                    }
                    return *result;
                });
                return m_base_index_long.addIterator(factory, makeLongTagFromSequence(sequence));
            }
            
            bool is_or_clause = (type_id == TypeId::LIST);
            // lists corresponds to OR operator, tuple - to AND
            std::unique_ptr<FT_IteratorFactory<UniqueAddress> > inner_factory;
            if (is_or_clause) {
                inner_factory = std::make_unique<db0::FT_ORXIteratorFactory<UniqueAddress> >();
            } else {
                inner_factory = std::make_unique<db0::FT_ANDIteratorFactory<UniqueAddress> >();
            }
            std::vector<std::unique_ptr<QueryIterator> > inner_neg_iterators;
            bool any = false;
            bool all = true;
            ForwardIterator it(LangToolkit::getIterator(arg));
            for (auto end = ForwardIterator::end(); it != end; ++it) {
                bool result = addIterator((*it).get(), *inner_factory, inner_neg_iterators, query_observers);
                any |= result;
                all &= result;
            }
            if (!inner_neg_iterators.empty()) {
                // FIXME: not implemented
                THROWF(db0::InputException) << "not implemented" << THROWF_END;
            }
            if (is_or_clause && !any) {
                return false;
            }
            // all components must be present with AND-clause
            if (!is_or_clause && !all) {
                return false;
            }

            // add constructed AND/OR query part
            factory.add(inner_factory->release(-1));
            return true;
        }
        
        if (type_id == TypeId::OBJECT_ITERABLE) {
            auto &obj_iter = LangToolkit::getTypeManager().extractObjectIterable(arg);
            // try interpreting the iterator as FT-query
            auto ft_query = obj_iter.beginFTQuery(query_observers, -1);
            if (!ft_query || ft_query->isEnd()) {
                return false;
            }
            factory.add(std::move(ft_query));
            return true;
        }
        
        if (type_id == TypeId::DB0_TAG_SET) {
            // collect negated iterators to be merged later
            auto &tag_set = LangToolkit::getTypeManager().extractTagSet(arg);
            std::vector<std::unique_ptr<QueryIterator> > inner_neg_iterators;
            if (tag_set.isNegated()) {
                NegFactory neg_factory(neg_iterators);
                for (auto &arg: tag_set.getArgs()) {
                    addIterator(arg.get(), neg_factory, inner_neg_iterators, query_observers);
                }
            } else {
                // just add as regular iterators
                for (auto &arg: tag_set.getArgs()) {
                    addIterator(arg.get(), factory, inner_neg_iterators, query_observers);
                }
            }
            if (!inner_neg_iterators.empty()) {
                // FIXME: not implemented
                THROWF(db0::InputException) << "not implemented" << THROWF_END;
            }
            return true;
        }
        
        THROWF(db0::InputException) << "Unable to interpret object of type: " << LangToolkit::getTypeName(arg)
            << " as a query" << THROWF_END;
    }

    bool TagIndex::addCompositeIterator(const CompositeTagDef &tag,
        db0::FT_IteratorFactory<UniqueAddress> &factory,
        std::vector<std::unique_ptr<QueryObserver> > &query_observers) const
    {
        if (tag.size() < 2 || !m_short_tag_index_map) {
            return false;
        }

        auto const &items = tag.getItems();
        auto firstKey = tryGetCompositeKey(items[0].get());
        if (!firstKey) {
            return false;
        }

        auto currentTagIndexPtr = m_short_tag_index_map->tryGet(
            *firstKey,
            m_class_factory,
            m_enum_factory,
            m_string_pool,
            m_cache,
            m_mutation_log
        );
        if (!currentTagIndexPtr) {
            return false;
        }

        auto *currentTagIndex = currentTagIndexPtr.get();
        for (std::size_t i = 1; i + 1 < items.size(); ++i) {
            auto key = currentTagIndex->tryGetCompositeKey(items[i].get());
            if (!key || !currentTagIndex->m_short_tag_index_map) {
                return false;
            }
            currentTagIndexPtr = currentTagIndex->m_short_tag_index_map->tryGet(
                *key,
                currentTagIndex->m_class_factory,
                currentTagIndex->m_enum_factory,
                currentTagIndex->m_string_pool,
                currentTagIndex->m_cache,
                currentTagIndex->m_mutation_log
            );
            if (!currentTagIndexPtr) {
                return false;
            }
            currentTagIndex = currentTagIndexPtr.get();
        }

        std::vector<std::unique_ptr<QueryIterator> > negIterators;
        auto result = currentTagIndex->addIterator(items.back().get(), factory, negIterators, query_observers);
        if (!negIterators.empty()) {
            THROWF(db0::InputException) << "Negated composite tag leaves are not supported" << THROWF_END;
        }
        return result;
    }

    std::optional<TagIndex::ShortTagT> TagIndex::tryGetCompositeKey(ObjectPtr arg) const
    {
        using TypeId = db0::bindings::TypeId;

        auto typeId = LangToolkit::getTypeManager().getTypeId(arg);
        if (typeId == TypeId::MEMO_OBJECT || typeId == TypeId::MEMO_IMMUTABLE_OBJECT) {
            return tryAddShortTagFromMemo(arg);
        }
        if (typeId == TypeId::DB0_TAG) {
            return tryAddShortTagFromTag(arg);
        }
        if (typeId == TypeId::STRING || typeId == TypeId::DB0_ENUM_VALUE || typeId == TypeId::DB0_ENUM_VALUE_REPR ||
            typeId == TypeId::DB0_FIELD_DEF || typeId == TypeId::DB0_CLASS)
        {
            return getShortTag(typeId, arg);
        }
        return std::nullopt;
    }
    
    TagIndex::ShortTagT TagIndex::getShortTag(TypeId type_id, ObjectPtr py_arg, ObjectSharedPtr *alt_repr) const
    {
        if (type_id == TypeId::STRING) {
            return getShortTagFromString(py_arg);
        } else if (type_id == TypeId::DB0_TAG) {
            return getShortTagFromTag(py_arg);
        } else if (type_id == TypeId::DB0_ENUM_VALUE) {
            return getShortTagFromEnumValue(py_arg, alt_repr);
        } else if (type_id == TypeId::DB0_ENUM_VALUE_REPR) {
            return getShortTagFromEnumValueRepr(py_arg, alt_repr);
        } else if (type_id == TypeId::DB0_FIELD_DEF) {
            return getShortTagFromFieldDef(py_arg);
        } else if (type_id == TypeId::DB0_CLASS) {
            return getShortTagFromClass(py_arg);
        } else if (type_id == TypeId::MEMO_OBJECT || type_id == TypeId::MEMO_IMMUTABLE_OBJECT) {
            return LangToolkit::getMemoUniqueAddress(py_arg).getAddress().getOffset();
        }
        THROWF(db0::InputException) << "Unable to interpret object of type: " << LangToolkit::getTypeName(py_arg)
            << " as a tag" << THROWF_END;
    }

    TagIndex::ShortTagT TagIndex::getShortTagFromEnumValueRepr(ObjectPtr py_arg, ObjectSharedPtr *alt_repr) const
    {
        // try translating enum value-repr to enum value
        auto &enum_value_repr = LangToolkit::getTypeManager().extractEnumValueRepr(py_arg);
        if (alt_repr) {
            *alt_repr = m_enum_factory.tryGetEnumLangValue(enum_value_repr);
            // value-repr associated tags don't exist
            if (!*alt_repr) {
                return {};
            }
            return getShortTagFromEnumValue(LangToolkit::getTypeManager().extractEnumValue(alt_repr->get()));
        } else {
            auto enum_value = m_enum_factory.tryGetEnumValue(enum_value_repr);
            // enum value-repr associated tags don't exist
            if (!enum_value) {
                return {};
            }
            // and so get the tag from the enum value
            return getShortTagFromEnumValue(*enum_value);
        }
    }
    
    TagIndex::ShortTagT TagIndex::getShortTag(ObjectPtr py_arg, ObjectSharedPtr *alt_repr) const
    {
        auto type_id = LangToolkit::getTypeManager().getTypeId(py_arg);
        return getShortTag(type_id, py_arg, alt_repr);
    }
    
    TagIndex::ShortTagT TagIndex::getShortTagFromString(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isString(py_arg));
        return LangToolkit::getTagFromString(py_arg, m_string_pool);
    }
    
    TagIndex::ShortTagT TagIndex::getShortTagFromTag(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isTag(py_arg));
        // NOTE: we use only the offset part as tag - to distinguish from enum and class tags (high bits)
        return LangToolkit::getTypeManager().extractTag(py_arg).getAddress(m_class_factory).getOffset();
    }

    TagIndex::ShortTagT TagIndex::getShortTagFromTag(const TagDef &tag_def) const {
        // NOTE: we use only the offset part as tag - to distinguish from enum and class tags (high bits)
        return tag_def.getAddress(m_class_factory).getOffset();
    }

    TagIndex::ShortTagT TagIndex::getShortTagFromEnumValue(const EnumValue &enum_value, ObjectSharedPtr *alt_repr) const
    {
        assert(enum_value);
        if (!db0::is_same(enum_value.m_fixture, m_fixture)) {
            // migrate to a different prefix if needed
            if (alt_repr) {
                *alt_repr = m_enum_factory.tryMigrateEnumLangValue(enum_value);
                if (!*alt_repr) {
                    // tag does not exist
                    return {};
                }
                return LangToolkit::getTypeManager().extractEnumValue(alt_repr->get()).getUID().asULong();
            } else {
                auto value = m_enum_factory.tryMigrateEnumValue(enum_value);
                if (!value) {
                    // tag does not exist
                    return {};
                }
                return (*value).getUID().asULong();
            }
        }
        return enum_value.getUID().asULong();
    }
    
    TagIndex::ShortTagT TagIndex::getShortTagFromEnumValue(ObjectPtr py_arg, ObjectSharedPtr *alt_repr) const
    {
        assert(LangToolkit::isEnumValue(py_arg));
        return getShortTagFromEnumValue(LangToolkit::getTypeManager().extractEnumValue(py_arg), alt_repr);
    }

    TagIndex::ShortTagT TagIndex::getShortTagFromClass(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isClassObject(py_arg));
        return getShortTagFromClass(*LangToolkit::getTypeManager().extractConstClass(py_arg));
    }
    
    TagIndex::ShortTagT TagIndex::getShortTagFromClass(const Class &type) const {
        return type.getAddress().getOffset();
    }

    TagIndex::ShortTagT TagIndex::getShortTagFromFieldDef(ObjectPtr py_arg) const
    {
        auto &field_def = LangToolkit::getTypeManager().extractFieldDef(py_arg);
        // class UID (32bit) + primary field ID (32 bit)
        return (static_cast<std::uint64_t>(field_def.m_class_uid) << 32) | field_def.m_member.getLongIndex();
    }
    
    TagIndex::ShortTagT TagIndex::getShortTag(ObjectSharedPtr py_arg, ObjectSharedPtr *alt_repr) const {
        return getShortTag(py_arg.get(), alt_repr);
    }
    
    std::optional<TagIndex::ShortTagT> TagIndex::tryAddShortTag(ObjectPtr py_arg, bool &inc_ref) const
    {
        auto type_id = LangToolkit::getTypeManager().getTypeId(py_arg);
        return tryAddShortTag(type_id, py_arg, inc_ref);
    }

    std::optional<TagIndex::ShortTagT> TagIndex::tryAddShortTag(ObjectSharedPtr py_arg, bool &inc_ref) const {
        return tryAddShortTag(py_arg.get(), inc_ref);
    }
    
    std::optional<TagIndex::ShortTagT> TagIndex::tryAddShortTag(TypeId type_id, ObjectPtr py_arg, bool &inc_ref) const
    {
        if (type_id == TypeId::STRING) {
            return addShortTagFromString(py_arg, inc_ref);
        } else if (type_id == TypeId::MEMO_OBJECT || type_id == TypeId::MEMO_IMMUTABLE_OBJECT) {
            return tryAddShortTagFromMemo(py_arg);
        } else if (type_id == TypeId::DB0_TAG) {
            return tryAddShortTagFromTag(py_arg);
        } else if (type_id == TypeId::DB0_ENUM_VALUE) {
            return getShortTagFromEnumValue(py_arg);
        } else if (type_id == TypeId::DB0_FIELD_DEF) {
            return getShortTagFromFieldDef(py_arg);
        } else if (type_id == TypeId::DB0_CLASS) {
            return getShortTagFromClass(py_arg);
        }
        THROWF(db0::InputException) << "Unable to interpret object of type: " << LangToolkit::getTypeName(py_arg)
            << " as a tag" << THROWF_END;
    }
    
    TagIndex::ShortTagT TagIndex::addShortTagFromString(ObjectPtr py_arg, bool &inc_ref) const
    {
        assert(LangToolkit::isString(py_arg));
        return LangToolkit::addTagFromString(py_arg, m_string_pool, inc_ref);
    }
    
    std::optional<TagIndex::ShortTagT> TagIndex::tryAddShortTagFromMemo(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isAnyMemoObject(py_arg));
        if (LangToolkit::getFixtureUUID(py_arg) != m_fixture_uuid) {
            // must be added as long tag
            return std::nullopt;
        }
        // NOTE: we use only the offset part as tag - to distinguish from enum and class tags (high bits)
        return LangToolkit::getMemoUniqueAddress(py_arg).getAddress().getOffset();
    }
    
    std::optional<TagIndex::ShortTagT> TagIndex::tryAddShortTagFromTag(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isTag(py_arg));
        auto &py_tag = LangToolkit::getTypeManager().extractTag(py_arg);
        auto addr_pair = py_tag.getLongAddress(m_class_factory);
        if (addr_pair.first != m_fixture_uuid) {
            // must be added as long tag
            return std::nullopt;
        }
        return addr_pair.second;
    }
    
    bool TagIndex::isScopeIdentifier(ObjectPtr ptr) const {
        return LangToolkit::isFieldDef(ptr);
    }

    bool TagIndex::isScopeIdentifier(ObjectSharedPtr ptr) const {
        return isScopeIdentifier(ptr.get());
    }

    bool TagIndex::isShortTag(ObjectPtr py_arg) const
    {
        auto type_id = LangToolkit::getTypeManager().getTypeId(py_arg);
        return type_id == TypeId::STRING || type_id == TypeId::MEMO_OBJECT ||
            type_id == TypeId::MEMO_IMMUTABLE_OBJECT || type_id == TypeId::DB0_ENUM_VALUE ||
            type_id == TypeId::DB0_FIELD_DEF || type_id == TypeId::DB0_ENUM_VALUE_REPR;
    }

    bool TagIndex::isShortTag(ObjectSharedPtr ptr) const {
        return isShortTag(ptr.get());
    }
    
    std::pair<std::unique_ptr<TagIndex::QueryIterator>, std::unique_ptr<QueryObserver> >
    TagIndex::splitBy(ObjectPtr py_arg, std::unique_ptr<QueryIterator> &&query, bool exclusive) const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        auto type_id = type_manager.getTypeId(py_arg);
        // must check for string since it's is an iterable as well
        if (type_id == TypeId::STRING || !LangToolkit::isIterable(py_arg)) {
            THROWF(db0::InputException) << "Invalid argument type: " << LangToolkit::getTypeName(py_arg) 
                << " (iterable expected)" << THROWF_END;
        }
        
        OR_QueryObserverBuilder split_factory(exclusive);
        // include ALL provided values first (OR-joined)
        for (auto it = ForwardIterator(LangToolkit::getIterator(py_arg)), end = ForwardIterator::end(); it != end; ++it) {
            if (isShortTag(*it)) {
                ObjectSharedPtr alt_repr = *it;
                auto tag_iterator = m_base_index_short.makeIterator(getShortTag(*it, &alt_repr));
                // use the alternative representation if such exists
                split_factory.add(std::move(tag_iterator), alt_repr);
            } else if (isLongTag(*it)) {
                auto tag_iterator = m_base_index_long.makeIterator(getLongTag(*it));
                split_factory.add(std::move(tag_iterator), *it);
            } else {
                THROWF(db0::InputException) << "Unable to convert to tag: " 
                    << LangToolkit::getTypeName((*it).get()) 
                    << THROWF_END;
            }
        }
        
        auto split_result = split_factory.release();
        if (exclusive) {
            db0::FT_ANDIteratorFactory<UniqueAddress, true> factory;
            factory.add(std::move(split_result.first));
            factory.add(std::move(query));
            return { factory.release(), std::move(split_result.second) };
        } else {
            db0::FT_ANDIteratorFactory<UniqueAddress, false> factory;
            factory.add(std::move(split_result.first));
            factory.add(std::move(query));
            return { factory.release(), std::move(split_result.second) };
        }
    }
    
    LongTagT TagIndex::getLongTag(ObjectSharedPtr py_arg) const {
        return getLongTag(py_arg.get());
    }

    LongTagT TagIndex::getLongTag(ObjectPtr py_arg) const {
        return getLongTag(LangToolkit::getTypeManager().getTypeId(py_arg), py_arg);
    }

    LongTagT TagIndex::getLongTag(TypeId type_id, ObjectPtr py_arg) const
    {
        // must check for string since it's is an iterable as well
        if (type_id == TypeId::DB0_TAG) {
            return getLongTagFromTag(py_arg);
        } else if (type_id == TypeId::MEMO_OBJECT || type_id == TypeId::MEMO_IMMUTABLE_OBJECT) {
            return getLongTagFromMemo(py_arg); 
        } else if (type_id == TypeId::STRING || !LangToolkit::isIterable(py_arg)) {
            THROWF(db0::InputException) << "Invalid argument (iterable expected)" << THROWF_END;
        }

        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        IterableSequence sequence(LangToolkit::getIterator(py_arg), ForwardIterator::end(), [&](ObjectSharedPtr arg) {
            return getShortTag(arg.get());
        });
        return makeLongTagFromSequence(sequence);
    }
    
    bool TagIndex::isLongTag(ObjectSharedPtr py_arg) const {
        return isLongTag(py_arg.get());
    }

    bool TagIndex::isLongTag(ObjectPtr py_arg) const
    {
        if (PyToolkit::isString(py_arg) || !PyToolkit::isSequence(py_arg) || PyToolkit::length(py_arg) != 2) {
            return false;
        }
        return isScopeIdentifier(PyToolkit::getItem(py_arg, 0)) && isShortTag(PyToolkit::getItem(py_arg, 1));
    }
    
    bool TagIndex::isLongTag(TypeId type_id, ObjectPtr py_arg) const
    {
        // assumed long tag if from a foreign scope
        if (type_id == TypeId::DB0_TAG) {
            auto &py_tag = LangToolkit::getTypeManager().extractTag(py_arg);
            auto addr_pair = py_tag.getLongAddress(m_class_factory);
            return addr_pair.first != m_fixture_uuid;
        }
        return false;
    }
    
    void TagIndex::commit() const
    {
        flush();
        m_base_index_short.commit();
        m_base_index_long.commit();
        // no need to commit invdividual nested intances since they're managed by cache
        if (m_short_tag_index_map) {
            m_short_tag_index_map->commit();
        }
        super_t::commit();
    }
    
    void TagIndex::detach() const
    {
        m_base_index_short.detach();
        m_base_index_long.detach();
        // no need to detach invdividual nested intances since they're managed by cache
        if (m_short_tag_index_map) {
            m_short_tag_index_map->detach();
        }
        super_t::detach();
    }

    db0::FT_BaseIndex<TagIndex::ShortTagT> &TagIndex::getBaseIndexShort() {
        return m_base_index_short;
    }

    const db0::FT_BaseIndex<TagIndex::ShortTagT> &TagIndex::getBaseIndexShort() const {
        return m_base_index_short;
    }

    const db0::FT_BaseIndex<LongTagT> &TagIndex::getBaseIndexLong() const {
        return m_base_index_long;
    }

    TagIndex::ShortTagIndexMap *TagIndex::tryGetShortTagIndexMap() {
        return m_short_tag_index_map.get();
    }

    const TagIndex::ShortTagIndexMap *TagIndex::tryGetShortTagIndexMap() const {
        return m_short_tag_index_map.get();
    }

    TagIndex::ShortTagIndexMap &TagIndex::getShortTagIndexMap()
    {
        if (!m_short_tag_index_map) {
            m_short_tag_index_map = std::make_unique<ShortTagIndexMap>(getMemspace(), m_cache);
            modify().m_reserved[0] = m_short_tag_index_map->getAddress().getOffset();
            m_mutation_log->onDirty();
        }
        return *m_short_tag_index_map;
    }

    const TagIndex::ShortTagIndexMap &TagIndex::getShortTagIndexMap() const {
        if (!m_short_tag_index_map) {
            THROWF(db0::InternalException) << "Short tag index map not initialized" << THROWF_END;
        }
        return *m_short_tag_index_map;
    }

    std::unique_ptr<TagIndex::QueryIterator> TagIndex::makeIterator(ObjectPtr obj_ptr) const {
        assert(obj_ptr);
        return makeIterator(getShortTag(obj_ptr));
    }

    std::unique_ptr<TagIndex::QueryIterator> TagIndex::makeIterator(const TagDef &tag_def) const {
        return makeIterator(getShortTagFromTag(tag_def));
    }

    std::unique_ptr<TagIndex::QueryIterator> TagIndex::makeIterator(const Class &type) const {
        return makeIterator(getShortTagFromClass(type));
    }
    
    std::unique_ptr<TagIndex::QueryIterator> TagIndex::makeIterator(ShortTagT tag) const {
        flush();
        return m_base_index_short.makeIterator(tag);
    }
    
    std::uint64_t getFindFixtureUUID(TagIndex::ObjectPtr obj_ptr)
    {
        using LangToolkit = TagIndex::LangToolkit;
        using TypeId = db0::bindings::TypeId;
        
        // NOTE: we don't report fixture UUID for tags since foreign tags (i.e. from different scope) are allowed        
        if (!obj_ptr || PyToolkit::isTag(obj_ptr)) {
            return 0;
        }
        
        auto fixture_uuid = LangToolkit::getFixtureUUID(obj_ptr);
        if (!fixture_uuid && !LangToolkit::isType(obj_ptr)) {
            auto type_id = LangToolkit::getTypeManager().getTypeId(obj_ptr);
            if (type_id != TypeId::STRING && LangToolkit::isIterable(obj_ptr)) {
                for (auto it = ForwardIterator(LangToolkit::getIterator(obj_ptr)), end = ForwardIterator::end(); it != end; ++it) {
                    auto uuid = getFindFixtureUUID((*it).get());
                    if (fixture_uuid && uuid && uuid != fixture_uuid) {
                        THROWF(db0::InputException) << "Inconsistent prefixes in find query";
                    }
                    if (uuid) {
                        fixture_uuid = uuid;
                    }
                }
            }
        }
        return fixture_uuid;
    }
    
    db0::swine_ptr<Fixture> getFindScope(db0::Snapshot &workspace, TagIndex::ObjectPtr const *args,
        std::size_t nargs, const char *prefix_name)
    {
        if (prefix_name) {
            return workspace.getFixture(prefix_name, std::nullopt);
        }
        
        std::uint64_t fixture_uuid = 0;
        for (std::size_t i = 0; i < nargs; ++i) {
            auto uuid = getFindFixtureUUID(args[i]);
            if (fixture_uuid && uuid && uuid != fixture_uuid) {
                THROWF(db0::InputException) << "Inconsistent prefixes in find query";
            }
            if (uuid) {
                fixture_uuid = uuid;
            }
        }
        
        return workspace.getFixture(fixture_uuid);
    }
    
    db0::swine_ptr<Fixture> getFindParams(db0::Snapshot &workspace, TagIndex::ObjectPtr const *args,
        std::size_t nargs, std::vector<TagIndex::ObjectPtr> &find_args, std::shared_ptr<Class> &type,
        TagIndex::TypeObjectPtr &lang_type, bool &no_result, const char *prefix_name)
    {
        using LangToolkit = TagIndex::LangToolkit;
        
        auto fixture = getFindScope(workspace, args, nargs, prefix_name);
        auto &class_factory = getClassFactory(*fixture);
        no_result = false;
        lang_type = nullptr;
        auto &type_manager = LangToolkit::getTypeManager();
        // locate and process type objects first
        std::size_t args_offset = 0;
        bool is_memo_base = false;
        while (args_offset < nargs) {
            // Python Memo type
            if (LangToolkit::isType(args[args_offset])) {
                lang_type = type_manager.getTypeObject(args[args_offset++]);
                if (LangToolkit::isAnyMemoType(lang_type)) {
                    // MemoBase type does not correspond to any find criteria
                    // but we may use its corresponding lang type
                    if (!type_manager.isMemoBase(lang_type)) {
                        type = class_factory.tryGetExistingType(lang_type);
                        if (!type) {
                            // indicate non-existing type
                            lang_type = nullptr;
                            no_result = true;
                        }
                    }
                }
            } else if (LangToolkit::isClassObject(args[args_offset])) {
                // extract type from the Class object provided as argument
                auto const_type = type_manager.extractConstClass(args[args_offset++]);
                if (*const_type->getFixture() != *fixture) {
                    THROWF(db0::InputException) << "Inconsistent prefixes in find query";
                }
                // can override MemoBase but not other types
                if (type && !is_memo_base) {
                    THROWF(db0::InputException) << "Multiple type objects not allowed in the find query" << THROWF_END;
                }
                // NOTE: we only override lang class if its present
                if (class_factory.hasLangType(*const_type)) {
                    lang_type = class_factory.getLangType(*const_type).get();
                }
                type = std::const_pointer_cast<Class>(const_type);
                // NOTE: no Class object associated with MemoBase, it's safe to assume false
                is_memo_base = false;
            }
            break;
        }
        
        while (args_offset < nargs) {
            find_args.push_back(args[args_offset]);
            ++args_offset;
        }

        return fixture;
    }
    
    LongTagT TagIndex::getLongTagFromTag(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isTag(py_arg));
        auto &py_tag = LangToolkit::getTypeManager().extractTag(py_arg);
        auto addr_pair = py_tag.getLongAddress(m_class_factory);
        return { addr_pair.first, addr_pair.second.getOffset() };
    }
    
    LongTagT TagIndex::getLongTagFromMemo(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isAnyMemoObject(py_arg));
        return {
            LangToolkit::getFixtureUUID(py_arg),
            LangToolkit::getMemoUniqueAddress(py_arg).getAddress().getOffset()
        };
    }
    
    bool TagIndex::isPendingUpdate(UniqueAddress addr) const {
        return m_object_cache.find(addr) != m_object_cache.end();               
    }
    
    void TagIndex::revert(ObjectPtr memo_ptr) const
    {
        auto addr = LangToolkit::getMemoUniqueAddress(memo_ptr);
        if (m_batch_op_short) {
            m_batch_op_short->revert(addr);
        }
        if (m_batch_op_long) {
            m_batch_op_long->revert(addr);
        }
        if (m_batch_op_types) {
            m_batch_op_types->revert(addr);
        }        
    }
    
    bool TagIndex::empty() const {
        if (!m_batch_op_short.empty() || !m_batch_op_long.empty() || !m_batch_op_types.empty()) {
            return false;
        }

        if (m_short_tag_index_map) {
            bool all_composite_indexes_empty = true;
            m_short_tag_index_map->forEachActive([&all_composite_indexes_empty](TagIndex &tag_index) {
                if (!tag_index.empty()) {
                    all_composite_indexes_empty = false;
                    return false;
                }
                return true;
            });
            return all_composite_indexes_empty;
        }

        return true;
    }
    
    bool TagIndex::assureEmpty() const
    {
        if (!m_batch_op_short.empty() || !m_batch_op_long.empty() || !m_batch_op_types.empty()) {
            return false;
        }

        m_batch_op_short.clear();
        m_batch_op_long.clear();
        m_batch_op_types.clear();
        return true;
    }

    bool isObjectPendingUpdate(db0::swine_ptr<Fixture> &fixture, UniqueAddress addr)
    {
        if (fixture->getAccessType() == db0::AccessType::READ_ONLY) {
            // no pending updates in read-only mode
            return false;
        }

        auto tag_index_ptr = fixture->tryGet<TagIndex>();
        return tag_index_ptr && tag_index_ptr->isPendingUpdate(addr);
    }
    
    std::unique_ptr<TagIndex::TP_Iterator> TagIndex::makeTagProduct(
        const std::vector<const ObjectIterable*> &object_iterables, const ObjectIterable* tags_iterable) const
    {
        // collect object related query iterators
        std::vector<std::unique_ptr<QueryIterator> > objects;
        for (auto obj_iter: object_iterables) {
            objects.push_back(obj_iter->beginFTQuery());
        }
        auto tags_query = tags_iterable->beginFTQuery();
        
        // and the inverted index factory function
        auto fixture_ptr = m_fixture;
        auto tag_func = [fixture_ptr, this](UniqueAddress tag_id, int direction) -> std::unique_ptr<QueryIterator> {
            // lock fixture to make sure it has not been closed
            auto fixture = fixture_ptr.lock();
            if (!fixture) {
                THROWF(db0::InternalException) << "Fixture closed while iteration";                
            }
            return this->makeIterator(tag_id.getAddress());
        };

        return std::make_unique<TP_Iterator>(
            std::move(objects), std::move(tags_query), tag_func
        );
    }
    
}   
