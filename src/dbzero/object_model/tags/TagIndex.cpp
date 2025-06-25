#include "TagIndex.hpp"
#include "ObjectIterator.hpp"
#include "OR_QueryObserver.hpp"
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

    TagIndex::TagIndex(Memspace &memspace, const ClassFactory &class_factory, EnumFactory &enum_factory, 
        RC_LimitedStringPool &string_pool, VObjectCache &cache, std::shared_ptr<MutationLog> mutation_log)
        : db0::v_object<o_tag_index>(memspace)        
        , m_string_pool(string_pool)
        , m_enum_factory(enum_factory)
        , m_base_index_short(memspace, cache)
        , m_base_index_long(memspace, cache)        
        , m_fixture_uuid(enum_factory.getFixture()->getUUID())
        , m_mutation_log(mutation_log)
    {
        assert(mutation_log);
        modify().m_base_index_short_ptr = m_base_index_short.getAddress();
        modify().m_base_index_long_ptr = m_base_index_long.getAddress();
    }
    
    TagIndex::TagIndex(mptr ptr, const ClassFactory &class_factory, EnumFactory &enum_factory,
        RC_LimitedStringPool &string_pool, VObjectCache &cache, std::shared_ptr<MutationLog> mutation_log)
        : db0::v_object<o_tag_index>(ptr)        
        , m_string_pool(string_pool)
        , m_enum_factory(enum_factory)
        , m_base_index_short(myPtr((*this)->m_base_index_short_ptr), cache)
        , m_base_index_long(myPtr((*this)->m_base_index_long_ptr), cache)
        , m_fixture_uuid(enum_factory.getFixture()->getUUID())
        , m_mutation_log(mutation_log)
    {
        assert(mutation_log);
    }
    
    TagIndex::~TagIndex()
    {
        assert(
            m_batch_operation_short.empty() && 
            m_batch_operation_long.empty() && 
            "TagIndex::flush() or close() must be called before destruction");
    }
    
    FT_BaseIndex<TagIndex::ShortTagT>::BatchOperationBuilder &
    TagIndex::getBatchOperationShort(ObjectPtr memo_ptr, ActiveValueT &result)
    {
        return getBatchOperation(memo_ptr, m_base_index_short, m_batch_operation_short, result);
    }

    db0::FT_BaseIndex<LongTagT>::BatchOperationBuilder &
    TagIndex::getBatchOperationLong(ObjectPtr memo_ptr, ActiveValueT &result)
    {
        return getBatchOperation(memo_ptr, m_base_index_long, m_batch_operation_long, result);
    }
    
    void TagIndex::addTags(ObjectPtr memo_ptr, ObjectPtr const *args, std::size_t nargs)
    {       
        using TypeId = db0::bindings::TypeId;
        if (nargs == 0) {
            return;
        }

        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        ActiveValueT active_key = { UniqueAddress(), nullptr };
        auto &batch_op_short = getBatchOperationShort(memo_ptr, active_key);
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

    void TagIndex::addTag(ObjectPtr memo_ptr, Address tag_addr) {
        addTag(memo_ptr, tag_addr.getOffset());
    }
    
    void TagIndex::addTag(ObjectPtr memo_ptr, ShortTagT tag)
    {
        ActiveValueT active_key = { UniqueAddress(), nullptr };
        auto &batch_operation = getBatchOperationShort(memo_ptr, active_key);
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
    
    void TagIndex::removeTags(ObjectPtr memo_ptr, ObjectPtr const *args, std::size_t nargs)
    {
        if (nargs == 0) {
            return;
        }

        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        ActiveValueT active_key = { UniqueAddress(), nullptr };
        auto &batch_operation = getBatchOperationShort(memo_ptr, active_key);
        for (std::size_t i = 0; i < nargs; ++i) {
            auto type_id = LangToolkit::getTypeManager().getTypeId(args[i]);
            // must check for string since it's an iterable as well
            if (type_id != TypeId::STRING && LangToolkit::isIterable(args[i])) {
                batch_operation->removeTags(active_key,
                    IterableSequence(LangToolkit::getIterator(args[i]), ForwardIterator::end(), [&](ObjectSharedPtr arg) {
                        return getShortTag(arg.get());
                    })
                );
                continue;
            }

            batch_operation->removeTag(active_key, getShortTag(type_id, args[i]));            
            m_mutation_log->onDirty();            
        }
    }
    
    void TagIndex::rollback()
    {
        // Reject any pending updates
        if (m_batch_operation_short) {
            m_batch_operation_short.reset();
        }
        if (m_batch_operation_long) {
            m_batch_operation_long.reset();
        }
        // undo inc-ref
        for (auto tag_addr: m_inc_refed_tags) {
            m_string_pool.unRefByAddr(tag_addr);
        }        
    }

    void TagIndex::clear()
    {
        rollback();
        m_base_index_short.clear();
        m_base_index_long.clear();    
    }
    
    void TagIndex::close()
    {
        if (m_batch_operation_short) {
            m_batch_operation_short.reset();
        }
        if (m_batch_operation_long) {
            m_batch_operation_long.reset();
        }
        m_object_cache.clear();
        m_active_cache.clear();        
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
    
    void TagIndex::flush() const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        // this is to resolve addresses of incomplete objects (must be done before flushing)
        buildActiveValues();
        // the purpose of callback is to incRef to objects when a new tag is assigned
        std::function<void(UniqueAddress)> add_tag_callback = [&](UniqueAddress obj_addr) {
            auto it = m_object_cache.find(obj_addr);
            assert(it != m_object_cache.end());
            type_manager.extractMutableObject(it->second.get()).incRef();
        };

        // add_index_callback adds reference to tags (string pool tokens)
        // unless such reference has already been added when the tag was first created
        std::function<void(ShortTagT)> add_index_callback = [&](ShortTagT tag_addr) {
            tryTagIncRef(tag_addr);
        };
        
        std::function<void(UniqueAddress)> remove_tag_callback = [&](UniqueAddress obj_addr) {
            auto it = m_object_cache.find(obj_addr);
            assert(it != m_object_cache.end());
            type_manager.extractMutableObject(it->second.get()).decRef();
        };
        
        std::function<void(ShortTagT)> erase_index_callback = [&](ShortTagT tag_addr) {
            tryTagDecRef(tag_addr);
        };

        // flush all short tags' updates
        if (!m_batch_operation_short.empty()) {
            m_batch_operation_short->flush(&add_tag_callback, &remove_tag_callback, 
                &add_index_callback, &erase_index_callback);
            assert(m_batch_operation_short.empty());
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
        if (!m_batch_operation_long.empty()) {
            m_batch_operation_long->flush(&add_tag_callback, &remove_tag_callback, 
                &add_long_index_callback, &erase_long_index_callback);
            assert(m_batch_operation_long->empty());
        }

        m_object_cache.clear();
        m_active_cache.clear();        
        m_inc_refed_tags.clear();
    }
    
    void TagIndex::buildActiveValues() const
    {
        for (auto &item: m_active_cache) {
            auto &memo = LangToolkit::getTypeManager().extractObject(item.first.get());
            if (!memo.hasInstance()) {
                // object might be defunct, in which case needs to be ignored
                if (memo.isDefunct()) {                   
                    continue;
                }
                THROWF(db0::InternalException) << "Tags cannot be flushed before initialization of @memo objects" << THROWF_END;
            }
            
            auto object_addr = memo.getUniqueAddress();
            assert(object_addr.isValid());
            // initialize active value with the actual object address
            item.second = object_addr;
            // add object to cache
            if (m_object_cache.find(object_addr) == m_object_cache.end()) {
                m_object_cache.emplace(object_addr, item.first);
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
        if (type_id == TypeId::MEMO_OBJECT) {
            auto addr = LangToolkit::getTypeManager().extractObject(arg).getUniqueAddress();
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
        return LangToolkit::getTypeManager().extractTag(py_arg).m_address.getOffset();
    }
    
    TagIndex::ShortTagT TagIndex::getShortTagFromEnumValue(const EnumValue &enum_value, ObjectSharedPtr *alt_repr) const
    {
        assert(enum_value);
        if (enum_value.m_fixture->getUUID() != m_fixture_uuid) {
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
        return LangToolkit::getTypeManager().extractConstClass(py_arg)->getAddress().getOffset();
    }
    
    TagIndex::ShortTagT TagIndex::getShortTagFromFieldDef(ObjectPtr py_arg) const
    {
        auto &field_def = LangToolkit::getTypeManager().extractFieldDef(py_arg);
        // class UID (32bit) + field ID (32 bit)
        return (static_cast<std::uint64_t>(field_def.m_class_uid) << 32) | field_def.m_member.m_field_id.getIndex();
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
        } else if (type_id == TypeId::MEMO_OBJECT) {
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
        assert(LangToolkit::isMemoObject(py_arg));
        auto &py_obj = LangToolkit::getTypeManager().extractObject(py_arg);
        if (py_obj.getFixtureUUID() != m_fixture_uuid) {
            // must be added as long tag
            return std::nullopt;
        }
        // NOTE: we use only the offset part as tag - to distinguish from enum and class tags (high bits)
        return py_obj.getAddress().getOffset();
    }
    
    std::optional<TagIndex::ShortTagT> TagIndex::tryAddShortTagFromTag(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isTag(py_arg));
        auto &py_tag = LangToolkit::getTypeManager().extractTag(py_arg);
        if (py_tag.m_fixture_uuid != m_fixture_uuid) {
            // must be added as long tag
            return std::nullopt;
        }
        // NOTE: we use only the offset part as tag - to distinguish from enum and class tags (high bits)
        return py_tag.m_address.getOffset();
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
        return type_id == TypeId::STRING || type_id == TypeId::MEMO_OBJECT || type_id == TypeId::DB0_ENUM_VALUE || 
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
        } else if (type_id == TypeId::MEMO_OBJECT) {
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
            return py_tag.m_fixture_uuid != m_fixture_uuid;
        }
        return false;
    }

    void TagIndex::commit() const
    {
        flush();
        m_base_index_short.commit();
        m_base_index_long.commit();
        super_t::commit();
    }
    
    void TagIndex::detach() const
    {
        m_base_index_short.detach();
        m_base_index_long.detach();
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
            return workspace.getFixture(prefix_name);
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
        auto &class_factory = fixture->get<ClassFactory>();
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
                if (LangToolkit::isMemoType(lang_type)) {
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
        return { py_tag.m_fixture_uuid, py_tag.m_address.getOffset() };
    }
    
    LongTagT TagIndex::getLongTagFromMemo(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isMemoObject(py_arg));
        auto &py_obj = LangToolkit::getTypeManager().extractObject(py_arg);
        return { py_obj.getFixtureUUID(), py_obj.getAddress().getOffset() };
    }
 
}