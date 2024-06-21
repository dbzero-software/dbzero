#include "TagIndex.hpp"
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/iterators.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/class/ClassFields.hpp>
#include <dbzero/core/collections/full_text/FT_ORXIterator.hpp>
#include <dbzero/core/collections/full_text/FT_ANDIterator.hpp>
#include <dbzero/core/collections/full_text/FT_ANDNOTIterator.hpp>
#include <dbzero/object_model/tags/TagSet.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>
#include "ObjectIterator.hpp"
#include "OR_QueryObserver.hpp"

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
    
    class NegFactory: public FT_IteratorFactory<std::uint64_t>
    {
    public:
        using QueryIterator = FT_Iterator<std::uint64_t>;
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

    TagIndex::TagIndex(const ClassFactory &class_factory, RC_LimitedStringPool &string_pool,
        db0::FT_BaseIndex<ShortTagT> &base_index_short, db0::FT_BaseIndex<LongTagT> &base_index_long)
        : m_class_factory(class_factory)
        , m_string_pool(string_pool)
        , m_base_index_short(base_index_short)
        , m_base_index_long(base_index_long)
    {
    }
    
    TagIndex::~TagIndex()
    {
        assert(
            m_batch_operation_short.empty() && 
            m_batch_operation_long.empty() && 
            "TagIndex::flush() or close() must be called before destruction");
    }
    
    FT_BaseIndex<TagIndex::ShortTagT>::BatchOperationBuilder &
    TagIndex::getBatchOperationShort(ObjectPtr memo_ptr)
    {
        return getBatchOperation(memo_ptr, m_base_index_short, m_batch_operation_short);
    }

    db0::FT_BaseIndex<TagIndex::LongTagT>::BatchOperationBuilder &
    TagIndex::getBatchOperationLong(ObjectPtr memo_ptr)
    {
        return getBatchOperation(memo_ptr, m_base_index_long, m_batch_operation_long);
    }

    void TagIndex::addTags(ObjectPtr memo_ptr, ObjectPtr const *args, std::size_t nargs)
    {
        using TypeId = db0::bindings::TypeId;
        if (nargs == 0) {
            return;
        }

        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        auto &batch_op_short = getBatchOperationShort(memo_ptr);
        // since it's less common, defer initialization until first occurence
        db0::FT_BaseIndex<LongTagT>::BatchOperationBuilder *batch_op_long_ptr = nullptr;
        auto &type_manager = LangToolkit::getTypeManager();
        for (std::size_t i = 0; i < nargs; ++i) {
            auto type_id = type_manager.getTypeId(args[i]);
            // must check for string since it's is an iterable as well
            if (type_id != TypeId::STRING && LangToolkit::isIterable(args[i])) {
                auto tag_sequence = IterableSequence(LangToolkit::getIterator(args[i]), ForwardIterator::end(), [&](ObjectSharedPtr arg) {
                    return makeShortTag(arg.get());
                });
                // sequence (pair) may represent a single long tag
                if (isLongTag<ForwardIterator>(LangToolkit::getIterator(args[i]), ForwardIterator::end())) {
                    if (!batch_op_long_ptr) {
                        batch_op_long_ptr = &getBatchOperationLong(memo_ptr);
                    }
                    auto tag = makeLongTagFromSequence(tag_sequence);
                    (*batch_op_long_ptr)->addTags(TagPtrSequence(&tag, &tag + 1));
                } else {
                    batch_op_short->addTags(tag_sequence);
                }
            } else {
                batch_op_short->addTag(makeShortTag(type_id, args[i]));
            }
        }
    }
    
    void TagIndex::addTag(ObjectPtr memo_ptr, ShortTagT tag)
    {
        auto &batch_operation = getBatchOperationShort(memo_ptr);
        batch_operation->addTags(TagPtrSequence(&tag, &tag + 1));
    }

    void TagIndex::addTag(ObjectPtr memo_ptr, LongTagT tag)
    {
        auto &batch_operation = getBatchOperationLong(memo_ptr);
        batch_operation->addTags(TagPtrSequence(&tag, &tag + 1));
    }

    void TagIndex::removeTags(ObjectPtr memo_ptr, ObjectPtr const *args, std::size_t nargs)
    {
        if (nargs == 0) {
            return;
        }

        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        auto &batch_operation = getBatchOperationShort(memo_ptr);
        for (std::size_t i = 0; i < nargs; ++i) {
            auto type_id = LangToolkit::getTypeManager().getTypeId(args[i]);
            // must check for string since it's is an iterable as well
            if (type_id != TypeId::STRING && LangToolkit::isIterable(args[i])) {
                batch_operation->removeTags(IterableSequence(LangToolkit::getIterator(args[i]), ForwardIterator::end(), [&](ObjectSharedPtr arg) {
                    return makeShortTag(arg.get());
                }));
                continue;
            }

            batch_operation->removeTag(makeShortTag(type_id, args[i]));
        }
    }
    
    void TagIndex::clear()
    {
        // Reject any pending updates
        if (m_batch_operation_short) {
            m_batch_operation_short.reset();
        }
        m_base_index_short.clear();

        if (m_batch_operation_long) {
            m_batch_operation_long.reset();
        }
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
    }
    
    void TagIndex::flush() const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        // the purpose of callback is to incRef to objects when a new tag is assigned
        std::function<void(std::uint64_t)> add_tag_callback = [&](std::uint64_t address) {
            auto it = m_object_cache.find(address);
            assert(it != m_object_cache.end());
            type_manager.extractMutableObject(it->second.get()).incRef();
        };

        std::function<void(std::uint64_t)> remove_tag_callback = [&](std::uint64_t address) {
            auto it = m_object_cache.find(address);
            assert(it != m_object_cache.end());
            type_manager.extractMutableObject(it->second.get()).decRef();
        };

        // flush all short tags' updates
        if (!m_batch_operation_short.empty()) {
            m_batch_operation_short->flush(&add_tag_callback, &remove_tag_callback);
        }

        // flush all long tags' updates
        if (!m_batch_operation_long.empty()) {
            m_batch_operation_long->flush(&add_tag_callback, &remove_tag_callback);
        }

        m_object_cache.clear();
    }

    std::unique_ptr<TagIndex::QueryIterator> TagIndex::find(ObjectPtr const *args, std::size_t nargs,
        std::shared_ptr<Class> &type, std::vector<std::unique_ptr<QueryObserver> > &observers) const
    {
        db0::FT_ANDIteratorFactory<std::uint64_t> factory;
        // the negated root-level query components
        std::vector<std::unique_ptr<QueryIterator> > neg_iterators;        
        if (nargs > 0) {
            // flush pending updates before querying
            flush();
            // if the 1st argument is a type then resolve as a TypedObjectIterator
            std::size_t offset = 0;
            bool result = true;
            if (LangToolkit::isType(args[offset])) {
                auto lang_type = LangToolkit::getTypeManager().getTypeObject(args[offset]);
                if (LangToolkit::isMemoType(lang_type)) {                
                    // resolve existing DB0 type from python type and then use type as tag
                    type = m_class_factory.tryGetExistingType(lang_type);
                    if (type) {
                        result &= m_base_index_short.addIterator(factory, type->getAddress());
                    } else {
                        // type not found implies no matching results exist
                        result = false;
                    }
                    ++offset;
                }
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
            return std::make_unique<FT_ANDNOTIterator<std::uint64_t> >(std::move(neg_iterators), -1);
        }
    }
    
    bool TagIndex::addIterator(ObjectPtr arg, db0::FT_IteratorFactory<std::uint64_t> &factory,
        std::vector<std::unique_ptr<QueryIterator> > &neg_iterators, std::vector<std::unique_ptr<QueryObserver> > &query_observers) const
    {
        using TypeId = db0::bindings::TypeId;
        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;

        auto type_id = LangToolkit::getTypeManager().getTypeId(arg);
        if (type_id == TypeId::STRING || type_id == TypeId::MEMO_OBJECT || type_id == TypeId::DB0_ENUM_VALUE) {
            return m_base_index_short.addIterator(factory, makeShortTag(type_id, arg));
        }
        
        // a python iterable
        if (type_id == TypeId::LIST || type_id == TypeId::TUPLE) {
            // check if an iterable can be converted into a long tag and attach to query if yes
            if (isLongTag<ForwardIterator>(LangToolkit::getIterator(arg), ForwardIterator::end())) {
                IterableSequence sequence(LangToolkit::getIterator(arg), ForwardIterator::end(), [&](ObjectSharedPtr arg) {
                    return makeShortTag(arg.get());
                });
                return m_base_index_long.addIterator(factory, makeLongTagFromSequence(sequence));
            }

            bool is_or_clause = (type_id == TypeId::LIST);
            // lists corresponds to OR operator, tuple - to AND
            std::unique_ptr<FT_IteratorFactory<std::uint64_t> > inner_factory;
            if (is_or_clause) {
                inner_factory = std::make_unique<db0::FT_ORXIteratorFactory<std::uint64_t> >();
            } else {
                inner_factory = std::make_unique<db0::FT_ANDIteratorFactory<std::uint64_t> >();
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
        
        if (type_id == TypeId::OBJECT_ITERATOR) {
            auto &obj_iter = LangToolkit::getTypeManager().extractObjectIterator(arg);
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
                for (auto it = tag_set.getArgs(), end = it + tag_set.size(); it != end; ++it) {
                    addIterator(*it, neg_factory, inner_neg_iterators, query_observers);
                }
            } else {
                // just add as regular iterators
                for (auto it = tag_set.getArgs(), end = it + tag_set.size(); it != end; ++it) {
                    addIterator(*it, factory, inner_neg_iterators, query_observers);
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
    
    TagIndex::ShortTagT TagIndex::makeShortTag(ObjectPtr py_arg) const
    {
        auto type_id = LangToolkit::getTypeManager().getTypeId(py_arg);
        return makeShortTag(type_id, py_arg);
    }
    
    TagIndex::ShortTagT TagIndex::makeShortTag(TypeId type_id, ObjectPtr py_arg) const
    {
        if (type_id == TypeId::STRING) {
            return makeShortTagFromString(py_arg);
        } else if (type_id == TypeId::MEMO_OBJECT) {
            return makeShortTagFromMemo(py_arg);
        } else if (type_id == TypeId::DB0_ENUM_VALUE) {
            return makeShortTagFromEnumValue(py_arg);
        } else if (type_id == TypeId::DB0_FIELD_DEF) {
            return makeShortTagFromFieldDef(py_arg);
        }
        THROWF(db0::InputException) << "Unable to interpret object of type: " << LangToolkit::getTypeName(py_arg)
            << " as a tag" << THROWF_END;
    }

    TagIndex::ShortTagT TagIndex::makeShortTagFromString(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isString(py_arg));
        return LangToolkit::addTag(py_arg, m_string_pool);
    }
    
    TagIndex::ShortTagT TagIndex::makeShortTagFromMemo(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isMemoObject(py_arg));
        // mark the object as tag
        auto &object = LangToolkit::getTypeManager().extractObject(py_arg);
        if (!object.isTag()) {
            LangToolkit::getTypeManager().extractMutableObject(py_arg).markAsTag();
        }
        return object.getAddress();
    }
    
    TagIndex::ShortTagT TagIndex::makeShortTagFromEnumValue(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isEnumValue(py_arg));
        return LangToolkit::getTypeManager().extractEnumValue(py_arg).getUID().asULong();
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
        return type_id == TypeId::STRING || type_id == TypeId::MEMO_OBJECT || type_id == TypeId::DB0_ENUM_VALUE || type_id == TypeId::DB0_FIELD_DEF;
    }

    bool TagIndex::isShortTag(ObjectSharedPtr ptr) const {
        return isShortTag(ptr.get());
    }

    TagIndex::ShortTagT TagIndex::makeShortTagFromFieldDef(ObjectPtr py_arg) const
    {
        auto &field_def = LangToolkit::getTypeManager().extractFieldDef(py_arg);
        // class UID (32bit) + field ID (32 bit)
        return (static_cast<std::uint64_t>(field_def.m_class_uid) << 32) | field_def.m_member.m_field_id;
    }
    
    std::pair<std::unique_ptr<TagIndex::QueryIterator>, std::unique_ptr<QueryObserver> >
    TagIndex::splitBy(ObjectPtr py_arg, std::unique_ptr<QueryIterator> &&query) const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        auto type_id = type_manager.getTypeId(py_arg);
        // must check for string since it's is an iterable as well
        if (type_id == TypeId::STRING || !LangToolkit::isIterable(py_arg)) {
            THROWF(db0::InputException) << "Invalid argument (iterable expected)" << THROWF_END;
        }
        
        OR_QueryObserverBuilder split_factory;        
        // include ALL provided values first (OR-joined)
        for (auto it = ForwardIterator(LangToolkit::getIterator(py_arg)), end = ForwardIterator::end(); it != end; ++it) {
            if (isShortTag(*it)) {
                auto tag_iterator = m_base_index_short.makeIterator(makeShortTag(*it));
                split_factory.add(std::move(tag_iterator), *it);
            } else if (isLongTag(*it)) {
                auto tag_iterator = m_base_index_long.makeIterator(makeLongTag(*it));
                split_factory.add(std::move(tag_iterator), *it);
            } else {
                THROWF(db0::InputException) << "Unable to convert to tag: " << LangToolkit::getTypeName((*it).get()) << THROWF_END;
            }
        }

        auto split_result = split_factory.release();
        db0::FT_ANDIteratorFactory<std::uint64_t> factory;
        factory.add(std::move(split_result.first));
        factory.add(std::move(query));
        return { factory.release(), std::move(split_result.second) };
    }
    
    TagIndex::ShortTagT TagIndex::makeShortTag(ObjectSharedPtr py_arg) const {
        return makeShortTag(py_arg.get());
    }

    TagIndex::LongTagT TagIndex::makeLongTag(ObjectSharedPtr py_arg) const {
        return makeLongTag(py_arg.get());
    }

    bool TagIndex::isLongTag(ObjectSharedPtr py_arg) const {
        return isLongTag(py_arg.get());
    }

    bool TagIndex::isLongTag(ObjectPtr py_arg) const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        auto type_id = type_manager.getTypeId(py_arg);
        // must check for string since it's is an iterable as well
        if (type_id == TypeId::STRING || !LangToolkit::isIterable(py_arg)) {
            return false;
        }
        
        return isLongTag<ForwardIterator>(LangToolkit::getIterator(py_arg), ForwardIterator::end());
    }
    
    TagIndex::LongTagT TagIndex::makeLongTag(ObjectPtr py_arg) const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        auto type_id = type_manager.getTypeId(py_arg);
        // must check for string since it's is an iterable as well
        if (type_id == TypeId::STRING || !LangToolkit::isIterable(py_arg)) {
            THROWF(db0::InputException) << "Invalid argument (iterable expected)" << THROWF_END;
        }

        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        IterableSequence sequence(LangToolkit::getIterator(py_arg), ForwardIterator::end(), [&](ObjectSharedPtr arg) {
            return makeShortTag(arg.get());
        });
        return makeLongTagFromSequence(sequence);
    }
    
    std::uint64_t getFindFixtureUUID(TagIndex::ObjectPtr obj_ptr)
    {
        using LangToolkit = TagIndex::LangToolkit;
        using TypeId = db0::bindings::TypeId;

        if (!obj_ptr) {
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

    std::uint64_t getFindFixtureUUID(TagIndex::ObjectPtr const *args, std::size_t nargs)
    {
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
        return fixture_uuid;
    }
    
}