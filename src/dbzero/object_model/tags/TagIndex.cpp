#include "TagIndex.hpp"
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/iterators.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/core/collections/full_text/FT_ANDNOTIterator.hpp>
#include <dbzero/object_model/tags/TagSet.hpp>
#include "ObjectIterator.hpp"

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
    
    class TagPtrSequence
    {
    public:
        TagPtrSequence(const std::uint64_t *begin, const std::uint64_t *end)
            : m_begin(begin)
            , m_end(end)
        {
        }

        const std::uint64_t *begin() const {
            return m_begin;
        }

        const std::uint64_t *end() const {
            return m_end;
        }

    private:
        const std::uint64_t *m_begin;
        const std::uint64_t *m_end;
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

    private:
        std::vector<std::unique_ptr<QueryIterator> > &m_neg_iterators;
    };

    TagIndex::TagIndex(const ClassFactory &class_factory, RC_LimitedStringPool &string_pool, db0::FT_BaseIndex &base_index)
        : m_class_factory(class_factory)
        , m_string_pool(string_pool)
        , m_base_index(base_index)
    {
    }
    
    TagIndex::~TagIndex()
    {
        assert(m_batch_operation.empty() && "TagIndex::flush() or close() must be called before destruction");
    }
    
    TagIndex::BatchOperationBuilder &TagIndex::getBatchOperation(ObjectPtr memo_ptr)
    {
        auto &memo = LangToolkit::getTypeManager().extractObject(memo_ptr);
        auto object_addr = memo.getAddress();
        // cache object locally
        if (m_object_cache.find(object_addr) == m_object_cache.end()) {
            m_object_cache.emplace(object_addr, memo_ptr);
        }

        if (!m_batch_operation) {
            m_batch_operation = m_base_index.beginBatchUpdate();
        }
        m_batch_operation->setActiveValue(object_addr);
        return m_batch_operation;
    }
    
    void TagIndex::addTags(ObjectPtr memo_ptr, ObjectPtr const *args, std::size_t nargs)
    {
        if (nargs == 0) {
            return;
        }

        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        auto &batch_operation = getBatchOperation(memo_ptr);
        for (std::size_t i = 0; i < nargs; ++i) {
            if (LangToolkit::isString(args[i])) {
                batch_operation->addTag(makeTag(args[i]));
            } else if (LangToolkit::isIterable(args[i])) {
                batch_operation->addTags(IterableSequence(LangToolkit::getIterator(args[i]), ForwardIterator::end(), [&](ObjectSharedPtr arg) {
                    return makeTag(arg.get());
                }));
            } else {
                THROWF(db0::InputException) << "Unable to resolve object as tag" << THROWF_END;
            }
        }
    }
    
    void TagIndex::addTag(ObjectPtr memo_ptr, std::uint64_t tag_addr)
    {
        auto &batch_operation = getBatchOperation(memo_ptr);
        batch_operation->addTags(TagPtrSequence(&tag_addr, &tag_addr + 1));
    }
    
    void TagIndex::removeTags(ObjectPtr memo_ptr, ObjectPtr const *args, std::size_t nargs)
    {
        if (nargs == 0) {
            return;
        }

        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;
        auto &batch_operation = getBatchOperation(memo_ptr);
        for (std::size_t i = 0; i < nargs; ++i) {
            if (LangToolkit::isString(args[i])) {
                batch_operation->removeTag(makeTag(args[i]));
            } else if (LangToolkit::isIterable(args[i])) {
                batch_operation->removeTags(IterableSequence(LangToolkit::getIterator(args[i]), ForwardIterator::end(), [&](ObjectSharedPtr arg) {
                    return makeTag(arg.get());
                }));
            } else {
                THROWF(db0::InputException) << "Unable to resolve object as tag" << THROWF_END;
            }
        }
    }
    
    void TagIndex::clear()
    {
        // Reject any pending updates
        if (m_batch_operation) {
            m_batch_operation.reset();
        }
        m_base_index.clear();
    }
    
    void TagIndex::close()
    {
        if (m_batch_operation) {
            m_batch_operation.reset();
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
            type_manager.extractObject(it->second.get()).incRef();
        };

        std::function<void(std::uint64_t)> remove_tag_callback = [&](std::uint64_t address) {
            auto it = m_object_cache.find(address);
            assert(it != m_object_cache.end());
            type_manager.extractObject(it->second.get()).decRef();
        };

        if (!m_batch_operation.empty()) {
            m_batch_operation->flush(&add_tag_callback, &remove_tag_callback);
        }

        m_object_cache.clear();
    }

    std::unique_ptr<TagIndex::QueryIterator> TagIndex::find(ObjectPtr const *args, std::size_t nargs,
        std::shared_ptr<Class> &type) const
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
                // resolve existing DB0 type from python type and then use type as tag
                type = m_class_factory.getExistingType(lang_type);
                result &= m_base_index.addIterator(factory, type->getAddress());
                ++offset;
            }
            
            while (result && (offset < nargs)) {
                result &= addIterator(args[offset], factory, neg_iterators);
                ++offset;
            }
        }
        
        auto query_iterator = factory.release(-1);
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
        std::vector<std::unique_ptr<QueryIterator> > &neg_iterators) const
    {
        using TypeId = db0::bindings::TypeId;
        using IterableSequence = TagMakerSequence<ForwardIterator, ObjectSharedPtr>;

        auto type_id = LangToolkit::getTypeManager().getTypeId(arg);
        if (type_id == TypeId::STRING) {
            return m_base_index.addIterator(factory, makeTag(arg));
        }
        
        // a python iterable
        if (type_id == TypeId::LIST || type_id == TypeId::TUPLE) {
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
                bool result = addIterator((*it).get(), *inner_factory, inner_neg_iterators);
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
            auto ft_query = obj_iter.beginFTQuery(-1);
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
                    addIterator(*it, neg_factory, inner_neg_iterators);
                }
            } else {
                // just add as regular iterators
                for (auto it = tag_set.getArgs(), end = it + tag_set.size(); it != end; ++it) {
                    addIterator(*it, factory, inner_neg_iterators);
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
    
    std::uint64_t TagIndex::makeTag(ObjectPtr py_arg) const
    {
        assert(LangToolkit::isString(py_arg));
        return LangToolkit::addTag(py_arg, m_string_pool);
    }
    
}