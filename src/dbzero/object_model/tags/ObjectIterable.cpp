#include "ObjectIterable.hpp"
#include "ObjectIterator.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/core/collections/full_text/FT_Serialization.hpp>
#include <dbzero/core/collections/range_tree/RT_Serialization.hpp>

namespace db0::object_model

{

    using SortedIterator = db0::SortedIterator<UniqueAddress>;
    std::unique_ptr<SortedIterator> validated(std::unique_ptr<SortedIterator> &&sorted_iterator)
    {
        if (sorted_iterator && sorted_iterator->keyTypeId() != typeid(Address)) {
            throw std::runtime_error("Invalid sorted iterator");
        }
        return std::move(sorted_iterator);
    }
    
    using QueryIterator = db0::FT_Iterator<UniqueAddress>;
    std::unique_ptr<QueryIterator> validated(std::unique_ptr<QueryIterator> &&query_iterator)
    {
        if (query_iterator && query_iterator->keyTypeId() != typeid(Address)) {
            throw std::runtime_error("Invalid query iterator");
        }
        return std::move(query_iterator);
    }
    
    ObjectIterable::ObjectIterable(db0::swine_ptr<Fixture> fixture, std::unique_ptr<QueryIterator> &&ft_query_iterator,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters)
        : m_fixture(fixture)
        , m_class_factory(fixture->get<ClassFactory>())
        , m_query_iterator(validated(std::move(ft_query_iterator)))
        , m_query_observers(std::move(query_observers))
        , m_filters(filters)        
        , m_type(type)
        , m_lang_type(lang_type) 
    {
    }
    
    ObjectIterable::ObjectIterable(db0::swine_ptr<Fixture> fixture, std::unique_ptr<SortedIterator> &&sorted_iterator,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters)
        : m_fixture(fixture)
        , m_class_factory(fixture->get<ClassFactory>())
        , m_sorted_iterator(validated(std::move(sorted_iterator)))        
        , m_query_observers(std::move(query_observers))
        , m_filters(filters)
        , m_type(type)
        , m_lang_type(lang_type)    
    {
    }
    
    ObjectIterable::ObjectIterable(db0::swine_ptr<Fixture> fixture, std::shared_ptr<IteratorFactory> factory,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters)
        : m_fixture(fixture)
        , m_class_factory(fixture->get<ClassFactory>())
        , m_factory(factory)        
        , m_query_observers(std::move(query_observers))
        , m_filters(filters)
        , m_type(type)
        , m_lang_type(lang_type)    
    {
    }
    
    ObjectIterable::ObjectIterable(db0::swine_ptr<Fixture> fixture, const ClassFactory &class_factory,
        std::unique_ptr<QueryIterator> &&ft_query_iterator, std::unique_ptr<SortedIterator> &&sorted_iterator, 
        std::shared_ptr<IteratorFactory> factory, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        std::vector<FilterFunc> &&filters, std::shared_ptr<Class> type, TypeObjectPtr lang_type, const SliceDef &slice_def)
        : m_fixture(fixture)
        , m_class_factory(class_factory)
        , m_query_iterator(std::move(ft_query_iterator))
        , m_sorted_iterator(std::move(sorted_iterator))
        , m_factory(factory)
        , m_query_observers(std::move(query_observers))
        , m_filters(std::move(filters))
        , m_type(type)
        , m_lang_type(lang_type)
        , m_slice_def(slice_def)
    {
    }
    
    bool ObjectIterable::isNull() const {
        return !m_query_iterator && !m_sorted_iterator && !m_factory;
    }
    
    bool ObjectIterable::isSliced() const {
        return !m_slice_def.isDefault();
    }
    
    std::unique_ptr<ObjectIterable::QueryIterator> ObjectIterable::beginFTQuery(
        std::vector<std::unique_ptr<QueryObserver> > &query_observers, int direction) const
    {
        if (isNull()) {
            return nullptr;
        }

        // pull FT iterator from factory if available
        std::unique_ptr<ObjectIterator::QueryIterator> result;
        if (m_factory) {
            result = m_factory->createFTIterator();
        } else {
            if (!m_query_iterator) {
                THROWF(db0::InputException) << "Invalid object iterator" << THROWF_END;
            }
            result = m_query_iterator->beginTyped(direction);
        }
        // rebase/clone observers
        if (result) {
            for (auto &observer: m_query_observers) {
                query_observers.push_back(observer->rebase(*result));
            }
        }
        return result;
    }
        
    std::unique_ptr<SortedIterator> ObjectIterable::beginSorted() const
    {
        if (isNull()) {
            return nullptr;
        }
        if (!m_sorted_iterator) {
            THROWF(db0::InputException) << "Invalid object iterator" << THROWF_END;
        }
        return m_sorted_iterator->beginSorted();
    }
    
    bool ObjectIterable::isSorted() const {
        return m_sorted_iterator != nullptr;
    }
    
    void ObjectIterable::serialize(std::vector<std::byte> &buf) const
    {
        auto fixture = getFixture();
        // FIXTURE uuid
        db0::serial::write(buf, fixture->getUUID());
        db0::serial::write<bool>(buf, this->isNull());
        if (this->isNull()) {
            return;
        }
        if (m_query_iterator) {
            assert(!m_sorted_iterator && !m_factory);
            db0::serial::write<std::uint8_t>(buf, 1);
            m_query_iterator->serialize(buf);
        }
        if (m_sorted_iterator) {
            assert(!m_query_iterator && !m_factory);
            db0::serial::write<std::uint8_t>(buf, 2);
            m_sorted_iterator->serialize(buf);
        }
        if (m_factory) {
            assert(!m_query_iterator && !m_sorted_iterator);
            db0::serial::write<std::uint8_t>(buf, 3);
            m_factory->serialize(buf);
        }
        db0::serial::write<std::uint8_t>(buf, isSliced());
        if (isSliced()) {
            db0::serial::write(buf, m_slice_def.m_start);
            db0::serial::write(buf, m_slice_def.m_stop);
            db0::serial::write(buf, m_slice_def.m_step);
        }
    }
    
    std::unique_ptr<ObjectIterable> ObjectIterable::deserialize(db0::swine_ptr<Fixture> &fixture,
        std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {
        std::uint64_t fixture_uuid = db0::serial::read<std::uint64_t>(iter, end);
        db0::swine_ptr<Fixture> fixture_;
        if (fixture->getUUID() == fixture_uuid) {
            fixture_ = fixture;
        } else {
            fixture_ = fixture->getWorkspace().getFixture(fixture_uuid);
        }
        bool is_null = db0::serial::read<bool>(iter, end);
        if (is_null) {
            // deserialize as null
            return std::make_unique<ObjectIterator>(fixture_, std::unique_ptr<QueryIterator>());            
        }
        
        std::unique_ptr<QueryIterator> query_iterator;
        std::unique_ptr<SortedIterator> sorted_iterator;
        std::shared_ptr<IteratorFactory> factory;

        auto &workspace = fixture_->getWorkspace();
        auto inner_type = db0::serial::read<std::uint8_t>(iter, end);
        if (inner_type == 1) {
            query_iterator = db0::deserializeFT_Iterator<UniqueAddress>(workspace, iter, end);            
        } else if (inner_type == 2) {
            sorted_iterator = db0::deserializeSortedIterator<UniqueAddress>(workspace, iter, end);            
        } else if (inner_type == 3) {
            factory = db0::deserializeIteratorFactory<UniqueAddress>(workspace, iter, end);
        } else {
            THROWF(db0::InputException) << "Invalid object iterable" << THROWF_END;
        }

        bool is_sliced = db0::serial::read<std::uint8_t>(iter, end);        
        std::remove_const<decltype(SliceDef::m_start)>::type start, stop;
        std::remove_const<decltype(SliceDef::m_step)>::type step;
        if (is_sliced) {
            db0::serial::read(iter, end, start);
            db0::serial::read(iter, end, stop);
            db0::serial::read(iter, end, step);            
        }
        
        auto &class_factory = fixture_->get<ClassFactory>();
        return std::unique_ptr<ObjectIterable>(new ObjectIterable(fixture_, class_factory, std::move(query_iterator),
            std::move(sorted_iterator), factory, {}, {}, nullptr, nullptr, is_sliced ? SliceDef{start, stop, step} : SliceDef{}));
    }
    
    double ObjectIterable::compareTo(const ObjectIterable &other) const
    {
        if (isNull()) {
            return other.isNull() ? 0.0 : 1.0;
        }
        if (other.isNull()) {
            return 1.0;
        }        
        std::unique_ptr<BaseIterator> it_own;
        std::unique_ptr<BaseIterator> it_other;
        return getBaseIterator(it_own).compareTo(other.getBaseIterator(it_other));
    }

    std::vector<std::byte> ObjectIterable::getSignature() const
    {
        if (isNull()) {
            return {};
        }        
        std::vector<std::byte> result;
        std::unique_ptr<BaseIterator> it_own;
        getBaseIterator(it_own).getSignature(result);
        return result;
    }
    
    ObjectIterator &ObjectIterable::makeIter(void *at_ptr, const std::vector<FilterFunc> &filters) const
    {
        auto fixture = getFixture();
        std::vector<FilterFunc> new_filters(this->m_filters);
        new_filters.insert(new_filters.end(), filters.begin(), filters.end());
        
        // no query observers for sorted iterator
        if (m_sorted_iterator) {
            auto sorted_iterator = beginSorted();
            return *new (at_ptr) ObjectIterator(fixture, std::move(sorted_iterator), m_type, 
                m_lang_type.get(), {}, new_filters, m_slice_def);
        }
        
        std::unique_ptr<QueryIterator> query_iterator;
        // note that query observers are not collected for the sorted iterator
        std::vector<std::unique_ptr<QueryObserver> > query_observers;
        if (m_query_iterator || m_factory) {
            query_iterator = beginFTQuery(query_observers, -1);
        }
        
        return *new (at_ptr) ObjectIterator(fixture, std::move(query_iterator), m_type, m_lang_type.get(),
            std::move(query_observers), new_filters, m_slice_def);
    }
    
    db0::swine_ptr<Fixture> ObjectIterable::getFixture() const
    {
        auto fixture = m_fixture.lock();
        if (!fixture) {
            THROWF(db0::InputException) << "ObjectIterator is no longer accessible (prefix or snapshot closed)" << THROWF_END;
        }
        return fixture;
    }
    
    const ObjectIterable::BaseIterator &
    ObjectIterable::getBaseIterator(std::unique_ptr<BaseIterator> &iter) const
    {
        if (m_query_iterator) {
            return *m_query_iterator;            
        } else if (m_sorted_iterator) {
            return *m_sorted_iterator;
        } else if (m_factory) {
            iter = m_factory->createBaseIterator();            
            return *iter;
        } else {
            THROWF(db0::InputException) << "Invalid object iterable" << THROWF_END;
        }
    }

    ObjectIterable &ObjectIterable::makeNew(void *at_ptr, ObjectIterable &&iterable) {
        return *new(at_ptr) ObjectIterable(std::move(iterable));
    }

    ObjectIterable &ObjectIterable::makeNew(void *at_ptr, db0::swine_ptr<Fixture> fixture, std::unique_ptr<QueryIterator> &&query_iterator,   
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters)
    {
        return *new(at_ptr) ObjectIterable(fixture, std::move(query_iterator), type, lang_type, 
            std::move(query_observers), filters);
    }

    ObjectIterable &ObjectIterable::makeNew(void *at_ptr, db0::swine_ptr<Fixture> fixture, std::unique_ptr<SortedIterator> &&sorted_iterator,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers, 
        const std::vector<FilterFunc> &filters)
    {
        return *new(at_ptr) ObjectIterable(fixture, std::move(sorted_iterator), type, lang_type,
            std::move(query_observers), filters);        
    }

    ObjectIterable &ObjectIterable::makeNew(void *at_ptr, db0::swine_ptr<Fixture> fixture, std::shared_ptr<IteratorFactory> factory,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers, 
        const std::vector<FilterFunc> &filters)        
    {
        return *new(at_ptr) ObjectIterable(fixture, factory, type, lang_type, std::move(query_observers), filters);
    }
    
    ObjectIterable &ObjectIterable::makeNewAppendFilters(void *at_ptr, const std::vector<FilterFunc> &filters) const
    {
        auto fixture = getFixture();
        std::vector<FilterFunc> _filters(this->m_filters);
        _filters.insert(_filters.end(), filters.begin(), filters.end());
        
        std::vector<std::unique_ptr<QueryObserver> > query_observers;
        std::unique_ptr<QueryIterator> query_iterator;
        std::unique_ptr<SortedIterator> sorted_iterator;
        if (m_query_iterator || m_factory) {
            assert(!m_sorted_iterator);
            query_iterator = beginFTQuery(query_observers, -1);
        } else if (m_sorted_iterator) {
            sorted_iterator = m_sorted_iterator->beginSorted();
        }

        return *new(at_ptr) ObjectIterable(fixture, m_class_factory, std::move(query_iterator), std::move(sorted_iterator),
            m_factory, std::move(query_observers), std::move(_filters), m_type, m_lang_type.get(), m_slice_def);
    }
    
    ObjectIterable &ObjectIterable::makeSlice(void *at_ptr, const SliceDef &slice_def) const
    {
        const SliceDef *def_ptr = &slice_def;
        if (slice_def.isDefault()) {
            def_ptr = &m_slice_def;
        } else if (!m_slice_def.isDefault()) {
            // multiple slicing is not supported
            THROWF(db0::InputException) << "Cannot slice an already sliced iterable (Operation not supported)" << THROWF_END;
        }
        
        auto fixture = getFixture();
        std::vector<FilterFunc> _filters(this->m_filters);
        std::vector<std::unique_ptr<QueryObserver> > query_observers;
        std::unique_ptr<QueryIterator> query_iterator;
        std::unique_ptr<SortedIterator> sorted_iterator;
        if (m_query_iterator || m_factory) {
            assert(!m_sorted_iterator);
            query_iterator = beginFTQuery(query_observers, -1);
        } else if (m_sorted_iterator) {
            sorted_iterator = m_sorted_iterator->beginSorted();
        }
        
        assert(def_ptr);
        return *new(at_ptr) ObjectIterable(fixture, m_class_factory, std::move(query_iterator), std::move(sorted_iterator),
            m_factory, std::move(query_observers), std::move(_filters), m_type, m_lang_type.get(), *def_ptr);  
    }
    
    ObjectIterable &ObjectIterable::makeNew(void *at_ptr, std::unique_ptr<SortedIterator> &&sorted_iterator,
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers, const std::vector<FilterFunc> &filters) const
    {
        auto fixture = getFixture();
        std::vector<FilterFunc> _filters(filters);

        // NOTE: iterator factory not passed, it's use forbidden with sorted iterators
        return *new(at_ptr) ObjectIterable(fixture, m_class_factory, {}, std::move(sorted_iterator),
            nullptr, std::move(query_observers), std::move(_filters), m_type, m_lang_type.get(), m_slice_def);
    }
    
    ObjectIterable &ObjectIterable::makeNew(void *at_ptr, std::unique_ptr<QueryIterator> &&query_iterator,
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers, const std::vector<FilterFunc> &filters) const
    {
        auto fixture = getFixture();
        std::vector<FilterFunc> _filters(filters);

        return *new(at_ptr) ObjectIterable(fixture, m_class_factory, std::move(query_iterator), {},
            m_factory, std::move(query_observers), std::move(_filters), m_type, m_lang_type.get(), m_slice_def);
    }
    
    std::size_t ObjectIterable::getSize() const
    {
        if (isNull()) {
            return 0;
        }

        std::unique_ptr<ObjectIterator::QueryIterator> iter;
        if (m_factory) {
            iter = m_factory->createFTIterator();
        } else if (m_query_iterator) {
            iter = m_query_iterator->beginTyped(-1);
        } else if (m_sorted_iterator) {
            iter = m_sorted_iterator->beginFTQuery();
        }
        
        std::size_t result = 0;
        if (iter) {
            Slice slice(iter.get(), m_slice_def);            
            while (!slice.isEnd()) {
                slice.next();                
                ++result;
            }        
        }
        return result;
    }

    void ObjectIterable::attachContext(ObjectPtr lang_context) const {
        m_lang_context = lang_context;
    }

}
