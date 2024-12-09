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

    using SortedIterator = db0::SortedIterator<std::uint64_t>;
    std::unique_ptr<SortedIterator> validated(std::unique_ptr<SortedIterator> &&sorted_iterator)
    {
        if (sorted_iterator && sorted_iterator->keyTypeId() != typeid(std::uint64_t)) {
            throw std::runtime_error("Invalid sorted iterator");
        }
        return std::move(sorted_iterator);
    }

    using QueryIterator = db0::FT_Iterator<std::uint64_t>;
    std::unique_ptr<QueryIterator> validated(std::unique_ptr<QueryIterator> &&query_iterator)
    {
        if (query_iterator && query_iterator->keyTypeId() != typeid(std::uint64_t)) {
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
        , m_decoration(std::move(query_observers))
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
        , m_decoration(std::move(query_observers))
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
        , m_decoration(std::move(query_observers))
        , m_filters(filters)
        , m_type(type)
        , m_lang_type(lang_type)    
    {
    }
    
    ObjectIterable::ObjectIterable(db0::swine_ptr<Fixture> fixture, const ClassFactory &class_factory,
        std::unique_ptr<QueryIterator> &&ft_query_iterator, std::unique_ptr<SortedIterator> &&sorted_iterator, 
        std::shared_ptr<IteratorFactory> factory, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        std::vector<FilterFunc> &&filters, std::shared_ptr<Class> type, TypeObjectPtr lang_type)
        : m_fixture(fixture)
        , m_class_factory(class_factory)
        , m_query_iterator(std::move(ft_query_iterator))
        , m_sorted_iterator(std::move(sorted_iterator))
        , m_factory(factory)
        , m_decoration(std::move(query_observers))
        , m_filters(std::move(filters))
        , m_type(type)
        , m_lang_type(lang_type)
    {
    }
    
    ObjectIterable::Decoration::Decoration(std::vector<std::unique_ptr<QueryObserver> > &&query_observers)
        : m_query_observers(std::move(query_observers))
        , m_decorators(m_query_observers.size())
    {
    }

    bool ObjectIterable::isNull() const {
        return !m_query_iterator && !m_sorted_iterator && !m_factory;
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
            for (auto &observer: m_decoration.m_query_observers) {
                query_observers.push_back(observer->rebase(*result));
            }
        }
        return result;
    }
    
    std::unique_ptr<QueryIterator> ObjectIterable::releaseQuery(std::vector<std::unique_ptr<QueryObserver> > &query_observers)
    {
        if (!m_query_iterator && m_factory) {
            m_query_iterator = m_factory->createFTIterator();
        }
        for (auto &observer: m_decoration.m_query_observers) {
            query_observers.push_back(std::move(observer));
        }
        return std::move(m_query_iterator);
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
        auto &workspace = fixture_->getWorkspace();
        auto inner_type = db0::serial::read<std::uint8_t>(iter, end);
        if (inner_type == 1) {
            auto query_iterator = db0::deserializeFT_Iterator<std::uint64_t>(workspace, iter, end);
            return std::make_unique<ObjectIterable>(fixture_, std::move(query_iterator));
        } else if (inner_type == 2) {
            auto sorted_iterator = db0::deserializeSortedIterator<std::uint64_t>(workspace, iter, end);
            return std::make_unique<ObjectIterable>(fixture_, std::move(sorted_iterator));
        } else if (inner_type == 3) {
            auto factory = db0::deserializeIteratorFactory<std::uint64_t>(workspace, iter, end);
            return std::make_unique<ObjectIterable>(fixture_, std::move(factory));
        } else {
            THROWF(db0::InputException) << "Invalid object iterable" << THROWF_END;
        }
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
    
    std::unique_ptr<ObjectIterator> ObjectIterable::iter(const std::vector<FilterFunc> &filters) const
    {
        auto fixture = getFixture();
        std::vector<FilterFunc> new_filters(this->m_filters);
        new_filters.insert(new_filters.end(), filters.begin(), filters.end());

        std::unique_ptr<QueryIterator> query_iterator = m_query_iterator ? 
            std::unique_ptr<QueryIterator>(static_cast<QueryIterator*>(m_query_iterator->begin().release())) : nullptr;
        std::unique_ptr<SortedIterator> sorted_iterator = m_sorted_iterator ? m_sorted_iterator->beginSorted() : nullptr;
        return std::unique_ptr<ObjectIterator>(new ObjectIterator(fixture, m_class_factory, std::move(query_iterator),
            std::move(sorted_iterator), m_factory, {}, std::move(new_filters), m_type, m_lang_type.get()));
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

}
