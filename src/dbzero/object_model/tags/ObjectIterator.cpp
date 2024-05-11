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

    using BaseIterator = db0::FT_IteratorBase;
    std::unique_ptr<BaseIterator> validated(std::unique_ptr<BaseIterator> &&base_iterator)
    {
        if (base_iterator && base_iterator->keyTypeId() != typeid(std::uint64_t)) {
            throw std::runtime_error("Invalid base/range iterator");
        }
        return std::move(base_iterator);
    }

    ObjectIterator::ObjectIterator(db0::swine_ptr<Fixture> fixture, std::unique_ptr<QueryIterator> &&ft_query_iterator)
        : m_fixture(fixture)
        , m_class_factory(fixture->get<ClassFactory>())
        , m_query_iterator(validated(std::move(ft_query_iterator)))
        , m_iterator_ptr(m_query_iterator.get())        
    {
    }

    ObjectIterator::ObjectIterator(db0::swine_ptr<Fixture> fixture, std::unique_ptr<SortedIterator> &&sorted_iterator)
        : m_fixture(fixture)
        , m_class_factory(fixture->get<ClassFactory>())
        , m_sorted_iterator(validated(std::move(sorted_iterator)))
        , m_iterator_ptr(m_sorted_iterator.get())        
    {
    }

    ObjectIterator::ObjectIterator(db0::swine_ptr<Fixture> fixture, std::unique_ptr<IteratorFactory> &&factory)
        : m_fixture(fixture)
        , m_class_factory(fixture->get<ClassFactory>())
        , m_factory(std::move(factory))        
    {
    }

    ObjectIterator *ObjectIterator::makeNew(void *at_ptr, db0::swine_ptr<Fixture> fixture, std::unique_ptr<QueryIterator> &&it) {
        return new (at_ptr) ObjectIterator(fixture, std::move(it));
    }

    bool ObjectIterator::next(std::uint64_t &addr)
    {
        assureInitialized();
        if (!m_iterator_ptr || m_iterator_ptr->isEnd()) {
            return false;
        } else {
            m_iterator_ptr->next(&addr);
            return true;
        }
    }
    
    ObjectIterator::ObjectPtr ObjectIterator::unload(std::uint64_t address) const {
        return LangToolkit::unloadObject(m_fixture, address, m_class_factory);
    }
    
    bool ObjectIterator::isNull() const {
        return !m_query_iterator && !m_sorted_iterator && !m_factory;
    }

    std::unique_ptr<ObjectIterator::QueryIterator> ObjectIterator::beginFTQuery(int direction) const
    {   
        if (isNull()) {
            return nullptr;
        }

        // pull FT iterator from factory if available
        if (m_factory) {
            return m_factory->createFTIterator();
        }
        if (!m_query_iterator) {
            THROWF(db0::InputException) << "Invalid object iterator" << THROWF_END;
        }
        return m_query_iterator->beginTyped(direction);
    }

    std::unique_ptr<SortedIterator> ObjectIterator::beginSorted() const
    {
        if (isNull()) {
            return nullptr;
        }
        if (!m_sorted_iterator) {
            THROWF(db0::InputException) << "Invalid object iterator" << THROWF_END;
        }
        return m_sorted_iterator->beginSorted();
    }
    
    bool ObjectIterator::isSorted() const {
        return m_sorted_iterator != nullptr;
    }

    void ObjectIterator::assureInitialized()
    {
        if (!m_initialized) {
            if (m_query_iterator) {
                m_iterator_ptr = m_query_iterator.get();
            } else if (m_sorted_iterator) {
                m_iterator_ptr = m_sorted_iterator.get();
            } else if (m_factory) {
                // initialize as base iterator
                assert(!m_base_iterator);
                m_base_iterator = m_factory->createBaseIterator();
                m_iterator_ptr = m_base_iterator.get();
            }
            m_initialized = true;
        }
    }
    
    void ObjectIterator::serialize(std::vector<std::byte> &buf) const
    {        
        // FIXTURE uuid
        db0::serial::write(buf, m_fixture->getUUID());
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
    
    void ObjectIterator::deserialize(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::vector<std::byte>::const_iterator &iter,
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
            new (at_ptr) ObjectIterator(fixture_, std::unique_ptr<QueryIterator>());
            return;
        }
        auto &workspace = fixture_->getWorkspace();
        auto inner_type = db0::serial::read<std::uint8_t>(iter, end);
        if (inner_type == 1) {
            auto query_iterator = db0::deserializeFT_Iterator<std::uint64_t>(workspace, iter, end);
            new (at_ptr) ObjectIterator(fixture_, std::move(query_iterator));
        } else if (inner_type == 2) {
            auto sorted_iterator = db0::deserializeSortedIterator<std::uint64_t>(workspace, iter, end);
            new (at_ptr) ObjectIterator(fixture_, std::move(sorted_iterator));
        } else if (inner_type == 3) {
            auto factory = db0::deserializeIteratorFactory<std::uint64_t>(workspace, iter, end);
            new (at_ptr) ObjectIterator(fixture_, std::move(factory));
        } else {
            THROWF(db0::InputException) << "Invalid object iterator" << THROWF_END;
        }
    }
    
    double ObjectIterator::compareTo(const ObjectIterator &other) const 
    {
        if (isNull()) {
            return other.isNull() ? 0.0 : 1.0;
        }
        if (other.isNull()) {
            return 1.0;
        }
        assert(m_iterator_ptr);
        return m_iterator_ptr->compareTo(*other.m_iterator_ptr);
    }

}
