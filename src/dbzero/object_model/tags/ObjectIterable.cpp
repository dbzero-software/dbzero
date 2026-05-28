// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectIterable.hpp"
#include "ObjectIterator.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/core/collections/full_text/FT_Serialization.hpp>
#include <dbzero/core/collections/full_text/FT_ANDIterator.hpp>
#include <dbzero/core/collections/range_tree/RT_Serialization.hpp>
#include <iterator>

namespace db0::object_model

{

    using SortedIterator = db0::SortedIterator<UniqueAddress>;
    
    std::unique_ptr<SortedIterator> validated(std::unique_ptr<SortedIterator> &&sorted_iterator)
    {
        if (sorted_iterator && sorted_iterator->keyTypeId() != typeid(UniqueAddress)) {
            throw std::runtime_error("Invalid sorted iterator");
        }
        return std::move(sorted_iterator);
    }
    
    using QueryIterator = db0::FT_Iterator<UniqueAddress>;
    std::unique_ptr<QueryIterator> validated(std::unique_ptr<QueryIterator> &&query_iterator)
    {
        if (query_iterator && query_iterator->keyTypeId() != typeid(UniqueAddress)) {
            throw std::runtime_error("Invalid query iterator");
        }
        return std::move(query_iterator);
    }
        
    ObjectIterable::ObjectIterable(db0::swine_ptr<Fixture> fixture, std::unique_ptr<QueryIterator> &&ft_query_iterator,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters, bool predicate_only)
        : m_fixture(fixture)
        , m_class_factory(getClassFactory(*fixture))
        , m_query_iterator(validated(std::move(ft_query_iterator)))
        , m_query_observers(std::move(query_observers))
        , m_filters(filters)        
        , m_type(type)
        , m_lang_type(lang_type)
        , m_access_mode(getAccessMode(type))
        , m_predicate_only(predicate_only)
    {
    }
    
    ObjectIterable::ObjectIterable(db0::swine_ptr<Fixture> fixture, std::unique_ptr<SortedIterator> &&sorted_iterator,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters, bool predicate_only)
        : m_fixture(fixture)
        , m_class_factory(getClassFactory(*fixture))
        , m_sorted_iterator(validated(std::move(sorted_iterator)))        
        , m_query_observers(std::move(query_observers))
        , m_filters(filters)
        , m_type(type)
        , m_lang_type(lang_type)    
        , m_access_mode(getAccessMode(type))
        , m_predicate_only(predicate_only)
    {
    }
    
    ObjectIterable::ObjectIterable(db0::swine_ptr<Fixture> fixture, std::shared_ptr<IteratorFactory> factory,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters, bool predicate_only)
        : m_fixture(fixture)
        , m_class_factory(getClassFactory(*fixture))
        , m_factory(factory)        
        , m_query_observers(std::move(query_observers))
        , m_filters(filters)
        , m_type(type)
        , m_lang_type(lang_type)    
        , m_access_mode(getAccessMode(type))
        , m_predicate_only(predicate_only)
    {
    }

    ObjectIterable::ObjectIterable(db0::swine_ptr<Fixture> fixture, const ClassFactory &class_factory,
        std::unique_ptr<QueryIterator> &&ft_query_iterator, std::unique_ptr<SortedIterator> &&sorted_iterator,
        std::shared_ptr<IteratorFactory> factory, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        std::vector<FilterFunc> &&filters, std::shared_ptr<Class> type, TypeObjectPtr lang_type, 
        const SliceDef &slice_def, AccessFlags access_mode, bool predicate_only)
        : m_fixture(fixture)
        , m_class_factory(class_factory)
        , m_query_iterator(std::move(ft_query_iterator))
        , m_sorted_iterator(std::move(sorted_iterator))
        , m_factory(factory)
        , m_query_observers(std::move(query_observers))
        , m_filters(std::move(filters))
        , m_data_filter_predicates()
        , m_type(type)
        , m_lang_type(lang_type)
        , m_slice_def(slice_def)
        , m_access_mode(access_mode)
        , m_predicate_only(predicate_only)
    {
    }

    ObjectIterable::ObjectIterable(const ObjectIterable &other, const std::vector<FilterFunc> &filters)
        : m_fixture(other.m_fixture)
        , m_class_factory(other.m_class_factory)
        , m_factory(other.m_factory)
        , m_filters(other.m_filters)
        , m_data_filter_predicates(other.m_data_filter_predicates)
        , m_type(other.m_type)
        , m_lang_type(other.m_lang_type)
        , m_slice_def(other.m_slice_def)
        , m_access_mode(other.m_access_mode)
        , m_predicate_only(other.m_predicate_only)
    {
        m_filters.insert(m_filters.end(), filters.begin(), filters.end());
        
        std::unique_ptr<QueryIterator> query_iterator;
        std::unique_ptr<SortedIterator> sorted_iterator;
        if (other.m_query_iterator || other.m_factory || !other.m_data_filter_predicates.empty()) {
            m_query_iterator = other.beginFTQuery(m_query_observers, -1);
        } else if (other.m_sorted_iterator) {
            m_sorted_iterator = other.m_sorted_iterator->beginSorted();
        }
    }
    
    ObjectIterable::ObjectIterable(const ObjectIterable &other, const SliceDef &slice_def)
        : m_fixture(other.m_fixture)
        , m_class_factory(other.m_class_factory)
        , m_factory(other.m_factory)
        , m_filters(other.m_filters)
        , m_data_filter_predicates(other.m_data_filter_predicates)
        , m_type(other.m_type)
        , m_lang_type(other.m_lang_type)
        , m_slice_def(other.m_slice_def.combineWith(slice_def))
        , m_access_mode(other.m_access_mode)
        , m_predicate_only(other.m_predicate_only)
    {
        std::unique_ptr<QueryIterator> query_iterator;
        std::unique_ptr<SortedIterator> sorted_iterator;
        if (other.m_query_iterator || other.m_factory || !other.m_data_filter_predicates.empty()) {
            m_query_iterator = other.beginFTQuery(m_query_observers, -1);
        } else if (other.m_sorted_iterator) {
            m_sorted_iterator = other.m_sorted_iterator->beginSorted();
        }
    }
    
    ObjectIterable::ObjectIterable(const ObjectIterable &other, std::unique_ptr<SortedIterator> &&sorted_iterator,
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers, const std::vector<FilterFunc> &filters)
        : m_fixture(other.m_fixture)
        , m_class_factory(other.m_class_factory)
        , m_sorted_iterator(std::move(sorted_iterator))
        // NOTE: iterator factory not passed, it's use forbidden with sorted iterators
        , m_factory(nullptr)
        , m_query_observers(std::move(query_observers))
        , m_filters(other.m_filters)
        , m_data_filter_predicates(other.m_data_filter_predicates)
        , m_type(other.m_type)
        , m_lang_type(other.m_lang_type)
        , m_slice_def(other.m_slice_def)
        , m_access_mode(other.m_access_mode)
        , m_predicate_only(other.m_predicate_only)
    {
        m_filters.insert(m_filters.end(), filters.begin(), filters.end());
    }
    
    ObjectIterable::ObjectIterable(const ObjectIterable &other, std::unique_ptr<QueryIterator> &&query_iterator,
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers, const std::vector<FilterFunc> &filters)
        : m_fixture(other.m_fixture)
        , m_class_factory(other.m_class_factory)
        , m_query_iterator(std::move(query_iterator))        
        , m_factory(other.m_factory)
        , m_query_observers(std::move(query_observers))
        , m_filters(other.m_filters)
        , m_data_filter_predicates(other.m_data_filter_predicates)
        , m_type(other.m_type)
        , m_lang_type(other.m_lang_type)
        , m_slice_def(other.m_slice_def)
        , m_access_mode(other.m_access_mode)
        , m_predicate_only(other.m_predicate_only)
    {
        m_filters.insert(m_filters.end(), filters.begin(), filters.end());
    }
    
    ObjectIterable::~ObjectIterable()
    {
    }
    
    bool ObjectIterable::isNull() const {
        return !m_query_iterator && !m_sorted_iterator && !m_factory && m_data_filter_predicates.empty();
    }
    
    bool ObjectIterable::isSliced() const {
        return !m_slice_def.isDefault();
    }
    
    std::unique_ptr<ObjectIterable::QueryIterator> ObjectIterable::beginFTQuery(
        int direction) const
    {
        if (isNull()) {
            return nullptr;
        }

        std::unique_ptr<ObjectIterator::QueryIterator> result;
        if (m_factory) {
            result = m_factory->createFTIterator();
        } else if (m_sorted_iterator) {
            result = m_sorted_iterator->beginFTQuery();
        } else if (m_query_iterator) {
            result = m_query_iterator->beginTyped(direction);
        } else if (m_data_filter_predicates.empty()) {
            return nullptr;
        }

        if (m_data_filter_predicates.empty()) {
            return result;
        }

        db0::FT_ANDIteratorFactory<UniqueAddress> factory;
        if (result) {
            factory.add(std::move(result));
        }
        for (const auto &predicate: m_data_filter_predicates) {
            if (!predicate) {
                continue;
            }
            std::vector<std::unique_ptr<QueryObserver> > query_observers;
            auto predicate_query = predicate->beginFTQuery(query_observers, direction);
            if (!predicate_query || predicate_query->isEnd()) {
                factory.clear();
                return nullptr;
            }
            factory.add(std::move(predicate_query));
        }
        return factory.release(direction);
    }

    void ObjectIterable::addDataFilterPredicates(std::vector<std::shared_ptr<ObjectIterable> > &&predicates)
    {
        if (predicates.empty()) {
            return;
        }
        m_data_filter_predicates.insert(
            m_data_filter_predicates.end(),
            std::make_move_iterator(predicates.begin()),
            std::make_move_iterator(predicates.end())
        );
    }

    bool ObjectIterable::hasDataFilterPredicates() const
    {
        return !m_data_filter_predicates.empty();
    }

    std::unique_ptr<ObjectIterable::QueryIterator> ObjectIterable::beginFTQuery(
        std::vector<std::unique_ptr<QueryObserver> > &query_observers, int direction) const
    {
        auto result = beginFTQuery(direction);
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
        bool base_is_null = !m_query_iterator && !m_sorted_iterator && !m_factory;
        // FIXTURE uuid
        db0::serial::write(buf, fixture->getUUID());
        db0::serial::write<bool>(buf, base_is_null);
        db0::serial::write<bool>(buf, m_predicate_only);
        if (base_is_null) {
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
        bool predicate_only = db0::serial::read<bool>(iter, end);
        if (is_null) {
            // deserialize as null
            return std::make_unique<ObjectIterable>(fixture_, std::unique_ptr<QueryIterator>(), nullptr, nullptr, 
                std::vector<std::unique_ptr<QueryObserver> >{}, std::vector<FilterFunc>{}, predicate_only);            
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
        std::remove_const<decltype(SliceDef::m_start)>::type start = 0, stop = 0;
        std::remove_const<decltype(SliceDef::m_step)>::type step = 1;
        if (is_sliced) {
            db0::serial::read(iter, end, start);
            db0::serial::read(iter, end, stop);
            db0::serial::read(iter, end, step);            
        }
        
        auto &class_factory = fixture_->get<ClassFactory>();
        return std::unique_ptr<ObjectIterable>(new ObjectIterable(fixture_, class_factory, std::move(query_iterator),
            std::move(sorted_iterator), factory, {}, {}, nullptr, nullptr, is_sliced ? SliceDef{start, stop, step} : SliceDef{}, {},
            predicate_only));
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
        
    std::size_t ObjectIterable::getSize() const
    {
        if (isNull()) {
            return 0;
        }

        if (!m_filters.empty()) {
            auto obj_iter = iter();
            std::size_t result = 0;
            while (!!obj_iter->next()) {
                ++result;
            }
            return result;
        }

        auto iter = beginFTQuery(-1);
        
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
    
    std::shared_ptr<Class> ObjectIterable::getType() const {
        return m_type;
    }

    ObjectIterable::TypeObjectPtr ObjectIterable::getLangType() const {
        return m_lang_type.get();
    }
    
    ObjectIterable::BaseIterator *ObjectIterable::getIteratorPtr() const
    {
        if (m_sorted_iterator) {
            return m_sorted_iterator.get();
        } else {
            return m_query_iterator.get();
        }
    }
    
    std::shared_ptr<ObjectIterator> ObjectIterable::iter() const {
        return std::make_shared<ObjectIterator>(*this);
    }
    
    bool ObjectIterable::empty() const
    {
        if (isNull()) {
            return true;
        }
        if (!m_filters.empty()) {
            auto obj_iter = iter();
            return !obj_iter->next();
        }

        auto iter = beginFTQuery(-1);
        
        if (iter) {
            Slice slice(iter.get(), m_slice_def);            
            while (!slice.isEnd()) {
                slice.next();                
                return false;
            }        
        }
        return true;
    }

    bool ObjectIterable::isPredicateOnly() const
    {
        return m_predicate_only;
    }

    AccessFlags ObjectIterable::getAccessMode(std::shared_ptr<Class> type) const
    {
        if (type) {
            return type->isNoCache() ? AccessFlags { AccessOptions::no_cache } : AccessFlags {};            
        }
        return {};
    }
    
    void getItemsByIndices(const ObjectIterable &iterable, const std::vector<std::uint64_t> &indices,
        std::function<void(unsigned int ord, ObjectIterable::ObjectSharedPtr)> callback)
    {
        std::vector<std::pair<std::uint64_t, unsigned int> > sorted_indices;
        sorted_indices.reserve(indices.size());
        for (unsigned int ord = 0; ord < indices.size(); ++ord) {
            sorted_indices.emplace_back(indices[ord], ord);
        }
        std::sort(sorted_indices.begin(), sorted_indices.end(),
            [](const auto &lhs, const auto &rhs) {
                return lhs.first < rhs.first;
            });
        
        // access items in sorted order, populating the callback
        auto iter = iterable.iter();
        std::uint64_t current_index = 0;
        ObjectIterable::ObjectSharedPtr last_item;
        for (const auto &[index, ord] : sorted_indices) {
            if (current_index > index) {
                // duplicate item
                assert(last_item.get());
                callback(ord, last_item);
                continue;
            }
            
            if (current_index < index) {
                auto to_skip = index - current_index;
                if (iter->skip(to_skip) < to_skip) {
                    THROWF(db0::IndexException) << "Index " << index << " out of range";
                }
                current_index = index;
            }

            last_item = iter->next();
            if (!last_item) {
                THROWF(db0::IndexException) << "Index " << index << " out of range";
            }
            ++current_index;
            callback(ord, last_item);
        }
    }
    
}
