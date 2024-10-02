#include "TypedObjectIterator.hpp"
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::object_model

{
    
    TypedObjectIterator::TypedObjectIterator(db0::swine_ptr<Fixture> fixture,
        std::unique_ptr<QueryIterator> &&ft_query_iterator, std::shared_ptr<Class> type, TypeObjectPtr lang_type,
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers, const std::vector<FilterFunc> &filters)
        : ObjectIterator(fixture, std::move(ft_query_iterator), std::move(query_observers), filters)
        , m_type(type)
        , m_lang_type(lang_type)
    {
    }
    
    TypedObjectIterator::TypedObjectIterator(db0::swine_ptr<Fixture> fixture,
        std::unique_ptr<SortedIterator> &&sorted_iterator, std::shared_ptr<Class> type, TypeObjectPtr lang_type,
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers, const std::vector<FilterFunc> &filters)
        : ObjectIterator(fixture, std::move(sorted_iterator), std::move(query_observers), filters)
        , m_type(type)
        , m_lang_type(lang_type)
    {
    }
    
    TypedObjectIterator::TypedObjectIterator(db0::swine_ptr<Fixture> fixture, const ClassFactory &class_factory,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::unique_ptr<QueryIterator> &&query_iterator, 
        std::unique_ptr<SortedIterator> &&sorted_iterator, std::shared_ptr<IteratorFactory> factory, 
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        std::vector<FilterFunc> &&filters)
        : ObjectIterator(fixture, class_factory, std::move(query_iterator), std::move(sorted_iterator),
            factory, std::move(query_observers), std::move(filters))
        , m_type(type)
        , m_lang_type(lang_type)
    {
    }
    
    TypedObjectIterator::ObjectSharedPtr TypedObjectIterator::unload(std::uint64_t address) const {
        return LangToolkit::unloadObject(m_fixture, address, m_type, m_lang_type.get());
    }

    std::unique_ptr<ObjectIterator> TypedObjectIterator::iter(const std::vector<FilterFunc> &filters) const {
        return iterTyped(filters);
    }

    std::unique_ptr<TypedObjectIterator> TypedObjectIterator::iterTyped(const std::vector<FilterFunc> &filters) const
    {
        std::vector<FilterFunc> new_filters(this->m_filters);
        new_filters.insert(new_filters.end(), filters.begin(), filters.end());

        std::unique_ptr<QueryIterator> query_iterator = m_query_iterator ? 
            std::unique_ptr<QueryIterator>(static_cast<QueryIterator*>(m_query_iterator->begin().release())) : nullptr;
        std::unique_ptr<SortedIterator> sorted_iterator = m_sorted_iterator ? m_sorted_iterator->beginSorted() : nullptr;
        return std::unique_ptr<TypedObjectIterator>(new TypedObjectIterator(m_fixture, m_class_factory, m_type, m_lang_type.get(),
            std::move(query_iterator), std::move(sorted_iterator), m_factory, {}, std::move(new_filters)));
    }

    std::unique_ptr<TypedObjectIterator> TypedObjectIterator::makeTypedIter(std::unique_ptr<QueryIterator> &&ft_query_iterator,
        std::vector<std::unique_ptr<QueryObserver> > &&observers, const std::vector<FilterFunc> &filters) const
    {
        std::vector<FilterFunc> _filters(filters);
        return std::unique_ptr<TypedObjectIterator>(new TypedObjectIterator(m_fixture, m_class_factory, m_type, m_lang_type.get(),
            std::move(ft_query_iterator), {}, m_factory, std::move(observers), std::move(_filters)));
    }
    
    std::unique_ptr<TypedObjectIterator> TypedObjectIterator::makeTypedIter(std::unique_ptr<SortedIterator> &&sorted_iterator,
        std::vector<std::unique_ptr<QueryObserver> > &&observers, const std::vector<FilterFunc> &filters) const
    {
        std::vector<FilterFunc> _filters(filters);
        return std::unique_ptr<TypedObjectIterator>(new TypedObjectIterator(m_fixture, m_class_factory, m_type, m_lang_type.get(),
            {}, std::move(sorted_iterator), m_factory, std::move(observers), std::move(_filters)));
    }
    
}
