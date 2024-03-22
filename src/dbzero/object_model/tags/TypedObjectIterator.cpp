#include "TypedObjectIterator.hpp"
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::object_model

{
    
    TypedObjectIterator::TypedObjectIterator(
        db0::swine_ptr<Fixture> fixture, std::unique_ptr<QueryIterator> &&ft_query_iterator, std::shared_ptr<Class> type)
        : ObjectIterator(fixture, std::move(ft_query_iterator))
        , m_type(type)
    {
    }

    TypedObjectIterator::TypedObjectIterator(
        db0::swine_ptr<Fixture> fixture, std::unique_ptr<SortedIterator> &&sorted_iterator, std::shared_ptr<Class> type)
        : ObjectIterator(fixture, std::move(sorted_iterator))
        , m_type(type)
    {
    }
        
    TypedObjectIterator *TypedObjectIterator::makeNew(void *at_ptr, db0::swine_ptr<Fixture> fixture,
        std::unique_ptr<QueryIterator> &&it, std::shared_ptr<Class> type)
    {
        return new (at_ptr) TypedObjectIterator(fixture, std::move(it), type);
    }

    TypedObjectIterator::ObjectPtr TypedObjectIterator::unload(std::uint64_t address) const {
        return LangToolkit::unloadObject(m_fixture, address, m_type);        
    }
    
    TypedObjectIterator::TypeObjectSharedPtr TypedObjectIterator::getLangClass() const
    {
        return m_type->getLangClass();
    }
    
    std::shared_ptr<Class> TypedObjectIterator::getType() const {
        return m_type;
    }
        
}
