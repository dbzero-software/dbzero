#include "ObjectIterator.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/core/collections/full_text/FT_Serialization.hpp>
#include <dbzero/core/collections/range_tree/RT_Serialization.hpp>

namespace db0::object_model

{
    
    ObjectIterator::ObjectIterator(db0::swine_ptr<Fixture> fixture, std::unique_ptr<QueryIterator> &&ft_query_iterator,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters)
        : ObjectIterable(fixture, std::move(ft_query_iterator), type, lang_type, std::move(query_observers), filters)
        , m_iterator_ptr(m_query_iterator.get())
    {
    }

    ObjectIterator::ObjectIterator(db0::swine_ptr<Fixture> fixture, std::unique_ptr<SortedIterator> &&sorted_iterator,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers, 
        const std::vector<FilterFunc> &filters)
        : ObjectIterable(fixture, std::move(sorted_iterator), type, lang_type, std::move(query_observers), filters)
        , m_iterator_ptr(m_sorted_iterator.get())
    {
    }

    ObjectIterator::ObjectIterator(db0::swine_ptr<Fixture> fixture, std::shared_ptr<IteratorFactory> factory,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters)
        : ObjectIterable(fixture, factory, type, lang_type, std::move(query_observers), filters)
    {
    }
    
    ObjectIterator::ObjectIterator(db0::swine_ptr<Fixture> fixture, const ClassFactory &class_factory,
        std::unique_ptr<QueryIterator> &&ft_query_iterator, std::unique_ptr<SortedIterator> &&sorted_iterator, 
        std::shared_ptr<IteratorFactory> factory, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        std::vector<FilterFunc> &&filters, std::shared_ptr<Class> type, TypeObjectPtr lang_type)
        : ObjectIterable(fixture, class_factory, std::move(ft_query_iterator), std::move(sorted_iterator), factory,
            std::move(query_observers), std::move(filters), type, lang_type)
    {
    }
    
    ObjectIterator::ObjectSharedPtr ObjectIterator::next()
    {
        assureInitialized();
        for (;;) {
            if (m_iterator_ptr && !m_iterator_ptr->isEnd()) {
                // collect decorators if any exist            
                if (!m_decoration.empty()) {
                    auto it = m_decoration.m_query_observers.begin();
                    for (auto &decor: m_decoration.m_decorators) {
                        decor = (*it)->getDecoration();
                        ++it;
                    }
                }
                std::uint64_t addr;
                m_iterator_ptr->next(&addr);
                auto obj_ptr = unload(addr);
                // check filters if any                
                for (auto &filter: m_filters) {
                    if (!filter(obj_ptr.get())) {
                        obj_ptr = nullptr;
                        break;
                    }
                }
                if (obj_ptr) {
                    return obj_ptr;
                }
            } else {
                return nullptr;
            }
        }
    }
    
    ObjectIterator::ObjectSharedPtr ObjectIterator::unload(std::uint64_t address) const
    {
        if (m_type) {
            // unload as typed if class is known
            return LangToolkit::unloadObject(getFixture(), address, m_type, m_lang_type.get());
        } else {
            return LangToolkit::unloadObject(getFixture(), address, m_type, m_lang_type.get());
        }        
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

    void ObjectIterator::assureInitialized() const {
        const_cast<ObjectIterator *>(this)->assureInitialized();
    }
    
}
