// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <random>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/core/collections/full_text/SortedIterator.hpp>
#include <dbzero/core/collections/full_text/IteratorFactory.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include "QueryObserver.hpp"
#include "ObjectIterable.hpp"
#include "Slice.hpp"

namespace db0::object_model

{
    
    class ClassFactory;
    class QueryObserver;
    using Object = db0::object_model::Object;
    using Serializable = db0::Serializable;

    /**
     * Full-text query result iterator over unspecified type objects
     * all objects must be from the same prefix
    */
    class ObjectIterator: public ObjectIterable
    {
    public:
        // Construct from a full-text query iterator
        // NOTE: lang_type is required here because results may need to be accessed as MemoBase
        // even if their exact type is unknown (this is for the model-less access to data)
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&, std::shared_ptr<Class> = nullptr,
            TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {}, 
            const std::vector<FilterFunc> & = {}, const SliceDef & = {});
        
        // Construct from a sorted iterator
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<SortedIterator> &&, std::shared_ptr<Class> = nullptr,
            TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {}, 
            const std::vector<FilterFunc> & = {}, const SliceDef & = {});
        
        // Construct iterator with additional filters
        ObjectIterator(const ObjectIterable &, const std::vector<FilterFunc> & = {});
        
        virtual ~ObjectIterator() = default;

        /**
         * Retrieve next object from the iterator         
         * @return nullptr if end of iteration reached
        */
        virtual ObjectSharedPtr next();
        
        inline unsigned int numDecorators() const {
            return m_decoration.size();
        }
        
        const std::vector<ObjectPtr> &getDecorators() const {
            return m_decoration.m_decorators;
        }

        // Try to skip specified number of items
        // @return number of actually skipped items
        std::size_t skip(std::size_t count);

    protected:
        friend class ObjectIterable;        
        // iterator_ptr valid both in case of m_query_iterator and m_sorted_iterator
        BaseIterator *m_iterator_ptr = nullptr;

        struct Decoration
        {
            std::vector<std::unique_ptr<QueryObserver> > m_query_observers;
            // decorators collected from observers for the last item
            std::vector<ObjectPtr> m_decorators;

            Decoration(std::vector<std::unique_ptr<QueryObserver> > &&query_observers);

            inline unsigned int size() const {
                return m_query_observers.size();
            }

            bool empty() const {
                return m_query_observers.empty();
            }
        };
        
        Decoration m_decoration;
        Slice m_slice;        
        
        // Unload object by address (must be from this iterator) skipping instance ID validation        
        virtual ObjectSharedPtr unload(Address) const;
        ObjectSharedPtr unload(db0::swine_ptr<Fixture> &, Address) const;
    };
    
}