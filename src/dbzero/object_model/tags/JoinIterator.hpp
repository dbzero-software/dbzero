// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/core/collections/full_text/TagProduct.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include "JoinIterable.hpp"

namespace db0::object_model

{
    
    class ClassFactory;
    using Object = db0::object_model::Object;

    /**
     * Tag-product query result iterator over unspecified type objects
     * all objects must be from the same prefix
    */
    class JoinIterator: public JoinIterable
    {
    public:
        // Construct from a tag-product query iterator
        // NOTE: lang_type is required here because results may need to be accessed as MemoBase
        // even if their exact type is unknown (this is for the model-less access to data)
        JoinIterator(db0::swine_ptr<Fixture>, std::unique_ptr<TP_Iterator> &&, const std::vector<std::shared_ptr<Class> > & = {},
            const std::vector<TypeObjectSharedPtr> & = {}, const std::vector<FilterFunc> & = {});
        
        // Construct iterator with additional filters
        JoinIterator(const JoinIterable &, const std::vector<FilterFunc> & = {});
        
        virtual ~JoinIterator() = default;
        
        /**
         * Retrieve next tuple of joined objects from the iterator
         * @return nullptr if end of iteration reached
        */
        virtual ObjectSharedPtr next();
        
    protected:
        friend class JoinIterable;
        std::unique_ptr<TP_Iterator> m_query_iterator;
        
        // Unload tuple of objects by their addresses (must be from this iterator) skipping instance ID validation
        virtual ObjectSharedPtr unloadTuple(const TP_Vector<UniqueAddress> &) const;
    };
    
}