// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "SplitIterable.hpp"
#include "ObjectIterator.hpp"

namespace db0::object_model

{
    
    class ClassFactory;
    
    class SplitIterator: public ObjectIterator
    {   
    public:
        using FilterFunc = ObjectIterator::FilterFunc;
        using LangToolkit = ObjectIterator::LangToolkit;

        SplitIterator(db0::swine_ptr<Fixture>, std::vector<db0::swine_ptr<Fixture> > &split_fixtures, 
            std::unique_ptr<QueryIterator> &&, std::shared_ptr<Class> = nullptr, TypeObjectPtr lang_type = nullptr, 
            std::vector<std::unique_ptr<QueryObserver> > && = {}, const std::vector<FilterFunc> & = {}, 
            const SliceDef & = {});
        
        SplitIterator(db0::swine_ptr<Fixture>, std::vector<db0::swine_ptr<Fixture> > &split_fixtures,
            std::unique_ptr<SortedIterator> &&, std::shared_ptr<Class> = nullptr, TypeObjectPtr lang_type = nullptr, 
            std::vector<std::unique_ptr<QueryObserver> > && = {}, const std::vector<FilterFunc> & = {}, 
            const SliceDef & = {});
        
        SplitIterator(const SplitIterable &);
        
        virtual ~SplitIterator();
        
    protected:
        // unloads from all split fixtures (as a tuple)
        ObjectSharedPtr unload(Address) const override;
        
    private:
        mutable std::vector<db0::weak_swine_ptr<Fixture> > m_split_fixtures;
        // split-fixture specific class factories (only valid with the split fixture)
        mutable std::vector<const ClassFactory*> m_class_factories;
        // a temporary buffer for results
        mutable std::vector<ObjectSharedPtr> m_temp;
        
        void init(std::vector<db0::swine_ptr<Fixture> > &split_fixtures);
    };
    
}
