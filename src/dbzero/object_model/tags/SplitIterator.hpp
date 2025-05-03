#pragma once

#include "SplitIterable.hpp"
#include "ObjectIterator.hpp"

namespace db0::object_model

{
    
    class SplitIterator: public ObjectIterator
    {   
    public:
        SplitIterator(db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&, std::shared_ptr<Class> = nullptr,
            TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {}, 
            const std::vector<FilterFunc> & = {}, const SliceDef & = {});
        
        ObjectSharedPtr next() override;
    };
    
}
