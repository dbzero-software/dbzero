#include "SplitIterable.hpp"

namespace db0::object_model

{
    
    SplitIterable::SplitIterable(db0::swine_ptr<Fixture> fixture, const std::vector<db0::swine_ptr<Fixture> > &split_fixtures,
        std::unique_ptr<QueryIterator> &&query, std::shared_ptr<Class> type, TypeObjectPtr lang_type, 
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters)
        : ObjectIterable(fixture, std::move(query), type, lang_type, std::move(query_observers), filters)
        , m_split_fixtures(split_fixtures)        
    {
    }    
    
    ObjectIterable &SplitIterable::makeNew(void *at_ptr, db0::swine_ptr<Fixture> fixture,
        const std::vector<db0::swine_ptr<Fixture> > &split_fixtures, 
        std::unique_ptr<QueryIterator> &&query, std::shared_ptr<Class> type, TypeObjectPtr lang_type,
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters)
    {
        return *new(at_ptr) SplitIterable(fixture, split_fixtures, std::move(query), type, lang_type,
            std::move(query_observers), filters);
    }

}