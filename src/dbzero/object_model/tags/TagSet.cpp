#include "TagSet.hpp"

namespace db0::object_model

{

    TagSet::TagSet(ObjectPtr const *args, std::size_t nargs, bool is_negated)
        : m_args(args)
        , m_nargs(nargs)
        , m_is_negated(is_negated)
    {
        // inc refs
        for (std::size_t i = 0; i < m_nargs; ++i) {
            LangToolkit::incRef(m_args[i]);
        }
    }

    TagSet::~TagSet()
    {
        // dec refs
        for (std::size_t i = 0; i < m_nargs; ++i) {
            LangToolkit::decRef(m_args[i]);
        }
    }
    
    bool TagSet::isNegated() const
    {
        return m_is_negated;
    }
    
    std::size_t TagSet::size() const
    {
        return m_nargs;
    }
    
    TagSet::ObjectPtr const *TagSet::getArgs() const
    {
        return m_args;
    }
    
}