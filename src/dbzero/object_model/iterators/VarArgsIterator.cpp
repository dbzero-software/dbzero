// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "VarArgsIterator.hpp"

namespace db0::object_model

{
    
    VarArgsIterator::VarArgsIterator(ObjectPtr const *args, unsigned int nargs, unsigned int index)
        : m_args(args)
        , m_nargs(nargs)        
        , m_index(index)
    {
        assert(m_index <= m_nargs);
    }
    
    bool VarArgsIterator::operator!=(VarArgsIterator const &other) const
    {
        return m_index != other.m_index;
    }

    VarArgsIterator::ObjectSharedPtr VarArgsIterator::operator*() const
    {
        assert(m_index < m_nargs);
        return m_args[m_index];
    }

    VarArgsIterator &VarArgsIterator::operator++()
    {
        ++m_index;
        return *this;
    }
    
    VarArgsIterator VarArgsIterator::end() const
    {
        return { m_args, m_nargs, m_nargs };
    }

}