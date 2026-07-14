// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/LangConfig.hpp>

namespace db0::object_model

{
    
    /**
     * Expose variable args under iterator's protocol
    */
    class VarArgsIterator
    {
    public:
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        VarArgsIterator(ObjectPtr const *args, unsigned int nargs, unsigned int index = 0);
        
        bool operator!=(VarArgsIterator const &other) const;
        ObjectSharedPtr operator*() const;
        VarArgsIterator &operator++();
        
        VarArgsIterator end() const;

    private:
        ObjectPtr const *m_args;
        unsigned int m_nargs;
        unsigned int m_index;
        
        VarArgsIterator();
    };

}
