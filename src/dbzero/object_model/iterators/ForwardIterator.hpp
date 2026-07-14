// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/LangConfig.hpp>

namespace db0::object_model

{
    
    /**
     * Wraps language specific iterable
    */
    class ForwardIterator
    {
    public:
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        ForwardIterator(ObjectSharedPtr lang_iterable);
        
        bool operator!=(ForwardIterator const &other) const;
        ObjectSharedPtr operator*() const;
        ForwardIterator &operator++();
        
        static ForwardIterator end();

    private:
        ObjectSharedPtr m_lang_iterable;
        ObjectSharedPtr m_iter;
        ObjectSharedPtr m_current_item;
        
        ForwardIterator() = default;
    };

}
