// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include "Dict.hpp"
#include "DictIterator.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>

namespace db0::object_model

{

    class DictView
    {
    public:
        using LangToolkit = db0::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;

        DictView(const Dict *dict, ObjectPtr lang_dict, IteratorType type);
        
        std::shared_ptr<DictIterator> getIterator() const;
        std::size_t size() const;
        
    private:
        const Dict *m_collection;
        IteratorType m_type;
        ObjectSharedPtr m_lang_dict_shared_ptr;
    };

}