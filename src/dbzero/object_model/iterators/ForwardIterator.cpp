// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ForwardIterator.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>

namespace db0::object_model

{

    ForwardIterator::ForwardIterator(ObjectSharedPtr lang_iterable)
        : m_lang_iterable(lang_iterable)
        , m_iter(LangToolkit::getIterator(lang_iterable.get()))
        , m_current_item(LangToolkit::next(m_iter.get()))
    {
    }
    
    bool ForwardIterator::operator!=(ForwardIterator const &other) const
    {
        return m_current_item != other.m_current_item;
    }

    ForwardIterator::ObjectSharedPtr ForwardIterator::operator*() const
    {
        assert(m_current_item.get());
        return m_current_item;
    }
    
    ForwardIterator &ForwardIterator::operator++()
    {
        m_current_item = LangToolkit::next(m_iter.get());
        return *this;
    }
    
    ForwardIterator ForwardIterator::end() {
        return {};
    }

}
