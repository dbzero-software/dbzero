// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include "WeakSet.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/iterators/BaseIterator.hpp>
#include <dbzero/object_model/item/Pair.hpp>

namespace db0::object_model

{

    class WeakSetIterator : public BaseIterator<WeakSetIterator, WeakSet>
    {
    public:
        ObjectSharedPtr next() override;
        // hides BaseIterator::is_end - we report end when no live element is pending
        bool is_end() const;

    protected:
        friend class WeakSet;
        WeakSetIterator(WeakSet::const_iterator iterator, const WeakSet *ptr, ObjectPtr lang_set_ptr);

        void restore() override;

    private:
        SetIndex m_index;
        SetIndex::joinable_const_iterator m_join_iterator;
        std::uint64_t m_current_hash = 0;
        o_typed_item m_current_key;
        bool m_inner_end = false;
        // pre-fetched next live element (nullptr if iterator is exhausted)
        ObjectSharedPtr m_pending;

        void setJoinIterator();
        // Advance underlying iterator and store next non-expired item in m_pending.
        void prefetchNext();
    };

}
