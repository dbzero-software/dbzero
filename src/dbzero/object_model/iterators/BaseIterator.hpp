// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <dbzero/workspace/Config.hpp>

namespace db0::object_model

{

    /**
     * Base type for implementing collection specific iterators
     * @tparam ClassT the implemented iterator type
     * @tparam CollectionT the underlyging related collection type
    */
    template<typename ClassT, typename CollectionT>
    class BaseIterator
    {
    public:
        using ThisType = BaseIterator<ClassT, CollectionT>;
        using LangToolkit = db0::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using IteratorT = typename CollectionT::const_iterator;

        virtual ObjectSharedPtr next() = 0;
        
        BaseIterator(IteratorT iterator, const CollectionT *ptr, ObjectPtr lang_collection_ptr)
            : m_iterator(iterator)
            , m_collection(ptr)
            , m_lang_collection_shared_ptr(lang_collection_ptr)
            , m_member_flags(ptr->getMemberFlags())
        {
        }
        
        bool operator==(const ThisType &other) const
        {
            assureAttached();
            other.assureAttached();
            return m_iterator == other.m_iterator;
        }

        bool operator!=(const ThisType &other) const
        {
            assureAttached();
            other.assureAttached();
            return !(m_iterator == other.m_iterator);
        }

        bool is_end() const
        {
            assureAttached();
            return m_iterator.is_end();
        }
        
        void detach() const
        {
            // NOTE: this needs to be reimplemented to save key + iterator invalidation
            m_detached = true;
        }
        
        virtual void restore() = 0;

    protected:
        IteratorT m_iterator;
        const CollectionT *m_collection;
        // reference to persist collections' related language specific object
        ObjectSharedPtr m_lang_collection_shared_ptr;
        // member access flags (e.g. no_cache)
        const AccessFlags m_member_flags;
        mutable bool m_detached = false;
        
        void assureAttached() const
        {
            if (m_detached) {
                const_cast<ThisType *>(this)->restore();
                m_detached = false;
            }
        }
    };

}