// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/workspace/WeakFixtureVector.hpp>
#include <dbzero/object_model/object/ObjectAnyImpl.hpp>

namespace db0::object_model

{
    
    using ObjectAnyImpl = db0::object_model::ObjectAnyImpl;
    using Class = db0::object_model::Class;
    using RC_LimitedStringPool = db0::pools::RC_LimitedStringPool;

    /**
     * An convenience wrapper which implements operationas associated with
     * applying and querying object (dbzero Object instance) specific tags
    */
    class ObjectTagManager
    {
    public:
        using LangToolkit = typename ObjectAnyImpl::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;

        // construct as empty
        ObjectTagManager();
        ObjectTagManager(ObjectPtr const *memo_ptr, std::size_t nargs);
        ~ObjectTagManager();
        
        /**
         * Assign tags from a language-specific collection (e.g. Python list)
        */
        void add(ObjectPtr const *args, Py_ssize_t nargs);
        
        void remove(ObjectPtr const *args, Py_ssize_t nargs);
        
        static ObjectTagManager *makeNew(void *at_ptr, ObjectPtr const *memo_ptr, std::size_t nargs);

    private:
        // Memo object to be assigned tags to (language specific)
        struct ObjectInfo
        {
            ObjectSharedPtr m_lang_ptr;
            const ObjectAnyImpl *m_object_ptr = nullptr;
            TagIndex *m_tag_index_ptr = nullptr;
            std::shared_ptr<Class> m_type;
            AccessType m_access_mode;
            // has any tags already assigned
            bool m_has_tags = false;

            ObjectInfo() = default;
            ObjectInfo(ObjectPtr memo_ptr);

            void add(ObjectPtr const *args, Py_ssize_t nargs);
            void remove(ObjectPtr const *args, Py_ssize_t nargs);
            
            db0::swine_ptr<Fixture> getFixture() const;
        };
        
        const bool m_empty = false;
        // first object's info
        ObjectInfo m_info;
        // optional additional objects' info
        ObjectInfo *m_info_vec_ptr;
        std::size_t m_info_vec_size = 0;
        AccessType m_access_mode;
        // fixtures of the tagged objects (to mark as updated)
        db0::WeakFixtureVector m_fixtures;
        bool m_on_updated = false;
        
        void onUpdated();
    };
    
}
