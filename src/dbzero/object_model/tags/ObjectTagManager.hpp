#pragma once

#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>

namespace db0::object_model

{

    using Object = db0::object_model::Object;
    using Class = db0::object_model::Class;
    using RC_LimitedStringPool = db0::pools::RC_LimitedStringPool;

    /**
     * An convenience wrapper which implements operationas associated with
     * applying and querying object (DBZero Object instance) specific tags
    */
    class ObjectTagManager
    {
    public:
        using LangToolkit = typename Object::LangToolkit;
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

        void clear();

        static ObjectTagManager *makeNew(void *at_ptr, ObjectPtr const *memo_ptr, std::size_t nargs);

    private:
        // Memo object to be assigned tags to (language specific)
        struct ObjectInfo
        {
            ObjectSharedPtr m_lang_ptr;
            const Object *m_object_ptr = nullptr;
            TagIndex *m_tag_index_ptr = nullptr;
            std::shared_ptr<Class> m_type;

            ObjectInfo() = default;
            ObjectInfo(ObjectPtr memo_ptr);

            void add(ObjectPtr const *args, Py_ssize_t nargs);
            void remove(ObjectPtr const *args, Py_ssize_t nargs);
        };

        const bool m_empty = false;
        // first object's info
        ObjectInfo m_info;
        // optional additional objects' info
        ObjectInfo *m_info_vec_ptr;
        std::size_t m_info_vec_size = 0;
    };

}
