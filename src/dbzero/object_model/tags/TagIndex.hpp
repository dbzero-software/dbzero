#pragma once

#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/collections/full_text/FT_BaseIndex.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <unordered_map>

namespace db0::object_model

{
    
    using Object = db0::object_model::Object;
    using RC_LimitedStringPool = db0::pools::RC_LimitedStringPool;
    
    /**
     * A class to represent a full-text (tag) index and the corresponding batch-update buffer
     * typically the TagIndex instance is associated with the Class object
    */
    class TagIndex
    {
    public:
        using LangToolkit = typename Object::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        // full-text query iterator
        using QueryIterator = FT_Iterator<std::uint64_t>;
        using ShortTagT = std::uint64_t;

        TagIndex(const ClassFactory &, RC_LimitedStringPool &, db0::FT_BaseIndex<ShortTagT> &);
        
        virtual ~TagIndex();
        
        void addTag(ObjectPtr memo_ptr, ShortTagT tag_addr);

        void addTags(ObjectPtr memo_ptr, ObjectPtr const *lang_args, std::size_t nargs);

        void removeTags(ObjectPtr memo_ptr, ObjectPtr const *lang_args, std::size_t nargs);
        
        /**
         * Construct query result iterator (resolve and execute language specific query)
         * args - will be AND-combined
         * @param type additional return value (if type was matched)
         */
        std::unique_ptr<QueryIterator> find(ObjectPtr const *args, std::size_t nargs,
            std::shared_ptr<Class> &type) const;

        // Clears the entire contents
        void clear();

        /**
        std::shared_ptr<DBZQueryIterator> beginFindAll(std::shared_ptr<DBZClass>, 
            const std::vector<dbz::long_ptr> &tag_ptrs) const;
        */
            
        // Get the underlying ObjectBook instance for querying
        // commit any pending transaction on tags        
        // const dbz::TheBook &getTheBook() const;

        // Flush any pending updates from the internal buffers
        void flush() const;
        
        // Close tag index without flushing any pending updates
        void close();

    private:
        using TypeId = db0::bindings::TypeId;

        const ClassFactory &m_class_factory;
        RC_LimitedStringPool &m_string_pool;
        db0::FT_BaseIndex<ShortTagT> &m_base_index_short;
        // Current batch-operation buffer (may not be initialized)
        mutable db0::FT_BaseIndex<ShortTagT>::BatchOperationBuilder m_batch_operation_short;
        // A cache of language objects held until flush/close is called
        // it's required to prevent unreferenced objects from being collected by GC
        // and to handle callbacks from the full-text index
        mutable std::unordered_map<std::uint64_t, ObjectSharedPtr> m_object_cache;
        
        db0::FT_BaseIndex<ShortTagT>::BatchOperationBuilder &getBatchOperationShort(ObjectPtr);

        /**
         * Make a tag from the provided argument (can be a string, type or a memo instance)        
        */        
        ShortTagT makeShortTag(ObjectPtr) const;
        ShortTagT makeShortTag(TypeId, ObjectPtr) const;
        ShortTagT makeShortTagFromString(ObjectPtr) const;
        ShortTagT makeShortTagFromMemo(ObjectPtr) const;
        ShortTagT makeShortTagFromEnumValue(ObjectPtr) const;

        bool addIterator(ObjectPtr, db0::FT_IteratorFactory<std::uint64_t> &factory,
            std::vector<std::unique_ptr<QueryIterator> > &neg_iterators) const;
    };

}
