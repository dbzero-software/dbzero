#pragma once

#include <memory>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/core/collections/full_text/SortedIterator.hpp>
#include <dbzero/core/collections/full_text/IteratorFactory.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/workspace/Snapshot.hpp>

namespace db0::object_model

{
    
    class ClassFactory;
    using Object = db0::object_model::Object;
    using Serializable = db0::Serializable;

    /**
     * Full-text query result iterator over unspecified type objects
     * all objects must be from the same prefix
    */
    class ObjectIterator: public Serializable
    {
    public:
        using LangToolkit = Object::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;

        // full-text query iterator (KeyT must be std::uint64_t)
        using QueryIterator = FT_Iterator<std::uint64_t>;
        using SortedIterator = db0::SortedIterator<std::uint64_t>;
        using IteratorFactory = db0::IteratorFactory<std::uint64_t>;
        // a common base for full-text and sorted iterators
        using BaseIterator = db0::FT_IteratorBase;

        // Construct from a full-text query iterator
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&);

        // Construct from a sorted iterator
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<SortedIterator> &&);
        
        // Construct from IteratorFactory (specialized on first use)
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<IteratorFactory> &&);

        virtual ~ObjectIterator() = default;
        
        /**
         * Retrieve next object's address from the iterator
         * @return false if end of iteration reached
        */
        bool next(std::uint64_t &addr);
        
        /**
         * Unload object by address (must be from this iterator)
        */
        virtual ObjectPtr unload(std::uint64_t address) const;
        
        /**
         * @param at_ptr memory location for object
         * @param args the tags, types or collections of tags, all args will be AND-combined
        */
        static ObjectIterator *makeNew(void *at_ptr, db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&);
        
        // Begin from the underlying full-text iterator (or fail if initialized from a sorted iterator)
        std::unique_ptr<QueryIterator> beginFTQuery(int direction = -1) const;

        std::unique_ptr<SortedIterator> beginSorted() const;
        
        bool isSorted() const;
                
        db0::swine_ptr<Fixture> getFixture() const {
            return m_fixture;
        }

        bool isNull() const;
        
        void serialize(std::vector<std::byte> &) const override;

        // placement-new deserialization
        static void deserialize(void *at_ptr, db0::swine_ptr<Fixture> &, std::vector<std::byte>::const_iterator &, 
            std::vector<std::byte>::const_iterator);
        
    protected:
        mutable db0::swine_ptr<Fixture> m_fixture;
        const ClassFactory &m_class_factory;
        std::unique_ptr<QueryIterator> m_query_iterator;
        std::unique_ptr<SortedIterator> m_sorted_iterator;
        std::unique_ptr<IteratorFactory> m_factory;
        // iterator_ptr valid both in case of m_query_iterator and m_sorted_iterator
        std::unique_ptr<BaseIterator> m_base_iterator;
        BaseIterator *m_iterator_ptr = nullptr;
        bool m_initialized = false;

        void assureInitialized();
    };
    
}