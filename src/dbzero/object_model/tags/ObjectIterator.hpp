#pragma once

#include <memory>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/core/collections/full_text/SortedIterator.hpp>
#include <dbzero/core/collections/full_text/IteratorFactory.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include "QueryObserver.hpp"

namespace db0::object_model

{
    
    class ClassFactory;
    class QueryObserver;
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
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&, 
            std::vector<std::unique_ptr<QueryObserver> > && = {});

        // Construct from a sorted iterator
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<SortedIterator> &&,
            std::vector<std::unique_ptr<QueryObserver> > && = {});

        // Construct from IteratorFactory (specialized on first use)
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<IteratorFactory> &&,
            std::vector<std::unique_ptr<QueryObserver> > && = {});

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
         * @param query_observer optional observer (to retrieve result decorations)
        */
        static ObjectIterator *makeNew(void *at_ptr, db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&,
            std::vector<std::unique_ptr<QueryObserver> > && = {});
        
        // Begin from the underlying full-text iterator (or fail if initialized from a sorted iterator)
        // collect query observers (make copy)
        std::unique_ptr<QueryIterator> beginFTQuery(std::vector<std::unique_ptr<QueryObserver> > &,
            int direction = -1) const;
        
        std::unique_ptr<SortedIterator> beginSorted() const;
        
        // Release the underlying query iterator + append all observers into provided output buffer, render this instance invalid
        std::unique_ptr<QueryIterator> releaseQuery(std::vector<std::unique_ptr<QueryObserver> > &);
        
        bool isSorted() const;
                
        db0::swine_ptr<Fixture> getFixture() const {
            return m_fixture;
        }

        bool isNull() const;
        
        void serialize(std::vector<std::byte> &) const override;

        // placement-new deserialization
        static void deserialize(void *at_ptr, db0::swine_ptr<Fixture> &, std::vector<std::byte>::const_iterator &, 
            std::vector<std::byte>::const_iterator);
        
        /**
         * Measure the similarity between the 2 query iterators
         * @return value from the range [0 - 1], where 0 = identical and 1 = completely different
        */
        double compareTo(const ObjectIterator &other) const;

        std::vector<std::byte> getSignature() const;
        
        inline unsigned int numDecorators() const {
            return m_decoration.size();
        }
        
        const std::vector<ObjectPtr> &getDecorators() const {
            return m_decoration.m_decorators;
        }

    protected:
        mutable db0::swine_ptr<Fixture> m_fixture;
        const ClassFactory &m_class_factory;
        std::unique_ptr<QueryIterator> m_query_iterator;
        std::unique_ptr<SortedIterator> m_sorted_iterator;
        std::unique_ptr<IteratorFactory> m_factory;
        std::unique_ptr<BaseIterator> m_base_iterator;
        // iterator_ptr valid both in case of m_query_iterator and m_sorted_iterator
        BaseIterator *m_iterator_ptr = nullptr;
        bool m_initialized = false;

        struct Decoration
        {
            std::vector<std::unique_ptr<QueryObserver> > m_query_observers;
            // decorators collected from observers for the last item
            std::vector<ObjectPtr> m_decorators;

            Decoration(std::vector<std::unique_ptr<QueryObserver> > &&query_observers);

            inline unsigned int size() const {
                return m_query_observers.size();
            }

            bool empty() const {
                return m_query_observers.empty();
            }
        };

        Decoration m_decoration;

        void assureInitialized();
    };
    
}