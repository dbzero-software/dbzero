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
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        // full-text query iterator (KeyT must be std::uint64_t)
        using QueryIterator = FT_Iterator<std::uint64_t>;
        using SortedIterator = db0::SortedIterator<std::uint64_t>;
        using IteratorFactory = db0::IteratorFactory<std::uint64_t>;
        // a common base for full-text and sorted iterators
        using BaseIterator = db0::FT_IteratorBase;
        using FilterFunc = std::function<bool(ObjectPtr)>;

        // Construct from a full-text query iterator
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&,
            std::vector<std::unique_ptr<QueryObserver> > && = {}, const std::vector<FilterFunc> & = {});

        // Construct from a sorted iterator
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<SortedIterator> &&,
            std::vector<std::unique_ptr<QueryObserver> > && = {}, const std::vector<FilterFunc> & = {});

        // Construct from IteratorFactory (specialized on first use)
        ObjectIterator(db0::swine_ptr<Fixture>, std::shared_ptr<IteratorFactory> factory,
            std::vector<std::unique_ptr<QueryObserver> > && = {}, const std::vector<FilterFunc> & = {});

        virtual ~ObjectIterator() = default;

        /**
         * Start the iteration over, possibly with additional application of provided filters
         */
        virtual std::unique_ptr<ObjectIterator> iter(const std::vector<FilterFunc> & = {}) const;

        /**
         * Retrieve next object from the iterator         
         * @return nullptr if end of iteration reached
        */
        ObjectSharedPtr next();
                
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
        
        static std::unique_ptr<ObjectIterator> deserialize(db0::swine_ptr<Fixture> &, std::vector<std::byte>::const_iterator &,
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

        const std::vector<FilterFunc> &getFilters() const {
            return m_filters;
        }
        
    protected:
        mutable db0::swine_ptr<Fixture> m_fixture;
        const ClassFactory &m_class_factory;
        std::unique_ptr<QueryIterator> m_query_iterator;
        std::unique_ptr<SortedIterator> m_sorted_iterator;
        std::shared_ptr<IteratorFactory> m_factory;
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
        const std::vector<FilterFunc> m_filters;

        // iter constructor
        ObjectIterator(db0::swine_ptr<Fixture>, const ClassFactory &, std::unique_ptr<QueryIterator> &&,
            std::unique_ptr<SortedIterator> &&, std::shared_ptr<IteratorFactory>, std::vector<std::unique_ptr<QueryObserver> > &&,
            std::vector<FilterFunc> &&filters);

        void assureInitialized();
        void assureInitialized() const;

        /**
         * Unload object by address (must be from this iterator)
        */
        virtual ObjectSharedPtr unload(std::uint64_t address) const;
    };
    
}