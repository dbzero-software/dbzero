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
    class ObjectIterator;
    using Object = db0::object_model::Object;
    using Serializable = db0::Serializable;

    /**
     * Full-text query result iterable over unspecified type objects
     * all objects must be from the same prefix
     * The foundation class of the ObjectIterator
    */
    class ObjectIterable: public Serializable
    {
    public:
        using LangToolkit = Object::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;
        using TypeObjectPtr = LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = LangToolkit::TypeObjectSharedPtr;

        // full-text query iterator (KeyT must be std::uint64_t)
        using QueryIterator = FT_Iterator<std::uint64_t>;
        using SortedIterator = db0::SortedIterator<std::uint64_t>;
        using IteratorFactory = db0::IteratorFactory<std::uint64_t>;
        // a common base for full-text and sorted iterators
        using BaseIterator = db0::FT_IteratorBase;
        using FilterFunc = std::function<bool(ObjectPtr)>;

        ObjectIterable(ObjectIterable &&) = default;

        // Construct from a full-text query iterator
        ObjectIterable(db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&, std::shared_ptr<Class> = nullptr, 
            TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {},
            const std::vector<FilterFunc> & = {});

        // Construct from a sorted iterator
        ObjectIterable(db0::swine_ptr<Fixture>, std::unique_ptr<SortedIterator> &&, std::shared_ptr<Class> = nullptr,
            TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {},
            const std::vector<FilterFunc> & = {});
        
        // Construct from IteratorFactory (specialized on first use)
        ObjectIterable(db0::swine_ptr<Fixture>, std::shared_ptr<IteratorFactory> factory, std::shared_ptr<Class> = nullptr,
            TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {},
            const std::vector<FilterFunc> & = {});
        
        virtual ~ObjectIterable() = default;

        /**
         * Start the iteration over, possibly with the application of additional provided filters
         */
        ObjectIterator &makeIter(void *at_ptr, const std::vector<FilterFunc> & = {}) const;
        
        // Begin from the underlying full-text iterator (or fail if initialized from a sorted iterator)
        // collect "rebased" query observers
        std::unique_ptr<QueryIterator> beginFTQuery(std::vector<std::unique_ptr<QueryObserver> > &,
            int direction = -1) const;
        
        std::unique_ptr<SortedIterator> beginSorted() const;
        
        // Retrieve the number of elements in the iterable
        // this operation requires query scan but is considerably faster than iteration on the Python side
        // especially in the case of a sorted iterable
        std::size_t getSize() const;
        
        bool isSorted() const;
        
        db0::swine_ptr<Fixture> getFixture() const;
        
        bool isNull() const;
        
        void serialize(std::vector<std::byte> &) const override;
        
        static std::unique_ptr<ObjectIterable> deserialize(db0::swine_ptr<Fixture> &, std::vector<std::byte>::const_iterator &,
            std::vector<std::byte>::const_iterator);
        
        /**
         * Measure the similarity between the 2 query iterables
         * @return value from the range [0 - 1], where 0 = identical and 1 = completely different
        */
        double compareTo(const ObjectIterable &other) const;

        std::vector<std::byte> getSignature() const;
        
        const std::vector<FilterFunc> &getFilters() const {
            return m_filters;
        }
        
        // Get associated language specific type of the results if it was specified
        TypeObjectPtr getLangType() const;

        static ObjectIterable &makeNew(void *at_ptr, ObjectIterable &&);

        static ObjectIterable &makeNew(void *at_ptr, db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&, 
            std::shared_ptr<Class> = nullptr, TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {}, 
            const std::vector<FilterFunc> & = {});

        static ObjectIterable &makeNew(void *at_ptr, db0::swine_ptr<Fixture>, std::unique_ptr<SortedIterator> &&,
            std::shared_ptr<Class> = nullptr, TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {}, 
            const std::vector<FilterFunc> & = {});

        static ObjectIterable &makeNew(void *at_ptr, db0::swine_ptr<Fixture>, std::shared_ptr<IteratorFactory> factory, 
            std::shared_ptr<Class> = nullptr, TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {}, 
            const std::vector<FilterFunc> & = {});
        
        // construct with additional filters
        ObjectIterable &makeNewAppendFilters(void *at_ptr, const std::vector<FilterFunc> &) const;
        // construct sorted
        ObjectIterable &makeNew(void *at_ptr, std::unique_ptr<SortedIterator> &&, std::vector<std::unique_ptr<QueryObserver> > && = {},
            const std::vector<FilterFunc> & = {}) const;
        ObjectIterable &makeNew(void *at_ptr, std::unique_ptr<QueryIterator> &&, std::vector<std::unique_ptr<QueryObserver> > && = {},
            const std::vector<FilterFunc> & = {}) const;
        
    protected:
        mutable db0::weak_swine_ptr<Fixture> m_fixture;
        const ClassFactory &m_class_factory;
        std::unique_ptr<QueryIterator> m_query_iterator;
        std::unique_ptr<SortedIterator> m_sorted_iterator;
        std::shared_ptr<IteratorFactory> m_factory;
        std::vector<std::unique_ptr<QueryObserver> > m_query_observers;
        const std::vector<FilterFunc> m_filters;        
        std::shared_ptr<Class> m_type = nullptr;
        TypeObjectSharedPtr m_lang_type = nullptr;

        // iter constructor
        ObjectIterable(db0::swine_ptr<Fixture>, const ClassFactory &, std::unique_ptr<QueryIterator> &&,
            std::unique_ptr<SortedIterator> &&, std::shared_ptr<IteratorFactory>, std::vector<std::unique_ptr<QueryObserver> > &&,
            std::vector<FilterFunc> &&filters, std::shared_ptr<Class>, TypeObjectPtr lang_type);
        
        // get the base iterator, possibly initialized from the factory
        const BaseIterator &getBaseIterator(std::unique_ptr<BaseIterator> &) const;
    };
    
}