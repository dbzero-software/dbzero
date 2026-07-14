// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/core/collections/full_text/TagProduct.hpp>
#include <dbzero/workspace/Snapshot.hpp>

namespace db0::object_model

{
    
    class ClassFactory;
    class JoinIterator;
    class ObjectIterable;
    using Object = db0::object_model::Object;
    
    /**
     * Join query result iterable over unspecified type objects
     * all objects must be from the same prefix
     * The foundation class of the JoinIterator
    */
    class JoinIterable
    {
    public:
        using LangToolkit = Object::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;
        using TypeObjectPtr = LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = LangToolkit::TypeObjectSharedPtr;

        // tag-product query iterator (KeyT must be UniqueAddres)
        using TP_Iterator = TagProduct<UniqueAddress>;
        // a common base for full-text and sorted iterators
        using BaseIterator = db0::FT_IteratorBase;
        using FilterFunc = std::function<bool(ObjectPtr*)>;
        
        JoinIterable(JoinIterable &&) = default;
        
        JoinIterable(db0::swine_ptr<Fixture>, std::unique_ptr<TP_Iterator> &&, std::vector<std::shared_ptr<Class> > && = {},
            const std::vector<TypeObjectPtr> & = {}, const std::vector<FilterFunc> & = {});
        JoinIterable(db0::swine_ptr<Fixture>, std::unique_ptr<TP_Iterator> &&, const std::vector<std::shared_ptr<Class> > & = {},
            const std::vector<TypeObjectSharedPtr> & = {}, const std::vector<FilterFunc> & = {});
        
        // Construct with additional filters
        JoinIterable(const JoinIterable &, const std::vector<FilterFunc> &);
        
        virtual ~JoinIterable();
        
        virtual std::shared_ptr<JoinIterator> iter() const;
        
        // Retrieve the number of elements in the iterable
        // this operation requires query scan but is considerably faster than iteration on the Python side
        // especially in the case of a sorted iterable
        std::size_t getSize() const;
        
        db0::swine_ptr<Fixture> getFixture() const;
        
        bool isNull() const;
                        
        const std::vector<FilterFunc> &getFilters() const {
            return m_filters;
        }
        
        // Get type of the results where specified
        const std::vector<std::shared_ptr<Class> > &getTypes() const;

        // Get associated language specific types where specified
        const std::vector<TypeObjectSharedPtr> &getLangTypes() const;
        
        // NOTE: ObjectIterable might be related with a specific context / scope (e.g. snapshot)
        // to prevent context deletion before the query, it's important to attach it
        // otherwise a segfault might happen when query iterated over, after closing the context
        void attachContext(ObjectPtr) const;
        
        bool empty() const;
        
    protected:
        mutable db0::weak_swine_ptr<Fixture> m_fixture;
        const ClassFactory &m_class_factory;
        std::unique_ptr<TP_Iterator> m_iterator;        
        std::vector<std::shared_ptr<Class> > m_types;
        std::vector<TypeObjectSharedPtr> m_lang_types;
        std::vector<FilterFunc> m_filters;
        mutable ObjectSharedPtr m_lang_context;

        void postInit();
    };
    
    /**
     * Resolve join parameters from user supplied arguments
     * @param args arguments passed to the find method
     * @param nargs number of arguments
     * @param prefix_name explicitly requested scope (fixture) to use, if not provided then the scope will be determined from the arguments
     * @return the join associated fixture (or exception raised if could not be determined)
     */
    db0::swine_ptr<Fixture> getJoinParams(db0::Snapshot &, JoinIterable::ObjectPtr const *args, std::size_t nargs,
        JoinIterable::ObjectPtr join_on_arg, std::vector<const ObjectIterable*> &object_iterables, 
        const ObjectIterable* &tag_iterable, std::vector<std::unique_ptr<ObjectIterable> > &iter_buf,
        const char *prefix_name = nullptr);
    
}