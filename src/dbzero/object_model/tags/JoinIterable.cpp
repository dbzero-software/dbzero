// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "JoinIterable.hpp"
#include "JoinIterator.hpp"
#include "ObjectIterable.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/tags/TagDef.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::object_model

{

    JoinIterable::JoinIterable(db0::swine_ptr<Fixture> fixture, std::unique_ptr<TP_Iterator> &&iterator,
        std::vector<std::shared_ptr<Class> > &&types, const std::vector<TypeObjectPtr> &lang_types,
        const std::vector<FilterFunc> &filters)
        : m_fixture(fixture)
        , m_class_factory(getClassFactory(*fixture))
        , m_iterator(std::move(iterator))
        , m_types(std::move(types))        
        , m_filters(filters)
    {
        m_lang_types.reserve(lang_types.size());
        for (auto &lt: lang_types) {
            m_lang_types.push_back(lt);
        }
        postInit();
    }
    
    JoinIterable::JoinIterable(db0::swine_ptr<Fixture> fixture, std::unique_ptr<TP_Iterator> &&iterator,
        const std::vector<std::shared_ptr<Class> > &types, const std::vector<TypeObjectSharedPtr> &lang_types,
        const std::vector<FilterFunc> &filters)
        : m_fixture(fixture)
        , m_class_factory(getClassFactory(*fixture))
        , m_iterator(std::move(iterator))
        , m_types(types)
        , m_lang_types(lang_types)
        , m_filters(filters)
    {
        postInit();
    }

    void JoinIterable::postInit()
    {
        if (m_iterator) {
            while (m_types.size() < m_iterator->getDimension()) {
                m_types.emplace_back();
            }
            while (m_lang_types.size() < m_iterator->getDimension()) {
                m_lang_types.emplace_back();
            }
        }
    }

    JoinIterable::~JoinIterable()
    {
    }
    
    db0::swine_ptr<Fixture> JoinIterable::getFixture() const
    {
        auto fixture = m_fixture.lock();
        if (!fixture) {
            THROWF(db0::InputException) << "JoinIterator is no longer accessible (prefix or snapshot closed)" << THROWF_END;
        }
        return fixture;
    }

    const std::vector<std::shared_ptr<Class> > &JoinIterable::getTypes() const {
        return m_types;
    }
        
    const std::vector<JoinIterable::TypeObjectSharedPtr> &JoinIterable::getLangTypes() const {
        return m_lang_types;
    }

    bool JoinIterable::isNull() const {
        return !m_iterator;
    }
    
    bool JoinIterable::empty() const {
        return isNull() || m_iterator->isEnd();
    }
    
    std::size_t JoinIterable::getSize() const
    {
        if (isNull()) {
            return 0;
        }
        std::size_t count = 0;
        auto it = m_iterator->begin();
        while (!it->isEnd()) {
            it->next();
            ++count;
        }
        return count;
    }
    
    std::shared_ptr<JoinIterator> JoinIterable::iter() const
    {
        auto query = m_iterator->begin();
        return std::make_shared<JoinIterator>(
            getFixture(), std::move(query), m_types, m_lang_types, m_filters
        );
    }
    
    void JoinIterable::attachContext(ObjectPtr lang_context) const {
        m_lang_context = lang_context;
    }

    std::uint64_t getJoinFixtureUUID(JoinIterable::ObjectPtr obj_ptr)
    {
        using LangToolkit = JoinIterable::LangToolkit;
        
        // NOTE: we don't report fixture UUID for tags since foreign tags (i.e. from different scope) are allowed        
        if (!obj_ptr || LangToolkit::isTag(obj_ptr)) {
            return 0;
        }
        
        return LangToolkit::getFixtureUUID(obj_ptr);
    }

    db0::swine_ptr<Fixture> getJoinScope(db0::Snapshot &workspace, JoinIterable::ObjectPtr const *args,
        std::size_t nargs, const char *prefix_name)
    {
        if (prefix_name) {
            return workspace.getFixture(prefix_name);
        }
        
        std::uint64_t fixture_uuid = 0;
        for (std::size_t i = 0; i < nargs; ++i) {
            auto uuid = getJoinFixtureUUID(args[i]);
            if (fixture_uuid && uuid && uuid != fixture_uuid) {
                THROWF(db0::InputException) << "Inconsistent prefixes in join() query";
            }
            if (uuid) {
                fixture_uuid = uuid;
            }
        }
        
        return workspace.getFixture(fixture_uuid);
    }
    
    db0::swine_ptr<Fixture> getJoinParams(db0::Snapshot &workspace, JoinIterable::ObjectPtr const *args, std::size_t nargs,
        JoinIterable::ObjectPtr join_on_arg, std::vector<const ObjectIterable*> &object_iterables, 
        const ObjectIterable* &tag_iterable, std::vector<std::unique_ptr<ObjectIterable> > &iter_buf,
        const char *prefix_name)
    {
        assert(join_on_arg);
        using LangToolkit = JoinIterable::LangToolkit;
        using TagIndex = db0::object_model::TagIndex;
        
        auto fixture = getJoinScope(workspace, args, nargs, prefix_name);
        auto &type_manager = LangToolkit::getTypeManager();
        auto &tag_index = fixture->get<TagIndex>();
        auto &class_factory = getClassFactory(*fixture);
        
        auto make_iterable = [&](ObjectPtr py_arg) -> const ObjectIterable* {
            if (LangToolkit::isIterable(py_arg)) {
                return &type_manager.extractObjectIterable(py_arg);
            } else if (LangToolkit::isType(py_arg)) {
                auto lang_type = type_manager.getTypeObject(py_arg);
                auto type = class_factory.tryGetExistingType(lang_type);
                // try creating the dbzero class when type is accessed for the first time
                if (!type) {
                    FixtureLock fixture(class_factory.getFixture());
                    type = class_factory.getOrCreateType(lang_type);
                }
                
                auto query = tag_index.makeIterator(*type);
                iter_buf.emplace_back(std::make_unique<ObjectIterable>(
                    fixture, std::move(query), type, lang_type
                ));
                return iter_buf.back().get();
            }        
            THROWF(db0::InputException) << "Invalid argument type in join() query" << THROWF_END;            
        };

        for (std::size_t args_offset = 0; args_offset < nargs; ++args_offset) {
            auto &py_arg = args[args_offset];
            object_iterables.push_back(make_iterable(py_arg));            
        }

        tag_iterable = make_iterable(join_on_arg);
        return fixture;
    }

}

