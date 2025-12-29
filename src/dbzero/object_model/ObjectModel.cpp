// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectModel.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Config.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>
#include <dbzero/object_model/bytes/ByteArray.hpp>

namespace db0::object_model

{
    
    std::function<void(db0::swine_ptr<Fixture> &, bool is_new, bool read_only, bool is_snapshot)> initializer()
    {       
        using TagIndex = db0::object_model::TagIndex;
        using ClassFactory = db0::object_model::ClassFactory;
        using EnumFactory = db0::object_model::EnumFactory;
        using Index = db0::object_model::Index;
        using Set = db0::object_model::Set;
        using Dict = db0::object_model::Dict;
        using FT_BaseIndexLong = db0::object_model::FT_BaseIndex<db0::num_pack<std::uint64_t, 2u> >;
        using LangToolkit = db0::object_model::LangConfig::LangToolkit;
        
        return [](db0::swine_ptr<Fixture> &fixture, bool is_new, bool read_only, bool is_snapshot)
        {
            // static GC0 bindings initialization
            GC0::registerTypes<Class, Object, List, Set, Dict, Tuple, Index, Enum, ByteArray>();
            auto &oc = fixture->getObjectCatalogue();
            if (is_new) {
                assert(!is_snapshot);
                if (read_only) {
                    THROWF(db0::InternalException) << "Cannot create a new fixture in read-only mode";
                }
                // create GC0 instance first
                auto &gc0 = fixture->createGC0(fixture);
                // create ClassFactory and register with the object catalogue
                auto &class_factory = fixture->addResource<ClassFactory>(fixture);
                auto &enum_factory = fixture->addResource<EnumFactory>(fixture);
                auto &tag_index = fixture->addResource<TagIndex>(
                    *fixture, 
                    class_factory, 
                    enum_factory, 
                    fixture->getLimitedStringPool(), 
                    fixture->getVObjectCache(),
                    fixture->addMutationHandler()
                );
                
                // flush from tag index on fixture commit (or close on close)
                fixture->addCloseHandler([&](bool commit) {
                    if (commit) {
                        tag_index.commit();
                        class_factory.commit();
                        enum_factory.commit();                        
                    } else {
                        tag_index.close();
                    }
                });

                fixture->addDetachHandler([&]() {
                    tag_index.detach();
                    class_factory.detach();
                    enum_factory.detach();
                });

                fixture->addRollbackHandler([&]() {
                    class_factory.rollback();
                    tag_index.rollback();                
                });
                
                fixture->addFlushHandler([&]() {
                    class_factory.flush();
                    tag_index.flush();
                });
                                
                // register resources with the object catalogue
                oc.addUnique(tag_index);
                oc.addUnique(class_factory);
                oc.addUnique(enum_factory);
                oc.addUnique(gc0);
            } else {
                // initialize GC0
                // FIXME: optimization possible - we can skip creating GC0 after implementing LangCacheView::detach
                // currently in read-only fixtures GC0 serves the function of detachable object tracking
                if (!is_snapshot) {
                    // snapshots don't require GC0                    
                    fixture->createGC0(fixture, oc.findUnique<db0::GC0>()->second(), read_only);
                }
                     
                auto &class_factory = fixture->addResource<ClassFactory>(fixture, oc.findUnique<ClassFactory>()->second());
                auto &enum_factory = fixture->addResource<EnumFactory>(fixture, oc.findUnique<EnumFactory>()->second());
                auto &tag_index = fixture->addResource<TagIndex>(
                    fixture->myPtr(oc.findUnique<TagIndex>()->second()), 
                    class_factory,
                    enum_factory,
                    fixture->getLimitedStringPool(), 
                    fixture->getVObjectCache(),
                    fixture->addMutationHandler()
                );
                
                // flush from tag index on fixture commit (or close on close)
                fixture->addCloseHandler([&](bool commit) {
                    if (commit) {
                        tag_index.commit();
                        class_factory.commit();
                        enum_factory.commit();                       
                    } else {
                        tag_index.close();
                    }
                });

                fixture->addDetachHandler([&]() {
                    tag_index.detach();
                    class_factory.detach();
                    enum_factory.detach();
                });
                
                fixture->addRollbackHandler([&]() {
                    class_factory.rollback();
                    tag_index.rollback();                    
                });
                
                fixture->addFlushHandler([&]() {
                    class_factory.flush();
                    tag_index.flush();
                });
                if (fixture->getAccessType() == db0::AccessType::READ_WRITE) {
                    // execute GC0::collect when opening an existing fixture as read-write
                    fixture->getGC0().collect();
                }
            }
        };
    }
    
}