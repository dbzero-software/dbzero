#include "ObjectModel.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/pandas/Block.hpp>
#include <dbzero/object_model/pandas/Dataframe.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/enum/Enum.hpp>

namespace db0::object_model

{
    
    std::function<void(db0::swine_ptr<Fixture> &, bool is_new)> initializer()
    {
        using Block = db0::object_model::pandas::Block;
        using DataFrame = db0::object_model::pandas::DataFrame;
        using FT_BaseIndex = db0::FT_BaseIndex;
        using TagIndex = db0::object_model::TagIndex;
        using ClassFactory = db0::object_model::ClassFactory;
        using Index = db0::object_model::Index;
        using Set = db0::object_model::Set;
        using Dict = db0::object_model::Dict;
        
        return [](db0::swine_ptr<Fixture> &fixture, bool is_new)
        {
            // static GC0 bindings initialization
            GC0::registerTypes<Class, Object, List, Block, DataFrame, Index, Enum>();
            auto &oc = fixture->getObjectCatalogue();
            if (is_new) {
                // create GC0 instance first
                auto &gc0 = fixture->addGC0(fixture);
                // create ClassFactory and register with the object catalogue
                auto &class_factory = fixture->addResource<ClassFactory>(fixture);
                auto &base_index = fixture->addResource<FT_BaseIndex>(*fixture, fixture->getVObjectCache());
                auto &tag_index = fixture->addResource<TagIndex>(class_factory, fixture->getLimitedStringPool(), base_index);

                // flush from tag index on fixture commit (or close on close)
                fixture->addCloseHandler([&tag_index](bool commit) {
                    if (commit) {
                        tag_index.flush();
                    } else {
                        tag_index.close();
                    }
                });

                // register resources with the object catalogue
                oc.addUnique(class_factory);
                oc.addUnique(gc0);
                oc.addUnique(base_index);
            } else {                
                fixture->addGC0(fixture, oc.findUnique<db0::GC0>()->second());
                auto &class_factory = fixture->addResource<ClassFactory>(fixture, oc.findUnique<ClassFactory>()->second());
                auto &base_index = fixture->addResource<FT_BaseIndex>(
                    fixture->myPtr(oc.findUnique<FT_BaseIndex>()->second()), fixture->getVObjectCache());
                auto &tag_index = fixture->addResource<TagIndex>(class_factory, fixture->getLimitedStringPool(), base_index);

                // flush from tag index on fixture commit (or close on close)
                fixture->addCloseHandler([&tag_index](bool commit) {
                    if (commit) {
                        tag_index.flush();
                    } else {
                        tag_index.close();
                    }
                });

                if (fixture->getAccessType() == db0::AccessType::READ_WRITE) {
                    // execute GC0::collect when opening an existing fixture as read-write
                    fixture->getGC0().collect();
                }
            }
        };
    }
    
}