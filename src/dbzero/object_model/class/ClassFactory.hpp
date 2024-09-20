#pragma once

#include <unordered_map>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/collections/map/v_map.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/object_model/has_fixture.hpp>

namespace db0::object_model

{
    
    class Class;
    struct ObjectId;
    using ClassPtr = db0::db0_ptr<Class>;
    using VClassMap = db0::v_map<db0::o_string, ClassPtr, o_string::comp_t>;
    using VClassPtrIndex = db0::v_bindex<ClassPtr>;
    using namespace db0;
    using namespace db0::pools;
    
    struct [[gnu::packed]] o_class_factory: public o_fixed<o_class_factory>
    {
        // 4 variants of class identification
        db0::db0_ptr<VClassMap> m_class_map_ptrs[4];
        // index of all class pointers
        db0::db0_ptr<VClassPtrIndex> m_class_ptr_index_ptr;
        std::array<std::uint64_t, 4> m_reserved = {0, 0, 0, 0};
        
        o_class_factory(Memspace &memspace);
    };
    
    class ClassFactory: public db0::has_fixture<v_object<o_class_factory> >
    {
    public:
        using super_t = db0::has_fixture<v_object<o_class_factory> >;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = typename LangToolkit::TypeObjectSharedPtr;

        ClassFactory(db0::swine_ptr<Fixture> &);

        ClassFactory(db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        // Copy all cached type mappings from another ClassFactory
        void initWith(const ClassFactory &);

        /**
         * Get existing class (or raise exception if not found)
         * @param lang_type the language specific type object (e.g. Python class)
         * @param typeid the user assigned type ID (optional)
        */
        std::shared_ptr<Class> getExistingType(TypeObjectPtr lang_type) const;
        
        /**
         * A non-throwing version of getExistingType
         * @return nullptr if the class is not found
        */
        std::shared_ptr<Class> tryGetExistingType(TypeObjectPtr lang_type) const;
        
        /**
         * Get existing or create a new DBZero class instance
         * @param lang_type the language specific memo type object (e.g. Python class)
         * @param type_id the user assigned type ID (optional)
         * @param prefix_name name of the associated prefix, for scoped classes        
        */
        std::shared_ptr<Class> getOrCreateType(TypeObjectPtr lang_type);
        
        // reference the DBZero object model's class by its pointer
        std::shared_ptr<Class> getTypeByPtr(ClassPtr, TypeObjectPtr lang_type) const;

        // retrieve type, possibly without a binding to any existing language specific type
        // the result is not cached and can only be used for read-only operations
        std::shared_ptr<const Class> getConstTypeByPtr(ClassPtr) const;
        
        // the index operator can only be used for pulling existing Class objects from the cache
        std::shared_ptr<Class> operator[](ClassPtr) const;

        void commit() const;
        
        void detach() const;

        // Iterate over all classes (whether having language specific type assigned or not)
        void forAll(std::function<void(std::shared_ptr<const Class>)>) const;
        
    private:
        // Language specific type to DBZero class mapping
        mutable std::unordered_map<TypeObjectPtr, std::shared_ptr<Class> > m_type_cache;
        // DBZero Class objects by pointer (may not have language specific type assigned yet)
        // Class instance may exist in ptr_cache but not in class_cache
        mutable std::unordered_map<ClassPtr, std::shared_ptr<Class> > m_ptr_cache;
        // class maps in 4 variants: 0: type ID, 1: name + module, 2: name + fields: 3: module + fields
        std::array<VClassMap, 4> m_class_maps;
        VClassPtrIndex m_class_ptr_index;

        // Pull through by-pointer cache
        std::shared_ptr<Class> getType(ClassPtr, std::shared_ptr<Class>);
        
        ClassPtr tryFindClassPtr(TypeObjectPtr lang_type, const char *type_id) const;

        // check if the class object (possibly from a different snapshot) exists in the current snapshot
        bool exists(const Class &) const;
    };

    std::optional<std::string> getNameVariant(ClassFactory::TypeObjectPtr lang_type,
        const char *type_id, int variant_id);
    
    // fetch immuatable Class object by UUID or throw
    // the result may not be linked to any language specific type and can only be used for read-only operations
    std::shared_ptr<const Class> fetchConstClass(db0::Snapshot &, const ObjectId &class_uuid);
    
}