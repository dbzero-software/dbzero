// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_map>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/collections/map/v_map.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/object_model/has_fixture.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{
    
    class Class;
    struct ObjectId;
    using ClassPtr = db0::db0_ptr<Class>;
    using VClassMap = db0::v_map<db0::o_string, ClassPtr, o_string::comp_t>;
    using VClassPtrIndex = db0::v_bindex<ClassPtr>;
    using namespace db0;
    using namespace db0::pools;

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_class_factory: public o_fixed_versioned<o_class_factory>
    {
        // 4 variants of class identification
        db0::db0_ptr<VClassMap> m_class_map_ptrs[4];
        // index of all class pointers
        db0::db0_ptr<VClassPtrIndex> m_class_ptr_index_ptr;
        std::array<std::uint64_t, 4> m_reserved = {0, 0, 0, 0};
        
        o_class_factory(Memspace &memspace);
    };
DB0_PACKED_END    
    
    ClassFactory &getClassFactory(Fixture &);
    const ClassFactory &getClassFactory(const Fixture &);
    
    class ClassFactory: public db0::has_fixture<v_object<o_class_factory> >
    {
    public:
        using super_t = db0::has_fixture<v_object<o_class_factory> >;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = typename LangToolkit::TypeObjectSharedPtr;

        ClassFactory(db0::swine_ptr<Fixture> &);
        ClassFactory(db0::swine_ptr<Fixture> &, Address address);
        ~ClassFactory();
        
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
         * Get existing or create a new dbzero class instance
         * @param lang_type the language specific memo type object (e.g. Python class)
         * @param type_id the user assigned type ID (optional)
         * @param prefix_name name of the associated prefix, for scoped classes        
        */
        std::shared_ptr<Class> getOrCreateType(TypeObjectPtr lang_type);

        // non-throwing version of getOrCreateType
        std::shared_ptr<Class> tryGetOrCreateType(TypeObjectPtr lang_type);
        
        struct ClassItem
        {
            std::shared_ptr<Class> m_class;    
            TypeObjectSharedPtr m_lang_type;

            bool operator!() const {
                return !m_class;
            }
        };
        
        // reference the dbzero object model's class by its pointer
        // @param optional language specific type object if known
        ClassItem getTypeByPtr(ClassPtr, TypeObjectPtr lang_type = nullptr) const;
        ClassItem getTypeByAddr(Address, TypeObjectPtr lang_type = nullptr) const;
        ClassItem tryGetTypeByPtr(ClassPtr, TypeObjectPtr lang_type = nullptr) const;
        ClassItem tryGetTypeByAddr(Address, TypeObjectPtr lang_type = nullptr) const;
        
        ClassItem getTypeByClassRef(std::uint32_t class_ref, TypeObjectPtr lang_type = nullptr) const;        
        // May return invalid ClassItem if the class is not found
        ClassItem tryGetTypeByClassRef(std::uint32_t class_ref, TypeObjectPtr lang_type = nullptr) const;        

        void flush() const;
        
        // discard all changes stored in the internal flush buffers (e.g. schema updates)
        void rollback();
        
        void commit() const;
        
        void detach() const;

        // Iterate over all classes (whether having language specific type assigned or not)
        void forAll(std::function<void(const Class &)>) const;
        
        // Get lang type associated with a specific class (if known) or throw
        TypeObjectSharedPtr getLangType(const Class &) const;
        TypeObjectSharedPtr getLangType(const ClassItem &) const;
        bool hasLangType(const Class &) const;
        
        // calculate class-ref from its address
        std::uint32_t getClassRef(Address class_addr) const;

    private:
        // Language specific type to dbzero class mapping
        mutable std::unordered_map<TypeObjectPtr, std::shared_ptr<Class> > m_type_cache;
        // dbzero Class objects by pointer (may not have language specific type assigned yet)
        // Class instance may exist in ptr_cache but not in class_cache
        mutable std::unordered_map<ClassPtr, ClassItem> m_ptr_cache;
        // class maps in 4 variants: 0: type ID, 1: name + module, 2: name + fields: 3: module + fields
        std::array<VClassMap, 4> m_class_maps;
        VClassPtrIndex m_class_ptr_index;
        // buffers with keys for potential rollback
        mutable std::vector<TypeObjectSharedPtr> m_pending_types;
        mutable std::vector<ClassPtr> m_pending_ptrs;
        // starting address of the "types" slot
        const std::pair<std::uint64_t, std::uint64_t> m_type_slot_addr_range;
        
        // Pull through by-pointer cache
        std::shared_ptr<Class> getType(ClassPtr, std::shared_ptr<Class>, TypeObjectPtr lang_type) const;
        
        ClassPtr tryFindClassPtr(TypeObjectPtr lang_type, const char *type_id) const;

        // check if the class object (possibly from a different snapshot) exists in the current snapshot
        bool exists(const Class &) const;

        // try finding language specific type object in the TypeManager's cache
        TypeObjectPtr tryFindLangType(const Class &) const;
    };
    
    std::optional<std::string> getNameVariant(ClassFactory::TypeObjectPtr lang_type,
        const char *type_id, int variant_id);    
    
}