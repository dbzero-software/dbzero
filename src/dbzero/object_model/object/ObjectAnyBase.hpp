// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/object_model/class/MemberID.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include "ValueTable.hpp"
#include "ObjectInitializer.hpp"
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/packed_int.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include "o_object.hpp"
#include "o_immutable_object.hpp"

namespace db0

{

    class Fixture;

}

namespace db0::object_model

{

    class Class;
    class ObjectAnyImpl;
    using Fixture = db0::Fixture;
    
    enum class ObjectOptions: std::uint8_t
    {
        // the dbzero instance has been deleted
        DROPPED = 0x01,
        // object is defunct - e.g. due to exception on __init__
        DEFUNCT = 0x02
    };
    
    using ObjectFlags = db0::FlagSet<ObjectOptions>;
    // NOTE: Object instances are created within the implementation specific realm_id (e.g. =1 for o_object)
    template <typename T> using ObjectVType = db0::v_object<T, 0, T::REALM_ID>;
    
    // Common init manager for all specializations
    class InitManager
    {
    public:
        static ObjectInitializerManager instance;
    };
    
    template <typename T, typename ImplT>
    class ObjectAnyBase: public db0::ObjectBase<ImplT, ObjectVType<T>, StorageClass::OBJECT_REF>
    {
    public:
        static constexpr unsigned char REALM_ID = T::REALM_ID;
        using super_t = db0::ObjectBase<ImplT, ObjectVType<T>, StorageClass::OBJECT_REF>;
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using TypeObjectPtr = typename LangToolkit::TypeObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using TypeManager = typename LangToolkit::TypeManager;
        using ObjectStem = ObjectVType<T>;
        using TypeInitializer = ObjectInitializer::TypeInitializer;
        
        db0::swine_ptr<Fixture> tryGetFixture() const;
        db0::swine_ptr<Fixture> getFixture() const;

        Memspace &getMemspace() const;

        inline std::shared_ptr<Class> getClassPtr() const {
            return this->m_type ? this->m_type : InitManager::instance.getInitializer(*this).getClassPtr();
        }
        
        inline const Class &getType() const {
            return this->m_type ? *this->m_type : InitManager::instance.getInitializer(*this).getClass();
        }
        
        Class &getType();
        
        /**
         * Change fixture of the uninitialized object
         * Object must not have any members yet either
         */
        void setFixture(db0::swine_ptr<Fixture> &);

        /**
         * The overloaded incRef implementation is provided to also handle non-fully initialized objects
        */
        void incRef(bool is_tag);
        void decRef(bool is_tag);
        
        // check for any refs (including auto-assigned type tags)
        bool hasAnyRefs() const;
        
        // check if any references from tags exist (i.e. are any tags assigned)
        bool hasTagRefs() const;
                
        Address getAddress() const;
        UniqueAddress getUniqueAddress() const;

        // NOTE: the operation is marked const because the dbzero state is not affected
        void setDefunct() const;

        inline bool isDropped() const {
            return m_flags.test(ObjectOptions::DROPPED);
        }

        inline bool isDefunct() const {
            return m_flags.test(ObjectOptions::DEFUNCT);
        }
        
        // is dropped or defunct
        inline bool isDead() const {
            return m_flags.any(
                static_cast<std::uint8_t>(ObjectOptions::DROPPED) | static_cast<std::uint8_t>(ObjectOptions::DEFUNCT)
            );
        }
        
        // the member called to indicate the object mutation
        void touch();
        
        void addExtRef() const;
        void removeExtRef() const;
        
        inline std::uint32_t getExtRefs() const {
            return m_ext_refs;
        }
        
    protected:        
        // Class will only be assigned after initialization
        std::shared_ptr<Class> m_type;
        mutable ObjectFlags m_flags;
        // reference counter for inner references from language objects
        // NOTE: inner references are held by internal dbzero buffers (e.g. TagIndex)
        // see also PyEXT_INCREF / PyEXT_DECREF
        mutable std::uint32_t m_ext_refs = 0;
        // A flag indicating that object's silent mutation has already been reflected
        // with the underlying MemLock / ResourceLock
        // NOTE: by silent mutation we mean a mutation that does not change data (e.g. +refcount(+-1) + (refcount-1))
        mutable bool m_touched = false;
        // NOTE: member assigned only to dropped objects (see replaceWithNull)
        // so that we can retrieve the address of the dropped instance after it has been destroyed
        const UniqueAddress m_unique_address;
        
        template <typename... Args> ObjectAnyBase(Args&&... args)
            : super_t(std::forward<Args>(args)...)
        {
        }
        
        // As a dropped object
        struct tag_as_dropped {};
        ObjectAnyBase(tag_as_dropped, UniqueAddress addr, unsigned int ext_refs);
        
        void _touch();
    };
    
    extern template class ObjectAnyBase<o_object_base, ObjectAnyImpl>;
    extern template class ObjectAnyBase<o_object, Object>;
    extern template class ObjectAnyBase<o_immutable_object, ObjectImmutableImpl>;
    
}

DECLARE_ENUM_VALUES(db0::object_model::ObjectOptions, 2)