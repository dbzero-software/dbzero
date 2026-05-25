// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "ObjectImplBase.hpp"
#include "o_immutable_object.hpp"

#include <functional>
#include <optional>

namespace db0::object_model

{

    class ObjectImmutableImpl: public ObjectImplBase<o_immutable_object, ObjectImmutableImpl>
    {
        // GC0 specific declarations
        GC0_Declare
    public:
        static constexpr unsigned char REALM_ID = o_immutable_object::REALM_ID;
        using super_t = ObjectImplBase<o_immutable_object, ObjectImmutableImpl>;
        
        template <typename... Args>
        ObjectImmutableImpl(Args&&... args)
            : super_t(std::forward<Args>(args)...)
        {
        }

        ObjectSharedPtr tryGet(MemberLoc, bool *is_auto_generated = nullptr) const;
        ObjectSharedPtr tryGet(const char *field_name, bool *is_auto_generated = nullptr) const;
        ObjectSharedPtr get(const char *field_name) const;
        ObjectSharedPtr getEmbeddedInstanceAtOffset(std::uint64_t offset) const;

        // Returns the address of an existing durable instance on intern-index hit.
        // Returns std::nullopt when this call created a new durable instance or
        // when the object was already initialized.
        std::optional<UniqueAddress> postInit(FixtureLock &);
        void setLangObject(ObjectPtr) const;
        void destroy();
        void dropInstance(FixtureLock &);

        static ObjectSharedPtr tryGetEmbeddedField(
            db0::swine_ptr<Fixture> &, ObjectPtr root_object, const o_embedded_object &,
            const FieldInfo &, AccessFlags member_flags
        );

    protected:
        friend super_t;

        ObjectSharedPtr tryGetEmbeddedField(const FieldInfo &) const;
        void getMembersImpl(std::unordered_set<std::string> &) const;
        void dropMembers(db0::swine_ptr<Fixture> &, Class &) const;

    private:
        ObjectPtr getLangObject() const;

        mutable ObjectPtr m_lang_object = nullptr;
    };
    
}
