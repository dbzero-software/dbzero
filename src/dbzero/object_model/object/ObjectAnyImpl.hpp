// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include "ObjectAnyBase.hpp"
#include "o_object.hpp"

#include <utility>

namespace db0::object_model

{
    
    // NOTE: ObjectAnyImpl is for reinterpret_cast purposes only
    // it allows accessing Object or ObjectImmutableImpl instances under a common base type
    class ObjectAnyImpl: public ObjectAnyBase<o_object_base, ObjectAnyImpl>
    {
    public:
        static constexpr unsigned char REALM_ID = o_object_base::REALM_ID;
        using super_t = ObjectAnyBase<o_object_base, ObjectAnyImpl>;

        template <typename StemT>
        static StemT castStem(ObjectStem &&stem)
        {
            static_assert(sizeof(StemT) == sizeof(ObjectStem), "Object stem cast requires identical stem size");
            static_assert(alignof(StemT) == alignof(ObjectStem), "Object stem cast requires identical stem alignment");
            static_assert(StemT::getRealmID() == REALM_ID, "Object stem cast requires a common object realm");
            return StemT(std::move(reinterpret_cast<StemT &>(stem)));
        }
        
    protected:
        friend super_t;
    };
    
}
