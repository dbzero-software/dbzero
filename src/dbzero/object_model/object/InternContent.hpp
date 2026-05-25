// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>

#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/object_model/object/ObjectInitializer.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>

namespace db0
{
    class Fixture;
}

namespace db0::object_model
{
    std::uint64_t intern_hash(db0::swine_ptr<db0::Fixture> &fixture, const o_embedded_object &object);
    std::uint64_t intern_hash(
        db0::swine_ptr<db0::Fixture> &fixture, const ImmutableObjectInitializer &initializer
    );

    int intern_compare(
        db0::swine_ptr<db0::Fixture> &fixture, const o_embedded_object &lhs, const o_embedded_object &rhs
    );
    int intern_compare(
        db0::swine_ptr<db0::Fixture> &fixture, const ImmutableObjectInitializer &lhs,
        const o_embedded_object &rhs
    );
    int intern_compare(
        db0::swine_ptr<db0::Fixture> &fixture, const o_embedded_object &lhs,
        const ImmutableObjectInitializer &rhs
    );
    int intern_compare(
        db0::swine_ptr<db0::Fixture> &fixture, const ImmutableObjectInitializer &lhs,
        const ImmutableObjectInitializer &rhs
    );
}
