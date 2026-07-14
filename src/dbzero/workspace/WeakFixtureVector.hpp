// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include "Fixture.hpp"

namespace db0

{
    
    // A data collection for Fixture weak references
    // optimized for storing only one element (the common case)
    struct WeakFixtureVector
    {
        db0::weak_swine_ptr<Fixture> m_fx_1;
        // only initialized if more than one fixture
        std::vector<db0::weak_swine_ptr<Fixture>> *m_fx_vec = nullptr;

        WeakFixtureVector() = default;
        WeakFixtureVector(db0::swine_ptr<Fixture> fixture);
        ~WeakFixtureVector();
        
        std::size_t size() const;

        void add(db0::swine_ptr<Fixture> fixture);

        db0::weak_swine_ptr<Fixture> operator[](std::size_t);
        const db0::weak_swine_ptr<Fixture> &operator[](std::size_t) const;
    };
    
}