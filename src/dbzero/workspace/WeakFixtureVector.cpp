// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "WeakFixtureVector.hpp"

namespace db0

{

    WeakFixtureVector::WeakFixtureVector(db0::swine_ptr<Fixture> fixture)
        : m_fx_1(fixture)
    {        
    }
    
    WeakFixtureVector::~WeakFixtureVector() {
        if (m_fx_vec) {
            delete m_fx_vec;
        }
    }
    
    void WeakFixtureVector::add(db0::swine_ptr<Fixture> fixture)
    {
        if (!m_fx_1) {
            m_fx_1 = fixture;
            return;
        }
        if (m_fx_1 == fixture) {
            return;
        }
        if (!m_fx_vec) {
            m_fx_vec = new std::vector<db0::weak_swine_ptr<Fixture>>();
            m_fx_vec->emplace_back(m_fx_1);
            return;
        }
        for (auto &fx: *m_fx_vec) {
            if (fx == fixture) {
                return;
            }
        }
        m_fx_vec->emplace_back(fixture);
    }

    std::size_t WeakFixtureVector::size() const 
    {
        if (!m_fx_1) {
            return 0;
        }
        if (!m_fx_vec) {
            return 1;
        }
        return 1 + m_fx_vec->size();
    }

    db0::weak_swine_ptr<Fixture> WeakFixtureVector::operator[](std::size_t idx)
    {
        if (idx == 0) {
            return m_fx_1;
        }
        if (m_fx_vec) {
            return (*m_fx_vec)[idx - 1];
        }
        throw std::out_of_range("Index out of range");        
    }

    const db0::weak_swine_ptr<Fixture> &WeakFixtureVector::operator[](std::size_t idx) const
    {
        if (idx == 0) {
            return m_fx_1;
        }
        if (m_fx_vec) {
            return (*m_fx_vec)[idx - 1];
        }
        throw std::out_of_range("Index out of range");
    }
    
}