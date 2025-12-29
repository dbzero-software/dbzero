// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_map>
#include <vector>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/core/collections/full_text/TagProduct.hpp>

namespace tests

{
    
    using namespace db0;
    using FT_IteratorT = FT_Iterator<std::uint64_t>;
    using FactoryFunc = std::function<std::unique_ptr<FT_IteratorT>(std::uint64_t, int)>;
    
    // Tag-product test data
    struct TP_Data  
    {
        std::unordered_map<std::uint64_t, std::vector<std::uint64_t> > m_index_1 {
            { 0, { 0, 1, 2, 3 }},
            { 1, { 3, 4, 5 }}
        };
        std::unordered_map<std::uint64_t, std::vector<std::uint64_t> > m_index_2 {
            { 9, { 0, 1, 2, 3 }},
            { 6, { 3, 4, 5 }}
        };
        std::unordered_map<std::uint64_t, std::vector<std::uint64_t> > m_index_3 {
            { 0, { 0, 1, 2, 3, 101, 102, 103 }},
            { 1, { 3, 4, 5, 103, 104, 105 } }
        };
    };
    
    FactoryFunc makeFactory(const std::unordered_map<std::uint64_t, std::vector<uint64_t> > &index);

    // NOTE: index captures the relationships between objects & tags
    TagProduct<std::uint64_t> makeTagProduct(const std::vector<std::uint64_t> &objects,
        const std::vector<std::uint64_t> &tags, const std::unordered_map<std::uint64_t, std::vector<uint64_t> > &index);        
    TagProduct<std::uint64_t> makeTagProduct(const std::vector<std::vector<std::uint64_t> > &objects,
        const std::vector<std::uint64_t> &tags, const std::unordered_map<std::uint64_t, std::vector<uint64_t> > &index);    
    
}