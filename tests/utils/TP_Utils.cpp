// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "TP_Utils.hpp"
#include <dbzero/core/collections/full_text/FT_FixedKeyIterator.hpp>

namespace tests

{
    
    FactoryFunc makeFactory(const std::unordered_map<std::uint64_t, std::vector<uint64_t> > &index)
    {
        return [index](std::uint64_t key, int direction) {
            auto it = index.find(key);
            if (it == index.end()) {
                return std::unique_ptr<FT_IteratorT>(nullptr);
            }
            return std::unique_ptr<FT_IteratorT>(new FT_FixedKeyIterator<std::uint64_t>(
                it->second.data(), it->second.data() + it->second.size(), direction
            ));
        };
    }

    // NOTE: index captures thre relationships between objects & tags
    TagProduct<std::uint64_t> makeTagProduct(const std::vector<std::uint64_t> &objects,
        const std::vector<std::uint64_t> &tags, const std::unordered_map<std::uint64_t, std::vector<uint64_t> > &index)
    {        
        std::vector<std::unique_ptr<FT_IteratorT>> sources;
        sources.emplace_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(objects.data(), objects.data() + objects.size()));
        return TagProduct<std::uint64_t>(
            std::move(sources),
            std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(tags.data(), tags.data() + tags.size()),
            makeFactory(index)
        );
    }
    
    TagProduct<std::uint64_t> makeTagProduct(const std::vector<std::vector<std::uint64_t> > &objects,
        const std::vector<std::uint64_t> &tags, const std::unordered_map<std::uint64_t, std::vector<uint64_t> > &index)
    {
        std::vector<std::unique_ptr<FT_IteratorT>> sources;
        for (const auto &obj_list: objects) {
            sources.emplace_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
                obj_list.data(), obj_list.data() + obj_list.size())
            );
        }
        return TagProduct<std::uint64_t>(
            std::move(sources),
            std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(tags.data(), tags.data() + tags.size()),
            makeFactory(index)
        );
    }
    
}
