// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SelectModified.hpp"
#include <unordered_set>
#include <dbzero/core/collections/full_text/FT_SpanIterator.hpp>
#include <dbzero/core/collections/full_text/FT_FixedKeyIterator.hpp>
#include <dbzero/core/storage/BaseStorage.hpp>
#include <dbzero/core/storage/ChangeLog.hpp>
#include <dbzero/core/memory/utils.hpp> 
#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{
    
    // get the boundary address of the DP-related span
    db0::UniqueAddress getDPBound(std::uint64_t dp_num, unsigned int dp_shift) 
    {
        auto offset = ((dp_num + 1) << dp_shift) - 1;
        return { Address::fromOffset(offset), db0::UniqueAddress::INSTANCE_ID_MAX };
    }
    
    std::unique_ptr<QueryIterator> selectModCandidates(std::unique_ptr<QueryIterator> &&query, const db0::BaseStorage &storage,
        StateNumType from_state, StateNumType to_state)
    {
        using DP_ChangeLogT = db0::BaseStorage::DP_ChangeLogT;
        auto dp_size = storage.getPageSize();
        auto dp_shift = db0::getPageShift(dp_size);

        // The algorithm works as follows:
        // 1. collect mutated DPs within the provided scope
        // 2. construct FT_SpanIterator containing the mutated DPs as spans
        // 3. AND-join the span filter and the original query
        // 4. refine results (lazy filter) by binary comparison of pre-scope and post-scope objects to identify actual mutations
        
        std::unordered_set<std::uint64_t> mutated_dps;
        storage.fetchDP_ChangeLogs(from_state, to_state + 1, [&](const DP_ChangeLogT &change_log) {
            for (auto page_num: change_log) {
                mutated_dps.insert(page_num);
            }
        });
        
        std::vector<db0::UniqueAddress> unique_dps;
        for (auto dp_num: mutated_dps) {
            unique_dps.push_back(getDPBound(dp_num, dp_shift));
        }
        
        auto dp_iter = std::make_unique<FT_FixedKeyIterator<db0::UniqueAddress> >(
            unique_dps.data(), unique_dps.data() + unique_dps.size()
        );
        auto span_iter = std::make_unique<FT_SpanIterator<db0::UniqueAddress> >(std::move(dp_iter), dp_shift);
        
        db0::FT_ANDIteratorFactory<db0::UniqueAddress> factory;
        factory.add(std::move(span_iter));
        factory.add(std::move(query));
        return factory.release();
    }
    
} 
    