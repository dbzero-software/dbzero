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
        // FIXME: log
        std::cout << "scope = " << from_state << " - " << to_state << std::endl;
        auto dp_size = storage.getPageSize();    
        auto dp_shift = db0::getPageShift(dp_size);

        // The algorithm works as follows:
        // 1. collect mutated DPs within the provided scope
        // 2. construct FT_SpanIterator containing the mutated DPs as spans
        // 3. AND-join the span filter and the original query
        // 4. refine results (lazy filter) by binary comparison of pre-scope and post-scope objects to identify actual mutations
        
        std::unordered_set<std::uint64_t> mutated_dps;
        storage.fetchChangeLogs(from_state, to_state + 1, [&](StateNumType, const db0::o_change_log &change_log) {
            auto it = change_log.begin(), end = change_log.end();
            if (it != end) {
                // first element holds the state number and should be ignored
                ++it;                
            }
            
            for (;it != end; ++it) {
                mutated_dps.insert(*it);
            }        
        });
        
        std::vector<db0::UniqueAddress> unique_dps;
        for (auto dp_num: mutated_dps) {
            unique_dps.push_back(getDPBound(dp_num, dp_shift));
        }

        // FIXME: log        
        std::cout << "unique-dps size: " << unique_dps.size() << std::endl;
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
    