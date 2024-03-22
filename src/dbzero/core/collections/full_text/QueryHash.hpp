#pragma once

#include <dbzero/core/utils/hash_combine.hpp>
#include <vector>
#include "FT_IteratorBase.hpp"

namespace db0

{

    /**
     * Structure to identify unique full-text/index based query
     */
    struct QueryHash
    {
        // combined hash from all underlying iterator types
        std::uint64_t m_type_hash = 0;
        // hash from all nested nodes
        std::uint64_t m_hash = 0;
        // size of the query (number of nested nodes)
        unsigned int m_query_size = 0;

        /**
         * Create as null/invalid
         */
        QueryHash() = default;

        /**
         * Combine with other hash
         */
        void operator+=(const QueryHash &);

        /**
         * Positional combine with other hash (when position of node is relevant)
         * @param at
         */
        QueryHash &combine(const QueryHash &, unsigned int at);

        bool operator==(const QueryHash &) const;

        bool operator!=(const QueryHash &) const;

        bool operator<(const QueryHash &) const;

        /**
         * Check if the hash object is valid
         * @return
         */
        operator bool() const;
    };

    /// Create hash as combined from multiple underlying (order and duplication invariant)
    QueryHash getQueryHash(std::vector<QueryHash> &);
    
} 

namespace std

{

    template <> struct hash<db0::QueryHash>
    {
        std::size_t operator()(const db0::QueryHash &query_hash) const noexcept {
            std::size_t hash_id = 0xcf9271;
            db0::hash_combine(hash_id, query_hash.m_type_hash);
            db0::hash_combine(hash_id, query_hash.m_hash);
            db0::hash_combine(hash_id, query_hash.m_query_size);
            return hash_id;
        }
    };
    
}
