#include "QueryHash.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

using namespace std;

namespace db0

{

    void QueryHash::operator+=(const QueryHash &other) 
    {
        db0::hash_combine(m_type_hash, other.m_type_hash);
        db0::hash_combine(m_hash, other.m_hash);
        m_query_size += other.m_query_size;
    }

    QueryHash &QueryHash::combine(const QueryHash &other, unsigned int at) 
    {
        db0::hash_combine(m_type_hash, other.m_type_hash);
        // include position to modify hash
        db0::hash_combine(m_hash, other.m_hash + at);
        m_query_size += other.m_query_size;
        return *this;
    }

    bool QueryHash::operator==(const QueryHash &other) const {
        return m_type_hash == other.m_type_hash && m_hash == other.m_hash && m_query_size == other.m_query_size;
    }

    bool QueryHash::operator!=(const QueryHash &other) const {
        return m_type_hash != other.m_type_hash || m_hash != other.m_hash || m_query_size != other.m_query_size;
    }

    bool QueryHash::operator<(const QueryHash &other) const
    {
        if (m_type_hash != other.m_type_hash) {
            return m_type_hash < other.m_type_hash;
        }
        if (m_hash != other.m_hash) {
            return m_hash < other.m_hash;
        }
        return m_query_size < other.m_query_size;
    }

    QueryHash getQueryHash(std::vector<QueryHash> &all_hash)
    {
        if (all_hash.empty()) {
            THROWF(db0::InternalException) << "Unexepcted empty hash set";
        }
        // sort (for order invariance)
        std::sort(all_hash.begin(), all_hash.end());
        // combine all ignoring duplicates
        auto it = all_hash.begin(), end = all_hash.end();
        auto result = *it;
        auto it_next = it;
        for (++it_next;it_next != end;++it,++it_next) {
            if (*it != *it_next) {
                result += *it_next;
            }
        }
        return result;
    }
    
    QueryHash::operator bool() const
    {
        return m_hash || m_type_hash || m_query_size;
    }

}
