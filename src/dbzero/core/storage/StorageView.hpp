#pragma once

#include "BaseStorage.hpp"

namespace db0

{

    /**
     * A simple interface providing storage reference at a particual state number
    */
    class StorageView
    {
    public:
        StorageView(BaseStorage &storage, const std::uint64_t &state_num_ref)
            : m_storage(storage)
            , m_state_num_ref(state_num_ref)
        {
        }
        
        std::pair<std::reference_wrapper<BaseStorage>, std::uint64_t> get() const {
            return {m_storage, m_state_num_ref};
        }
        
        bool operator==(const StorageView &other) const {
            // compare pointers
            return &(m_storage.get()) == &(other.m_storage.get());
        }

    private:
        std::reference_wrapper<BaseStorage> m_storage;
        const std::uint64_t &m_state_num_ref;
    };
    
}