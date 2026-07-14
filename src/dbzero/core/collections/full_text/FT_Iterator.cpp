// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FT_Iterator.hpp"
#include <cassert>

namespace db0

{
        
    template <typename key_t, typename key_storage_t> 
    const std::type_info &FT_Iterator<key_t, key_storage_t>::keyTypeId() const {
        return typeid(key_t);
    }
    
    template <typename key_t, typename key_storage_t> 
    FT_Iterator<key_t, key_storage_t>::FT_Iterator(std::uint64_t uid)
        : FT_IteratorBase(uid) 
    {
    }
    
    template <typename key_t, typename key_storage_t>
    void FT_Iterator<key_t, key_storage_t>::getKey(key_storage_t &key) const
    {
        key = this->getKey();
    }
    
    template <typename key_t, typename key_storage_t>
    void FT_Iterator<key_t, key_storage_t>::fetchKeys(std::function<void(const key_t *key_buf, std::size_t key_count)> f,
        std::size_t batch_size) const
    {
        std::vector<key_t> buf(batch_size);
        auto it = beginTyped(1);
        std::size_t count = 0;
        while (!it->isEnd()) {
            // flush keys to the sink function
            if (count == batch_size) {
                f(&buf.front(), count);
                count = 0;
            }
            buf[count++] = it->getKey();
            ++(*it);
        }
        // flush the remaining keys
        if (count > 0) {
            f(&buf.front(), count);
        }
    }
    
    template <typename key_t, typename key_storage_t> 
    std::unique_ptr<FT_IteratorBase> FT_Iterator<key_t, key_storage_t>::begin() const {
        return beginTyped(-1);
    }    
    
    template <typename key_t, typename key_storage_t>
    void FT_Iterator<key_t, key_storage_t>::serialize(std::vector<std::byte> &v) const 
    {
        db0::serial::write<FTIteratorType>(v, this->getSerialTypeId());
        this->serializeFTIterator(v);
    }

    template <typename key_t, typename key_storage_t> 
    bool FT_Iterator<key_t, key_storage_t>::isSimple() const
    {
        auto type_id = this->getSerialTypeId();
        return type_id == FTIteratorType::Index || type_id == FTIteratorType::RangeTree;        
    }
    
    template <typename key_t, typename key_storage_t> 
    void FT_Iterator<key_t, key_storage_t>::scanQueryTree(
        std::function<void(const FT_Iterator<key_t, key_storage_t> *, int depth)> scan_function, int depth) const
    {
        assert(this->getDepth() == 1);
        scan_function(this, depth);
    }
    
    template <typename key_t, typename key_storage_t> 
    std::size_t FT_Iterator<key_t, key_storage_t>::getDepth() const {
        return 1u;
    }
    
    template <typename key_t, typename key_storage_t>
    bool FT_Iterator<key_t, key_storage_t>::swapKey(key_storage_t &key) const
    {
        key_storage_t current_key;
        this->getKey(current_key);
        if (current_key == key) {
            return false;
        }
        key = current_key;
        return true;
    }

    // Explicit template instantiations - must be after all method definitions
    template class db0::FT_Iterator<UniqueAddress>;
    template class db0::FT_Iterator<std::uint64_t>;
    template class db0::FT_Iterator<int>;
    template class db0::FT_Iterator<const UniqueAddress*, CP_Vector<UniqueAddress>>;
    template class db0::FT_Iterator<const std::uint64_t*, CP_Vector<std::uint64_t>>;

}
