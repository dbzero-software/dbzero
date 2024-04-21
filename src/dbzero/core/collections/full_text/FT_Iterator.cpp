#include "FT_Iterator.hpp"

namespace db0

{
    
    template <typename key_t> std::uint64_t FT_Iterator<key_t>::getIndexKey() const {
        return std::uint64_t();
    }
    
    template <typename key_t> const std::type_info &FT_Iterator<key_t>::keyTypeId() const {
        return typeid(key_t);
    }
    
    template class db0::FT_Iterator<std::uint64_t>;    
    template class db0::FT_Iterator<int>;    

    template <typename key_t> void FT_Iterator<key_t>::fetchKeys(std::function<void(const key_t *key_buf, std::size_t key_count)> f,
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

    template <typename key_t> std::unique_ptr<FT_IteratorBase>
    FT_Iterator<key_t>::begin() const {
        return beginTyped(-1);
    }    
    
    template <typename key_t> void FT_Iterator<key_t>::serialize(std::vector<std::byte> &v) const {
        db0::serial::write<FTIteratorType>(v, this->getSerialTypeId());
        this->serializeFTIterator(v);
    }

} // dbz namespace {
