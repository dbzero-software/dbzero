#include "ByteArray.hpp"

namespace db0::object_model {
    GC0_Define(ByteArray)

    ByteArray *ByteArray::makeNew(void *at_ptr, db0::swine_ptr<Fixture> & fixture, std::byte * bytes, std::size_t size)
    {
        return new (at_ptr) ByteArray(fixture, bytes, size);
    }

    ByteArray::ByteArray(db0::swine_ptr<Fixture> &fixture, std::byte * bytes, std::size_t size)
        : super_t(fixture)
    {
        for(std::size_t i =0; i < size; ++i, ++bytes){
            v_bvector::push_back(o_byte(*bytes));
        }
    }

    std::byte ByteArray::getItem(std::size_t i) const {
        if (i >= size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        o_byte byte= (*this)[i];
        return byte.m_byte;
    }

}