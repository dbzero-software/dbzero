#include "XValue.hpp"

namespace db0::object_model

{

    bool XValue::operator<(const XValue &other) const {
        return getIndex() < other.getIndex();
    }

    bool XValue::operator<(std::uint32_t index) const {
        return getIndex() < index;
    }
    
    bool XValue::operator==(std::uint32_t index) const {
        return getIndex() == index;
    }
    
    bool XValue::operator==(const XValue &other) const {
        return getIndex() == other.getIndex();
    }
    
    bool XValue::operator!=(const XValue &other) const {
        return getIndex() != other.getIndex();        
    }
    
    bool XValue::equalTo(const XValue &other) const {
        return std::memcmp(this, &other, sizeof(XValue)) == 0;
    }
    
}   