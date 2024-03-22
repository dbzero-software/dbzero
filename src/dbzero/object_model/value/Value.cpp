#include "Value.hpp"

namespace db0::object_model 

{
    
    bool Value::operator==(const Value &other) const
    {
        return m_store == other.m_store;
    }
       
}