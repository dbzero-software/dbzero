#include "FieldID.hpp"

namespace std

{

    std::ostream &operator<<(std::ostream &os, const db0::object_model::FieldID &field_id)
    {
        if (field_id) {
            os << field_id.getIndex() << "/" << field_id.getOffset();
        } else {
            os << "null";
        }
        return os;
    }

}