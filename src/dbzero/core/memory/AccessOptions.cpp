#include <dbzero/core/memory/AccessOptions.hpp>

DEFINE_ENUM_VALUES(db0::AccessOptions, "rely", "read", "write", "create", "no_cache")

namespace db0

{

    AccessType parseAccessType(const std::string &access_type)
    {
        AccessType result = AccessType::READ_ONLY;
        for (char c: access_type) {
            switch (c) {
                case 'w':
                    result = AccessType::READ_WRITE;
                case 'r':
                    break;
                default:
                    THROWF(InternalException) << "Invalid access type: " << access_type;                    
            }
        }
        return result;
    }

}