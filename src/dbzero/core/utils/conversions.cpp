#include "conversions.hpp"

namespace db0

{

    std::optional<std::string> getOptionalString(const char *str)
    {
        if (!str) {
            return std::nullopt;
        }
        return std::string(str);
    }

}