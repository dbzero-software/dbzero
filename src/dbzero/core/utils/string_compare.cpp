#include "string_compare.hpp"

namespace db0

{

    bool iequals(const std::string &a, const std::string &b)
    {
        return std::equal(a.begin(), a.end(), b.begin(), b.end(), [](char a, char b) {
            return tolower(a) == tolower(b);
        });
    }

}
