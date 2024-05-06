#include "Serializable.hpp"
#include "hash.hpp"
#include <dbzero/core/utils/base32.hpp>

namespace db0::serial

{

    void Serializable::getUUID(char *uuid_buf) const
    {
        std::vector<std::byte> bytes;
        serialize(bytes);
        std::uint32_t hash[8];
        sha256(reinterpret_cast<const std::uint8_t *>(bytes.data()), bytes.size(), hash);
        auto size = db0::base32_encode(reinterpret_cast<std::uint8_t *>(hash), sizeof(hash), uuid_buf);
        uuid_buf[size] = 0;
    }

}