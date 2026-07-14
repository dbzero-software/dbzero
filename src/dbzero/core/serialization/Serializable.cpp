// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Serializable.hpp"
#include "hash.hpp"
#include <dbzero/core/utils/base32.hpp>

namespace db0::serial

{
    
    void Serializable::getUUID(char *uuid_buf) const
    {
        std::vector<std::byte> bytes;
        serialize(bytes);
        std::byte hash[32];
        sha256(reinterpret_cast<const std::uint8_t *>(bytes.data()), bytes.size(), hash);
        auto size = db0::base32_encode(reinterpret_cast<std::uint8_t *>(hash), sizeof(hash), uuid_buf);
        uuid_buf[size] = 0;
    }
    
    void getSignature(const Serializable &serializable, std::vector<std::byte> &v)
    {
        std::vector<std::byte> bytes;
        serializable.serialize(bytes);
        // calculate hash from bytes as a signature
        db0::serial::sha256(bytes, v);
    }

}