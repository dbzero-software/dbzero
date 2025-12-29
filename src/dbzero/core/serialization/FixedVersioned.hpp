// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/serialization/Fixed.hpp>

namespace db0
{

DB0_PACKED_BEGIN

template<typename T, std::uint16_t VER=0>
class DB0_PACKED_ATTR o_fixed_versioned : private version_base<VER, true>, public o_fixed<T> {
    typedef version_base<VER, true> ver_type;

public:
    using o_fixed<T>::o_fixed;

    static constexpr bool getIsVerStored() {
        return ver_type::isVerStored();
    }

    static constexpr std::uint16_t getImplVer() {
        return ver_type::implVer();
    }

    template <class buf_t> static std::uint16_t getObjVer(buf_t at) {
        return ver_type::objVer(at);
    }

    std::uint16_t getObjVer() const {
        return ver_type::objVer();
    }
};

DB0_PACKED_END

}
