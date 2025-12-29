// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Foundation.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <cassert>

namespace db0 

{

    Foundation::Arranger& Foundation::Arranger::operator = (Arranger&& ) = default;
    Foundation::Arranger::~Arranger(){}
    
    Foundation::Arranger::Arranger(Arranger&& rhs) noexcept {
        *this = std::move(rhs);
    }

    Foundation::Arranger::Arranger(
        const std::byte *_from, std::byte *_at) noexcept
        : from(_from)
        , at(_at)
    {
    }
    
    std::size_t Foundation::Arranger::getSizeToHere () const {
        return at - from;
    }

    Foundation::Arranger Foundation::Arranger::operator [](std::uint16_t newImplVersion) {
        (void)newImplVersion;
        return std::move(*this);
    }

    Foundation::Arranger::operator std::size_t () const {
        return getSizeToHere();
    }

    void Foundation::SafeSizeBase::assertPrecondition(std::uint16_t _implVer){
        if(_implVer >= objVer)
            return ;
        THROWF(db0::InternalException) 
            << "critical internal error - object version invalid!!! " 
            << "Precondition mode, impl version: " << _implVer << "; obj version: " << objVer;
    }

    Foundation::SafeSizeBase::~SafeSizeBase() = default;
    Foundation::SafeSizeBase::SafeSizeBase(SafeSizeBase&& rhs) noexcept {
        *this = std::move(rhs);
    }

    Foundation::SafeSizeBase::SafeSizeBase(
        size_t _sizeSoFar
        , std::uint16_t _currImplVer
        , std::uint16_t _objVer
        , std::uint16_t _implVer
    ) noexcept (false)
        : sizeSoFar(_sizeSoFar)
        , currImplVer(_currImplVer)
        , objVer(_objVer)
    {
        assertPrecondition(_implVer);
    }

    void Foundation::SafeSizeBase::handleNewImplVersion(std::uint16_t newImplVersion) {
        currImplVer = newImplVersion;
    }

    Foundation::SafeSizeBase::operator std::size_t() const {
        return sizeSoFar;
    }

    Foundation::Meter& Foundation::Meter::operator = (Meter&& ) = default;
    Foundation::Meter::~Meter(){}

    Foundation::Meter::Meter(Meter&& rhs) noexcept {
        *this = std::move(rhs);
    }

    Foundation::Meter::Meter(std::size_t sizeSoFar) noexcept
        : sizeSoFar(sizeSoFar)
    {
    }
    
    Foundation::Meter Foundation::Meter::operator[](std::uint16_t newImplVersion) {
        (void)newImplVersion;
        return std::move(*this);
    }
        
    Foundation::Meter::operator std::size_t() const {
        return sizeSoFar;
    }

} 
