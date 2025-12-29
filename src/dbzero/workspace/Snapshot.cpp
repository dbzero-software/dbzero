// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Snapshot.hpp"
#include "PrefixName.hpp"
#include "Fixture.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    bool checkAccessType(const Fixture &fixture, AccessType requested) {
        return (requested != AccessType::READ_WRITE || fixture.getAccessType() == AccessType::READ_WRITE);
    }

    bool checkAccessType(const Fixture &fixture, std::optional<AccessType> requested) 
    {
        if (!requested) {
            return true;
        }
        return checkAccessType(fixture, *requested);
    }
    
    void assureAccessType(const Fixture &fixture, std::optional<AccessType> requested)
    {
        if (!checkAccessType(fixture, requested)) {
            THROWF(db0::AccessTypeException) << "Unable to update the read-only prefix: " << fixture.getPrefix().getName();
        }
    }
    
    db0::swine_ptr<Fixture> Snapshot::findFixture(const PrefixName &prefix_name) const
    {
        auto result = tryFindFixture(prefix_name);
        if (!result) {
            THROWF(db0::InputException) << "Prefix with name " << prefix_name << " not found";
        }
        return result;
    } 

    db0::swine_ptr<Fixture> Snapshot::getFixture(
        const PrefixName &prefix_name, std::optional<AccessType> access_type)
    {
        auto fixture = tryGetFixture(prefix_name, access_type);
        if (!fixture) {
            THROWF(db0::InputException) << "Prefix: " << prefix_name << " not found";
        }
        return fixture;
    }
    
    db0::swine_ptr<Fixture> Snapshot::getFixture(std::uint64_t uuid, std::optional<AccessType> access_type)
    {
        auto fixture = tryGetFixture(uuid, access_type);
        if (!fixture) {
            THROWF(db0::InputException) << "Prefix: " << uuid << " not found";
        }
        return fixture;
    }
    
    Snapshot &Snapshot::getHeadWorkspace() const {
        return const_cast<Snapshot &>(*this);
    }

    std::optional<AccessType> Snapshot::tryGetAccessType() const {
        return std::nullopt;
    }
    
}