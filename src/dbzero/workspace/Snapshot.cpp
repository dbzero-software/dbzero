#include "Snapshot.hpp"
#include "PrefixName.hpp"
#include "Fixture.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    db0::swine_ptr<Fixture> Snapshot::findFixture(const PrefixName &prefix_name) const
    {
        auto result = tryFindFixture(prefix_name);
        if (!result) {
            THROWF(db0::InputException) << "Prefix with name " << prefix_name << " not found";
        }
        return result;
    }

}