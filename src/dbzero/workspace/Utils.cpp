#include "Utils.hpp"

namespace db0

{

#ifndef NDEBUG

    std::uint64_t writeBytes(db0::swine_ptr<Fixture> fixture, const char *data, std::size_t len)
    {
        db0::v_object<o_binary> obj(*fixture, reinterpret_cast<const std::byte*>(data), len);
        return obj.getAddress();
    }

    void freeBytes(db0::swine_ptr<Fixture> fixture, std::uint64_t address)
    {
        db0::v_object<o_binary> obj(fixture->myPtr(address));
        obj.destroy();
    }

    std::string readBytes(db0::swine_ptr<Fixture> fixture, std::uint64_t address)
    {
        db0::v_object<o_binary> obj(fixture->myPtr(address));
        return std::string(reinterpret_cast<const char*>(obj->getBuffer()), obj->size());
    }

#endif

}