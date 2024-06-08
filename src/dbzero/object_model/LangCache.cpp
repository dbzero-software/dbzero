#include "LangCache.hpp"

namespace db0::object_model

{
    
    LangCache::ObjectSharedPtr LangCache::get(std::uint64_t address) const
    {
        auto it = m_cache.find(address);
        if (it == m_cache.end()) {
            return {};
        }

        return it->second.m_object;
    }
    
    LangCache::~LangCache() {
        clear();
    }
    
    void LangCache::add(std::uint64_t address, ObjectPtr obj, bool strong_ref)
    {
        m_cache.emplace(address, LangCacheItem { obj, strong_ref });
        if (strong_ref) {
            LangToolkit::incRef(obj);
        }
    }
    
    std::size_t LangCache::size() const {
        return m_cache.size();
    }

    void LangCache::erase(std::uint64_t address)
    {
        auto it = m_cache.find(address);
        if (it == m_cache.end()) {
            return;
        }
        if (it->second.m_strong_ref) {
            LangToolkit::decRef(it->second.m_object);
        }
        m_cache.erase(it);
    }

    void LangCache::clear()
    {
        for (auto &item : m_cache) {
            if (item.second.m_strong_ref) {
                LangToolkit::decRef(item.second.m_object);
            }
        }
        m_cache.clear();
    }

}