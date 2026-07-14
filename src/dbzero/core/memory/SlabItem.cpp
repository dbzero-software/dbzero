// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SlabItem.hpp"

namespace db0

{
    
    SlabItem::SlabItem(std::shared_ptr<SlabAllocator> slab, CapacityItem cap)
        : m_slab(slab)
        , m_cap_item(cap)
    {
    }
    
    SlabItem::~SlabItem() {
        assert(!m_is_dirty && "SlabItem destroyed while still dirty");
    }
    
    void SlabItem::commit() const
    {                        
        assert(m_slab);
        m_slab->commit();                
    }

    void SlabItem::detach() const
    {
        assert(m_slab);                     
        m_slab->detach();        
    }

}

namespace std 

{
    ostream &operator<<(ostream &os, const db0::CapacityItem &item) {
        os << "CapacityItem(capacity=" << item.m_remaining_capacity << ", slab=" << item.m_slab_id << ")";
        return os;
    }

    ostream &operator<<(ostream &os, const db0::SlabDef &def) {
        os << "SlabDef(slab=" << def.m_slab_id << ", capacity=" << def.m_remaining_capacity << ")";
        return os;
    }

}