// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ValueTable.hpp"
#include <limits>
#include <cassert>

namespace db0::object_model

{

    PosVT::Data::Data(std::size_t size)
        : m_types(size)
        , m_values(size)
    {
    }
    
    PosVT::PosVT(const Data &data, unsigned int offset)
    {
        assert(data.m_types.size() == data.m_values.size());
        auto at = arrangeMembers()
            (TypesArrayT::type(), data.m_types, offset).ptr();

        // cannot use arranger to instantiate o_unbound_array because it lacks sizeOf() method
        o_unbound_array<Value>::__new(at, data.m_values);
    }

    o_unbound_array<Value> &PosVT::values() {
        return getDynAfter(types(), o_unbound_array<Value>::type());
    }

    const o_unbound_array<Value> &PosVT::values() const {
        return getDynAfter(types(), o_unbound_array<Value>::type());
    }

    std::size_t PosVT::size() const {
        return types().size();
    }
    
    std::size_t PosVT::measure(const Data &data, unsigned int offset)
    {
        assert(data.m_types.size() == data.m_values.size());
        return measureMembers()
            (TypesArrayT::measure(data.m_types, offset))
            (o_unbound_array<Value>::measure(data.m_values));        
    }
    
    bool PosVT::find(unsigned int index, unsigned int &pos) const {
        return types().find(index, pos);
    }
    
    bool PosVT::find(unsigned int index, std::pair<StorageClass, Value> &result) const
    {
        unsigned int pos;
        if (!find(index, pos)) {
            return false;
        }
        
        result.first = types()[pos];
        result.second = values()[pos];
        return true;
    }
    
    void PosVT::set(unsigned int pos, StorageClass type, Value value)
    {
        assert(pos < this->size());
        types()[pos] = type;
        values()[pos] = value;
    }
    
    bool PosVT::operator==(const PosVT &other) const
    {
        // bitwise comparison
        if (this->sizeOf() != other.sizeOf()) {
            return false;
        }
        return std::memcmp(this, &other, this->sizeOf()) == 0;
    }

    void PosVT::Data::clear()
    {
        m_types.clear();
        m_values.clear();
    }
    
    bool PosVT::Data::empty() const {
        return m_types.empty();
    }
    
    std::size_t PosVT::Data::size() const {
        return m_types.size();
    }
    
    IndexVT::IndexVT(const XValue *begin, const XValue *end)
    {
        this->arrangeMembers()
            (o_micro_array<XValue>::type(), begin, end);
    }

    std::size_t IndexVT::measure(const XValue *begin, const XValue *end)
    {
        return measureMembers()
            (o_micro_array<XValue>::measure(begin, end));
    }

    bool IndexVT::find(unsigned int index, std::pair<StorageClass, Value> &result) const
    {
        // since xvalues array is sorted, find using bisect
        auto &xval = this->xvalues();
        auto it = std::lower_bound(xval.begin(), xval.end(), index);
        if (it == xval.end() || it->getIndex() != index) {
            // element not found
            return false;
        }

        result.first = it->m_type;
        result.second = it->m_value;
        return true;
    }

    bool IndexVT::find(unsigned int index, unsigned int &pos) const
    {
        // since xvalues array is sorted, find using bisect
        auto &xval = this->xvalues();
        auto it = std::lower_bound(xval.begin(), xval.end(), index);
        if (it == xval.end() || it->getIndex() != index) {
            // element not found
            return false;
        }

        pos = it - xval.begin();
        return true;
    }

    void IndexVT::set(unsigned int pos, StorageClass type, Value value)
    {
        auto &xval = this->xvalues()[pos];
        xval.m_type = type;
        xval.m_value = value;
    }
    
    bool IndexVT::operator==(const IndexVT &other) const
    {
        // bitwise comparison
        if (this->sizeOf() != other.sizeOf()) {
            return false;
        }
        return std::memcmp(this, &other, this->sizeOf()) == 0;
    }
    
}