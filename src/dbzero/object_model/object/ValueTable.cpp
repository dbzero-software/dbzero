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

    PosVT::PosVT(const Data &data)
    {
        assert(data.m_types.size() == data.m_values.size());
        auto at = arrangeMembers()
            (o_micro_array<StorageClass>::type(), data.m_types).ptr();

        // cannot use arranger to instantiate o_unbound_array because it lacks sizeOf() method
        o_unbound_array<Value>::__new(at, data.m_values);
    }
    
    o_unbound_array<Value> &PosVT::values()
    {
        return getDynAfter(types(), o_unbound_array<Value>::type());
    }

    const o_unbound_array<Value> &PosVT::values() const
    {
        return getDynAfter(types(), o_unbound_array<Value>::type());
    }
    
    std::size_t PosVT::sizeOf() const
    {
        return sizeOfMembers()
            (o_micro_array<StorageClass>::type())
            (o_unbound_array<Value>::measure(this->size()));
    }

    std::size_t PosVT::size() const
    {
        return types().size();
    }
    
    std::size_t PosVT::measure(const Data &data)
    {
        assert(data.m_types.size() == data.m_values.size());
        return measureMembers()
            (o_micro_array<StorageClass>::measure(data.m_types))
            (o_unbound_array<Value>::measure(data.m_values));        
    }
    
    bool PosVT::find(unsigned int index, std::pair<StorageClass, Value> &result) const
    {
        if (index >= this->size()) {
            // index not in the range
            return false;
        }
        
        result.first = types()[index];
        result.second = values()[index];
        return true;
    }

    void PosVT::set(unsigned int index, StorageClass type, Value value)
    {
        assert(index < this->size());
        types()[index] = type;
        values()[index] = value;
    }
    
    void PosVT::Data::clear()
    {
        m_types.clear();
        m_values.clear();
    }
    
    bool PosVT::Data::empty() const
    {
        return m_types.empty();
    }
    
    std::size_t PosVT::Data::size() const
    {
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
    
}