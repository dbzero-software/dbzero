// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

    /**
     * Stores exactly N elements (fixed size) of type item_t in sorted ascending order
     */
DB0_PACKED_BEGIN
    template <typename item_t, int N, typename item_comp_t = std::less<item_t> >
    class DB0_PACKED_ATTR o_sorted_sequence
        : public o_fixed<o_sorted_sequence<item_t, N, item_comp_t> >
    {
    protected :
        item_t data[N];

    public :
        using const_iterator = const item_t *;

        o_sorted_sequence(const item_t *in)
        {
            std::memcpy(data, in, sizeof(data));
            std::sort(data, data + N, item_comp_t());
        }

        const item_t *getData() const {
            return data;
        }

        const_iterator begin() const {
            return data;
        }

        const_iterator end() const {
            return data + N;
        }

        const_iterator find(const item_t &item) const
        {
            // find using bisect
            auto it = std::lower_bound(begin(), end(), item, item_comp_t());
            if (it!=end() && !item_comp_t()(*it, item) && !item_comp_t()(item, *it)) {
                return it;
            }

            return end();
        }

        // get element number pointed by the iterator
        std::uint32_t getIndex(const_iterator it) const {
            return it - begin();
        }

        void setAt(unsigned int index, const item_t &item) {
            data[index] = item;
        }
    };
DB0_PACKED_END
    
    template <typename item_t, int N, typename AddrT = Address, typename item_comp_t = std::less<item_t> >
    class v_sorted_sequence
        : public db0::v_object<o_sorted_sequence<item_t, N, item_comp_t> >
    {
        using inner_t = o_sorted_sequence<item_t, N, item_comp_t>;
        using super_t = db0::v_object<inner_t>;
    public :
        using addr_t = AddrT;
        using joinable_const_iterator = db0::joinable_const_iterator<item_t, item_comp_t>;

        /**
         * Construct with exactly N elements
         * @param data unsorted array of values to store
         */
        v_sorted_sequence(Memspace &memspace, const item_t *data)
            : super_t(memspace, data)
        {
        }
        
        /**
         * Construct with exactly N elements
         * @param data / end unsorted array of values to store
         */
        v_sorted_sequence(Memspace &memspace, const item_t *data, const item_t *end)
            : super_t(memspace, data)
        {
            assert((end - data)==N);
        }
        
        v_sorted_sequence(mptr ptr)
            : super_t(ptr)
        {
        }
        
        v_sorted_sequence(std::pair<Memspace*, AddrT> addr)
            : v_sorted_sequence(addr.first->myPtr(addr.second))
        {            
        }

        class const_iterator
        {
            const item_t *it;
        public :
            const_iterator(const item_t &item)
                : it(&item)
            {
            }

            const item_t &operator*() const {
                return *it;
            }

            const_iterator &operator++() {
                ++it;
                return *this;
            }

            bool operator!=(const const_iterator &other) const {
                return it!=other.it;
            }

            bool operator==(const const_iterator &other) const {
                return it==other.it;
            }
        };
        
        joinable_const_iterator beginJoin(int direction) const
        {
            if (direction > 0) {
                return joinable_const_iterator((*this)->begin(), (*this)->end(), (*this)->begin(), direction);
            } else {
                return joinable_const_iterator((*this)->begin(), (*this)->end(), (*this)->begin() + N - 1, direction);
            }
        }

        const_iterator begin() const {
            return *(*this)->begin();
        }

        const_iterator end() const {
            return *(*this)->end();
        }

        const_iterator find(const item_t &item) const {
            return *(*this)->find(item);
        }

        /**
         * @return BN storage size used by this data structure
         */
        static std::uint64_t getStorageSize() {
            return inner_t::sizeOf();
        }
        
        bool updateExisting(const item_t &value, item_t *old_value = nullptr)
        {
            auto it = (*this)->find(value);
            if (it == (*this)->end()) {
                return false;
            }

            if (old_value) {
                *old_value = *it;
            }        
            auto index = (*this)->getIndex(it);
            this->modify().setAt(index, value);
            return true;            
        }

        bool findOne(item_t &value) const
        {
            auto it = (*this)->find(value);
            if (it == (*this)->end()) {
                return false;
            }

            value = *it;        
            return true;
        }
    
    };

}
