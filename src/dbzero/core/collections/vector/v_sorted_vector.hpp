// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <list>
#include <cstring>
#include <algorithm>
#include <dbzero/core/collections/vector/joinable_const_iterator.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/utils/heap.hpp>
#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/collections/b_index/type.hpp>
#include <dbzero/core/serialization/Serializable.hpp>

namespace db0

{

    /**
     * inverting comparator
    */
    template <class T,class comp_t> struct inverted_comp_t 
    {
        comp_t _comp;

        inverted_comp_t() = default;

        inverted_comp_t(comp_t _comp)
            : _comp(_comp)
        {
        }

        bool operator()(const T &val0,const T &val1) const {
            return _comp(val1,val0);
        }
    };  

    /**
     * Sorted vector state types
     * only transition from sv_growing to sv_shrinking is allowed
     */
    enum class sv_state: std::uint32_t {        
        growing = 0x00 ,
        shrinking = 0x01
    };
    
    /**
     * Sorted vector container type
     * data_t - contained element type (comparable)
     * comp_t - data comparer
     */
DB0_PACKED_BEGIN    
    template <class data_t, class comp_t = std::less<data_t> > class DB0_PACKED_ATTR o_sv_container
        : public o_base<o_sv_container<data_t,comp_t>, 0, false >
    {
        using self = o_sv_container<data_t, comp_t>;
        using super_t = o_base<o_sv_container<data_t,comp_t>, 0, false>;
        friend super_t;

        o_sv_container(const o_sv_container &other);

        o_sv_container(std::uint32_t capacity, sv_state state)
            : m_capacity(capacity)
            , m_size(0)
            , m_state(state)
        {
            assert(m_capacity > 0);
        }
        
    public :
        using iterator = data_t*;
        using const_iterator = const data_t*;
        using joinable_const_iterator = db0::joinable_const_iterator<data_t, comp_t>;
        using DestroyF = std::function<void(const data_t&)>;
        using CallbackT = std::function<void(data_t)>;

        static std::size_t measure(const o_sv_container &other) {
            return other.sizeOf();
        }
        
        static std::size_t measure(std::uint32_t max_size, sv_state) {
            return sizeof(self) + (max_size * sizeof(data_t));
        }

        template <class buf_t> static std::size_t safeSizeOf(buf_t at) {
            return sizeof(self) + self::__const_ref(at).m_capacity * sizeof(data_t);
        }

        bool empty() const {
            return (m_size == 0);
        }

        bool is_full() const {
            return (m_size == m_capacity);
        }

        /**
         * pop "count" items from the beginning of the vector
         */
        void pop_front(int count, DestroyF item_destroy_func = {}) 
        {
            if (count > 0) {
                assert (count <= this->m_size);
                iterator it_dest = begin();
                const_iterator it_src = it_dest;
                const_iterator it_end = end();
                this->m_size -= count;
                if (item_destroy_func) {
                    while (count-- > 0) {
                        item_destroy_func(*it_src);
                        ++it_src;
                    }
                } else {
                    while (count-- > 0) {
                        ++it_src;
                    }
                }
                while (it_src!=it_end) {
                    *it_dest = *it_src;
                    ++it_dest;
                    ++it_src;
                }
            }
        }

        /**
         * pop "count" items from end of the vector
         */
        void pop_back(int count, DestroyF item_destroy_func = {}) 
        {
            if (count > 0) {
                assert (count <= this->m_size);
                const_iterator it = end();
                --it;
                this->m_size -= count;
                if (item_destroy_func) {
                    while (count-- > 0) {
                        item_destroy_func(*it);
                        --it;
                    }
                } else {
                    while (count-- > 0) {
                        --it;
                    }
                }
            }
        }

        /**
         * Append sorted items from other vector, no item destroyed
         * NOTICE : sort order must be preserved
         */
        void appendSorted(const_iterator it_begin, const_iterator it_end)
        {
            assert(this->m_size + std::distance(it_begin, it_end) <= m_capacity);
            std::memcpy(this->end(), it_begin, std::distance(it_begin, it_end) * sizeof(data_t));
            this->m_size += std::distance(it_begin, it_end);
        }

        void popBack(std::size_t count)
        {
            assert (count <= this->m_size);
            this->m_size -= count;            
        }

        template <class ItemIterator> void bulkPushBack(ItemIterator it, std::size_t count)
        {
            assert ((m_size + count) <= m_capacity);
            data_t *it_dest = end();
            this->m_size += count;
            while (count-- > 0) {
                *it_dest = *it;
                ++it_dest;
                ++it;
            }
        }

        /**
         * Ascending only items order allowed (throws)
         */
        void push_back(const data_t &data) 
        {
            assert (!is_full());
            // assert ascending order is preserved
            assert (empty() || comp_t()(getData()[m_size - 1],data));
            getData()[m_size++] = data;
        }

        /**
         * Bulk insert collection of reverse sorted items
         */
        template <class ItemIterator> void bulkInsertReverseSorted(ItemIterator it_begin, std::size_t count) 
        {
            SortedArray<data_t,comp_t> data_buf(begin(), end());
            data_t *it_src = (data_t*)data_buf.m_end - 1;
            data_t *it_dest = it_src + count;
            assert(this->m_size + count <= m_capacity);
            this->m_size += count;
            // iterate data backwards
            while (count > 0) {
                // insertion point
                data_buf.m_end = (it_src + 1);
                data_t *item = const_cast<data_t*>(data_buf.join(data_buf.m_begin, *it_begin, 1) + count - 1);
                while (it_dest != item) {
                    *it_dest = *it_src;
                    --it_src;
                    --it_dest;                
                }
                assert(it_dest < this->begin() + this->m_capacity);
                *it_dest = *it_begin;
                --it_dest;
                ++it_begin;
                --count;
            }
        }

        /**
         * bulk insert data, preserve ascending order
         */
        void bulkInsert(const std::vector<data_t> &data) 
        {
            assert (m_size + (int)data.size() <= m_capacity);
            // reverse-sort heap
            heap<data_t,inverted_comp_t<data_t,comp_t> > s_heap((int)data.size());
            {
                typename std::vector<data_t>::const_iterator it = data.begin();
                while (it!=data.end()) {
                    s_heap.insert(*it);
                    ++it;
                }
            }
            bulkInsertReverseSorted(s_heap.beginPopFront(),data.size());
        }
        
        /**
         * @param data_size number of elements to insert
         * @param callback_ptr - optional callback function to call for each inserted unique element
         * @return number of unique elements inserted
         */
        template <typename iterator_t> size_t bulkInsertUnique(iterator_t data_begin, iterator_t data_end, std::size_t data_size,
            CallbackT *callback_ptr = nullptr)
        {
            std::size_t unique_count = 0;
            assert(m_size + data_size <= m_capacity);
            SortedArray<data_t,comp_t> data_buf(begin(), end());
            // sort heap
            heap<data_t,comp_t> s_heap(data_size);
            {
                auto it = data_begin;
                while (it!=data_end) {
                    s_heap.insert(*it);
                    ++it;
                }
            }
            data_t *item = const_cast<data_t*>(data_buf.m_begin);
            while (!s_heap.empty() && item != data_buf.m_end) {
                item = const_cast<data_t*>(data_buf.join(item, s_heap.front(), 1));
                if (item != data_buf.m_end) {
                    // insert / grow vector
                    if (data_buf.m_comp(s_heap.front(), *item)) {
#ifdef  __linux__
	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
                        memmove((item + 1), item, (data_buf.m_end - item) * sizeof(data_t));
#ifdef  __linux__
	#pragma GCC diagnostic pop
#endif
                        ++(data_buf.m_end);
                        ++(this->m_size);
                        ++unique_count;
                        if (callback_ptr) {
                            (*callback_ptr)(s_heap.front());
                        }
                    }
                    *item = s_heap.front();
                    s_heap.pop_front();
                }
            }
            // complete with append / grow vector
            if (!s_heap.empty()) {
                item = const_cast<data_t*>(data_buf.m_end);
                *item = s_heap.front();
                ++unique_count;
                if (callback_ptr) {
                    (*callback_ptr)(s_heap.front());
                }
                s_heap.pop_front();
                ++(this->m_size);
                while (!s_heap.empty()) {
                    // append if unique
                    if (data_buf.m_comp(*item,s_heap.front())) {
                        ++item;
                        *item = s_heap.front();
                        ++(this->m_size);
                        ++unique_count;
                        if (callback_ptr) {
                            (*callback_ptr)(s_heap.front());
                        }
                    }
                    s_heap.pop_front();
                }
            }
            return unique_count;
        }

        /**
         * insert new / update existing
         * @return number of unique items inserted
         */
        template <typename iterator_t> std::size_t bulkInsertUnique(iterator_t data_begin, iterator_t data_end) {
            return bulkInsertUnique(data_begin, data_end, std::distance(data_begin, data_end));
        }

        /**
         * @return number of unique items inserted
         */
        std::size_t bulkInsertUnique(const std::vector<data_t> &data) {
            return bulkInsertUnique(data.begin(), data.end());
        }

        void bulkEraseUnique(const std::vector<data_t> &data, DestroyF item_destroy_func = {})
        {
            // sort heap
            heap<data_t,comp_t> s_heap((int)data.size());
            {
                auto it = data.begin(), end = data.end();
                while (it != end) {
                    s_heap.insert(*it);
                    ++it;
                }
            }
            SortedArray<data_t,comp_t> data_buf(begin(), end());
            data_t *item = reinterpret_cast<data_t*>(data_buf.m_begin);
            while (!s_heap.empty() && (item!=data_buf.m_end)) {
                item = (data_t*)data_buf.join(item, s_heap.front(), 1);
                if (item != data_buf.m_end) {
                    // erase item
                    if (!data_buf.m_comp(s_heap.front(),*item)) {
                        if (item_destroy_func) item_destroy_func(*item);
                        memmove(item, &item[1], (data_buf.m_end - item - 1) * sizeof(data_t));
                        // compact vector
                        --(data_buf.m_end);
                        --(this->m_size);
                    }
                    s_heap.pop_front();
                }
            }
        }

        /**
         * insert (preserve ascending sequence order)
         * @return inserted element
         */
        iterator insert(const data_t &data)
        {
            assert(!is_full());
            SortedArray<data_t,comp_t> data_buf(begin(), end());
            iterator item = const_cast<data_t*>(data_buf.join(data_buf.m_begin, data, 1));
            if (item != data_buf.m_end) {
                iterator it_dest = (iterator)data_buf.m_end;
                iterator it_src = it_dest;
                --it_src;
                while (it_src != item) {
                    *it_dest = *it_src;
                    --it_dest;
                    --it_src;
                }
                *it_dest = *it_src;
            }
            *item = data;
            ++this->m_size;
            return item;
        }

        /**
         * insert if unique entry
         * @return item iterator, flag (true if new entry)
         */
        std::pair<iterator, bool> insertUnique(const data_t &data) 
        {
            assert (!is_full());
            SortedArray<data_t,comp_t> data_buf(begin(),end());
            data_t *item = reinterpret_cast<data_t*>(data_buf.join(data_buf.m_begin, data, 1));
            if ((item==data_buf.m_end) || (data_buf.m_comp(data,*item))) {
                if (item != data_buf.m_end) {
                    data_t *it_dest = reinterpret_cast<data_t*>(data_buf.m_end);
                    data_t *it_src = it_dest;
                    --it_src;
                    while (it_src != item) {
                        *it_dest = *it_src;
                        --it_dest;
                        --it_src;
                    }
                    *it_dest = *it_src;
                }
                *item = data;
                ++this->m_size;
                return { item, true };
            } else {
                // duplicate, no insert
                return { item, false };
            }
        }

        /**
         * Erase element at specified position
         */
        void eraseAt(std::size_t index, DestroyF item_destroy_func = {})
        {
            assert (index < this->m_size);
            // pack vector
            data_t *dest = &getData()[index];
            data_t *src = dest + 1;
            data_t *data_end = &getData()[m_size];
            if (item_destroy_func) item_destroy_func(*dest);            
            while (src < data_end) {
                *dest = *src;
                ++dest;
                ++src;
            }
            --(this->m_size);
        }

        /**
         * Erase multiple elements in a single call
         * InputIterator - points to "const iterator" of this vector, must contain ascending sorted iterators
         * it / it_end - may contain excess elements (which will be ignored)
         */
        template<typename InputIterator> std::size_t bulkEraseSorted(InputIterator it, InputIterator it_end,
            DestroyF item_destroy_func = {}, CallbackT *callback_ptr = nullptr)
        {
            iterator data_it = getData();
            iterator data_end = &getData()[m_size];
            
            std::size_t erase_count = 0;
            while (it != it_end) {
                auto data_found_it = std::lower_bound(data_it, data_end, *it);
                if (data_found_it == data_end) {
                    break;
                }

                if (*it == *data_found_it) {
                    if (item_destroy_func) {
                        item_destroy_func(*data_found_it);
                    }
                    if (callback_ptr) {
                        (*callback_ptr)(*data_found_it);
                    }
                    // Set new begining of search range
                    data_it = data_found_it;
                    // Shift vector by one element
                    std::copy(std::next(data_found_it), data_end, data_found_it);
                    --data_end;
                    ++erase_count;
                }
                ++it;
            }
            this->m_size -= erase_count;
            return erase_count;
        }

        template <typename KeyT> std::size_t bulkErase(std::function<bool(KeyT)> f, DestroyF item_destroy_func = {},
            CallbackT *callback_ptr = nullptr)
        {
            iterator it = getData();
            iterator it_end = &getData()[m_size];
            
            std::size_t erase_count = 0;
            while (it != it_end) {
                if (f(*it)) {
                    if (item_destroy_func) {
                        item_destroy_func(*it);
                    }
                    if (callback_ptr) {
                        (*callback_ptr)(*it);
                    }
                    ++erase_count;                    
                } else {
                    if (erase_count > 0) {
                        *(it - erase_count) = *it;
                    }
                }
                ++it;
            }
            this->m_size -= erase_count;
            return erase_count;
        }
        
        /**
         * Find element by key
         * @return nullptr if element not found, valid iterator if found
         */
        template <class KeyT> const_iterator find(KeyT key) const
        {
            SortedArray<data_t,comp_t> data_buf(begin(), end());
            const_iterator it = data_buf.join(data_buf.m_begin, key, 1);
            if (it == data_buf.m_end) {
                return 0;
            }
            // check equal value
            if (data_buf.m_comp(*it,key) || data_buf.m_comp(key,*it)) {
                return 0;
            } else {
                return it;
            }
        }

        template <class KeyT> const_iterator findLowerEqualBound(KeyT key) const
        {
            SortedArray<data_t,comp_t> data_buf(begin(),end());
            auto it = data_buf.join(data_buf.m_end, key, -1);
            if (it == data_buf.m_end) {
                return 0;
            }
            return it;
        }
        
        /**
         * Find set of elements by set of keys (sorted ascending, no duplicates)
         */
        template <class KeyT> void findSet(const std::set<KeyT> &keys, std::list<const data_t*> &find_result) const
        {
            SortedArray<data_t,comp_t> data_buf(begin(),end());
            const data_t *item = data_buf.m_begin;
            auto it_key = keys.begin();
            while (it_key != keys.end() && item != data_buf.m_end) {
                // search within the remaining subset
                item = data_buf.join(item, *it_key, 1);
                if (item == data_buf.m_end) {
                    // key not found
                    find_result.push_back(0);
                } else {
                    find_result.push_back(item);
                    ++item;
                }
                ++it_key;
            }
            while (it_key!=keys.end()) {
                // key not found
                find_result.push_back(0);
                ++it_key;
            }
        }

        bool isGrowing() const {
            return (m_state == sv_state::growing);
        }

        bool isShrinking() const {
            return (m_state == sv_state::shrinking);
        }

        /**
         * modify state to "sv_shrinking"
         */
        void setShrinking() {
            m_state = sv_state::shrinking;
        }

        iterator begin() {
            return getData();
        }

        const_iterator begin() const {
            return getData();
        }

        iterator begin(std::size_t index) 
        {
            assert (index < (size_t)m_size);
            return getData() + index;
        }

        const_iterator begin(std::size_t index) const 
        {
            assert (index < (std::size_t)m_size);
            return getData() + index;
        }

        iterator end() {
            return getData() + m_size;
        }

        const_iterator end() const {
            return getData() + m_size;
        }

        const data_t &front() const 
        {
            assert (!empty());
            return getData()[0];
        }

        const data_t &back() const 
        {
            assert (!empty());
            return getData()[m_size - 1];
        }

        // NOTE: buffer's address may be affected by modify therefore items should only be modified by index
        data_t &modifyItem(unsigned int index) {
            return getData()[index];
        }

        joinable_const_iterator beginJoin(int direction) const
        {
            if (direction > 0) {
                return joinable_const_iterator(getData(), getData() + m_size, getData(), direction);
            } else {
                return joinable_const_iterator(getData(), getData() + m_size,
                    (m_size > 0) ? (getData() + m_size - 1): getData(), direction);
            }
        }
        
        /**
         * Evaluate maximum number of items that can be stored
         * with the specified buffer (size)
         */
        static std::uint32_t getMaxSize(std::size_t buf_size)
        {
            std::size_t size_of_members = sizeof(self);
            if (buf_size > size_of_members) {
                return static_cast<std::uint32_t>((buf_size - size_of_members) / sizeof(data_t));
            } else {
                return 0;
            }
        }

        void destroy(Memspace &, DestroyF item_destroy_func = {}) const
        {
            // destroy all items
            if (item_destroy_func) {
                for (const data_t *it = begin(), *it_end = end(); it!=it_end; ++it) {
                    item_destroy_func(*it);
                }
            }
        }

        /**
         * @param item must be item from this collection
         * @return 0 based calculated element index (position within vector)
         */
        inline std::uint64_t getItemIndex(const data_t *item) const 
        {
            assert(item < end());
            return item - getData();
        }

    public:
        // maximum size (capacity)
        std::uint32_t m_capacity;
        // actual size
        std::uint32_t m_size = 0;
        // vector state (0 = growing, 1 = shrinking)
        sv_state m_state;

        inline data_t *getData() {
            return reinterpret_cast<data_t*>(reinterpret_cast<std::byte*>(this) + sizeof(self));
        }

        inline const data_t *getData () const {
            return reinterpret_cast<const data_t*>(reinterpret_cast<const std::byte*>(this) + sizeof(self));
        }
    };
DB0_PACKED_END

    /**
     * NOTICE : destroy does not call any overlaid item destructors
     * in order to destroy items, call erase / clear first
     */
    template <typename data_t, typename AddrT = std::uint64_t, typename comp_t = std::less<data_t> > 
    class v_sorted_vector:
        public v_object<o_sv_container<data_t,comp_t> >
    {
    private :

        static std::size_t calculateCapacity(std::size_t data_size)
        {
            std::size_t result = 1;
            while (result < data_size) {
                result <<= 1;
            }
            return result;
        }

    public:
        using self_t = v_sorted_vector<data_t, AddrT, comp_t>;
        using super_t = v_object<o_sv_container<data_t,comp_t> >;
        using addr_t = AddrT;        
        using c_type = o_sv_container<data_t,comp_t>;
        using ptr_t = typename super_t::ptr_t;
        using iterator = typename c_type::iterator;
        using const_iterator = typename c_type::const_iterator;
        using joinable_const_iterator = typename c_type::joinable_const_iterator;
        using DestroyF = std::function<void(const data_t&)>;
        using CallbackT = std::function<void(data_t)>;
        
        v_sorted_vector() = default;

        /**
         * V-Space allocating constructor
         */
        v_sorted_vector(Memspace &memspace, std::uint32_t capacity = 8, sv_state state = sv_state::growing, DestroyF item_destroy_func = {})
            : super_t(memspace, capacity, state)
            , m_item_destroy_func(item_destroy_func)
        {
            assert((*this)->m_capacity > 0);
        }
        
        v_sorted_vector(Memspace &memspace, const v_sorted_vector &other)
            : super_t(memspace, *other.getData())
            , m_item_destroy_func(other.m_item_destroy_func)
        {
        }

        /**
         * Construct and populate with items in collection specified by range begin / end \
         * max_size will be evaluated according to collection size
         * begin / end assumed to be ascending sorted
         */
        v_sorted_vector(Memspace &memspace, const data_t *begin, const data_t *end, std::optional<std::uint64_t> capacity = {},
            sv_state state = sv_state::growing, DestroyF item_destroy_func = {})
            : super_t(memspace, (capacity ? *capacity : calculateCapacity(std::distance(begin, end))), state)
            , m_item_destroy_func(item_destroy_func)
        {
            assert((*this)->m_capacity > 0);
            bulkPushBack(begin, std::distance(begin, end));
        }
        
        /**
         * V-Space referencing constructor
         */
        v_sorted_vector(const ptr_t &ptr, DestroyF item_destroy_func = {})
            : super_t(ptr)
            , m_item_destroy_func(item_destroy_func)
        {
            assert((*this)->m_capacity > 0);
        }

        /**
         * V-Space referencing constructor
         */
        v_sorted_vector(mptr ptr, DestroyF item_destroy_func = {})
            : super_t(ptr)
            , m_item_destroy_func(item_destroy_func)
        {
            assert((*this)->m_capacity > 0);
        }
        
        v_sorted_vector(std::pair<Memspace*, AddrT> addr)
            : v_sorted_vector(addr.first->myPtr(addr.second))
        {
        }

        // type ID for serialization
        static std::uint64_t getSerialTypeId()
        {
            return db0::serial::typeId<self_t>(
                (db0::serial::typeId<data_t>() << 32) | (db0::serial::typeId<AddrT>() << 16) |
                static_cast<std::uint16_t>(db0::serial::CollectionType::VSortedVector)
            );
        }

        const data_t &operator[](size_t index) const {
            return (*this)->getData()[index];
        }
        
        /**
         * Erase element at specified position
         */
        void eraseAt(int index, bool &addr_changed) 
        {
            // erase element
            this->modify().eraseAt(index, m_item_destroy_func);
            // compact vector if necessary (shrinking state only)
            addr_changed |= compactShrinking();
        }

        /**
         * Erase item without changing address of this instance
         * @param it_item
         */
        void eraseItem(const_iterator it_item)
        {
            // erase element
            // NOTE: need to use index because modify() may invalidate iterator
            auto index = (*this)->getItemIndex(it_item);
            this->modify().eraseAt(index, m_item_destroy_func);
        }

        /**
         * Erase specified element
         */
        void eraseItem(const_iterator it_item, bool &was_addr_changed)
        {
            eraseItem(it_item);
            // compact vector if necessary (shrinking state only)
            was_addr_changed |= compactShrinking();
        }

        /**
         * erase element by key, throws
         * NOTE : this address may change as an effect of erase
         * addr_changed = flag set to true on address changed
         * vector will be auto-destroyed if compacted to size = 0
         */
        template<class KeyT>
        bool erase(KeyT key, bool &addr_changed) 
        {
            const_iterator data = (*this)->find(key);
            if (data) {
                eraseItem(data, addr_changed);
                return true;
            } else {
                return false;
            }
        }

        void pop_front(int count, bool &addr_changed) 
        {
            this->modify().pop_front(count, m_item_destroy_func);
            // compact vector if necessary ( shrinking state only )
            addr_changed |= compactShrinking();
        }

        void pop_back(int count, bool &addr_changed) 
        {
            this->modify().pop_back(count, m_item_destroy_func);
            // compact vector if necessary ( shrinking state only )
            addr_changed |= compactShrinking();
        }
        
        bool empty() const {
            return ((*this)->m_size == 0);
        }
        
        void destroy()
        {
            // container destroy
            (*this)->destroy(this->getMemspace(), m_item_destroy_func);
            super_t::destroy();
        }

        /**
         * grow vector if necessary
         * @return inserted item iterator
         */
        iterator insert(const data_t &data, bool &addr_changed, std::optional<std::uint32_t> max_size = {})
        {
            addr_changed |= growVector((*this)->m_size + 1, max_size);
            return this->modify().insert(data);
        }
        
        /**
         * Insert, resize if necessary
         * @param data element to insert
         * @return
         */
        iterator insert(const data_t &data, std::optional<std::uint32_t> max_size = {})
        {
            growVector((*this)->m_size + 1, max_size);
            return this->modify().insert(data);
        }

        /**
         * Update existing element in place, without changing its key part
        */
        bool updateExisting(const data_t &data, data_t *old_data = nullptr)
        {
            auto it = (*this)->find(data);
            if (it == (*this)->end()) {
                return false;
            }
            
            if (old_data) {
                *old_data = *it;
            }
            // NOTE: need to use index because modify() may invalidate iterator
            auto index = (*this)->getItemIndex(it);
            this->modify().modifyItem(index) = data;
            return true;
        }

        bool findOne(data_t &data) const
        {
            auto it = (*this)->find(data);
            if (it == (*this)->end()) {
                return false;
            }

            data = *it;
            return true;
        }
        
        /**
         * grow vector if necessary
         */
        std::pair<iterator,bool> insertUnique(const data_t &data, bool &addr_changed, 
            std::optional<std::uint32_t> max_size = {})
        {
            addr_changed |= growVector((*this)->m_size + 1, max_size);
            return this->modify().insertUnique(data);
        }
        
        /**
         * @return true if object relocated
         */
        bool bulkInsert(const std::vector<data_t> &data, std::optional<std::uint32_t> max_size = {})
        {
            bool result = growVector((*this)->m_size + data.size(), max_size);
            this->modify().bulkInsert(data);
            return result;
        }
        
        /**
         * insert new / update existing
         * @result - optional buffer to write number of items requested to add / number of items actually added
         * @return true if object relocated
         */
        template <typename iterator_t> bool bulkInsertUnique(iterator_t data_begin, iterator_t data_end, 
            std::pair<std::uint32_t, std::uint32_t> *result,
            CallbackT *callback_ptr = nullptr, std::optional<std::uint32_t> max_size = {})
        {
            std::size_t data_size = std::distance(data_begin, data_end);
            if (result) {
                result->first = static_cast<std::uint32_t>(data_size);
            }
            bool addr_change = growVector((*this)->m_size + data_size, max_size);
            std::size_t unique_count = this->modify().bulkInsertUnique(data_begin, data_end, data_size, callback_ptr);
            if (result) {
                result->second = static_cast<std::uint32_t>(unique_count);
            }
            return addr_change;
        }

        /**
         * insert new / update existing
         * @return true if object relocated
         */
        bool bulkInsertUnique(const std::vector<data_t> &data) {
            return bulkInsertUnique(data.begin(), data.end());
        }

        /**
         * count - number of items being inserted (must be REVERSE sorted)
         * @return true if object relocated
         */
        template <class ItemIterator> bool bulkInsertReverseSorted(ItemIterator it_begin, std::size_t count,
            std::optional<std::uint32_t> max_size = {})
        {
            bool result = growVector((*this)->m_size + count, max_size);
            this->modify().bulkInsertReverseSorted(it_begin, count);
            return result;
        }

        /**
         * count - number of items being inserted (must be sorted)
         * @return true if object relocated
         */
        template <class ItemIterator> bool bulkPushBack(ItemIterator it_begin, std::size_t count, 
            std::optional<std::uint32_t> max_size = {})
        {
            bool result = growVector((*this)->m_size + count, max_size);
            this->modify().bulkPushBack(it_begin, count);
            return result;
        }

        /**
         * it - must point to sorted collection of const_iterators
         */
        template <class InputIterator>
        std::size_t bulkEraseSorted(InputIterator it, InputIterator it_end, CallbackT *callback_ptr = nullptr) {
            return this->modify().bulkEraseSorted(it, it_end, m_item_destroy_func, callback_ptr);
        }

        template <class InputIterator>
        std::size_t bulkEraseSorted(InputIterator it, InputIterator it_end, bool &addr_changed, CallbackT *callback_ptr = nullptr)
        {
            auto erase_count = this->modify().bulkEraseSorted(it, it_end, m_item_destroy_func, callback_ptr);
            addr_changed |= compactShrinking();
            return erase_count;
        }

        template <typename KeyT>
        std::size_t bulkErase(std::function<bool(KeyT)> f, bool &addr_changed, CallbackT *callback_ptr = nullptr)
        {
            auto erase_count = this->modify().bulkErase(f, m_item_destroy_func, callback_ptr);
            addr_changed |= compactShrinking();
            return erase_count;
        }
        
        /**
         * Sorts element before erasing, if your elements are already sorted then better use bulkEraseSorted
         */
        template<typename InputIterator>
        std::size_t bulkErase(InputIterator it, InputIterator it_end, CallbackT *callback_ptr = nullptr) 
        {
            using KeyType = typename std::decay<decltype(*it)>::type;
            std::vector<KeyType> sorted_keys(it, it_end);
            std::sort(sorted_keys.begin(), sorted_keys.end(), [](const KeyType &k0, const KeyType &k1) {
                return (k0 < k1);
            });
            // erase from sorted
            return bulkEraseSorted(sorted_keys.begin(), sorted_keys.end(), callback_ptr);
        }

        /**
         * Erase existing items only, ignore other
         */
        void bulkEraseUnique(const std::vector<data_t> &data) {
            this->modify().bulkEraseUnique(data, m_item_destroy_func);
        }

        /**
         * Grows vector x2 to fit new_size and up to the max_size
        */
        bool growVector(std::size_t new_size, std::optional<std::uint32_t> max_size = {})
        {
            assert(!max_size || (new_size <= *max_size));
            assert((*this)->m_capacity > 0);
            if ((*this)->m_capacity < new_size) {
                std::uint32_t new_capacity = (*this)->m_capacity;
                while (new_capacity < new_size) {
                    new_capacity <<= 1;
                }
                if (max_size) {
                    new_capacity = std::min(new_capacity, *max_size);
                }
                v_sorted_vector new_vector(this->getMemspace(), (*this)->begin(), (*this)->end(), new_capacity,
                    (*this)->m_state, this->m_item_destroy_func);
                // delete VSPACE "this"
                this->destroy();
                // claim new identity
                (*this) = new_vector;
                return true;
            } else {
                return false;
            }
        }

        /**
         * Compact to fit content
         * @return true on object relocated
         */
        bool compact()
        {
            std::uint32_t _size = (*this)->m_size;
            // align to pow-2
            std::uint32_t new_capacity = 1;
            while (new_capacity < _size) {
                new_capacity <<= 1;
            }
            if (new_capacity > 4 && new_capacity < (*this)->m_capacity) {
                // VSPACE copy resized ( preserve sv_shrinking state of the new object )
                v_sorted_vector new_vector(this->getMemspace(), (*this)->begin(), (*this)->end(), new_capacity, 
                    (*this)->m_state, this->m_item_destroy_func);
                // delete VSPACE "this"
                this->destroy();
                // claim new identity
                (*this) = new_vector;
                return true;
            } else {
                return false;
            }            
        }
        
        /**
         * @return true on object relocated
         */
        bool compactShrinking()
        {
            if ((*this)->isShrinking()) {
                return compact();
            } else {
                return false;
            }
        }

        /**
         * Evaluate maximum number of items that can be stored
         * with the specified buffer (size)
         */
        static std::uint32_t getMaxSize(std::uint32_t buf_size) {
            return c_type::getMaxSize(buf_size);
        }

        /**
         * Split current vector at specified split point
         * NOTICE : this object not relocated
         */
        v_sorted_vector split(const_iterator it_split)
        {
            assert(it_split!=(*this)->end());
            auto elem_count = std::distance(it_split, (*this)->end());
            v_sorted_vector new_vector(this->getMemspace(), it_split, (*this)->end(), (*this)->m_capacity, 
                (*this)->m_state, this->m_item_destroy_func);
            this->modify().m_size -= elem_count;
            return new_vector;
        }

        /**
         * Append other sorted vector (move all items)
         * all data from "other_vector" must be >= from this vector's data
         */
        void moveSorted(v_sorted_vector &&other_vector) {
            this->modify().appendSorted(other_vector->begin(), other_vector->end());
        }
        
        const_iterator begin() const {
            return (*this)->begin();
        }

        const_iterator end() const {
            return (*this)->end();
        }

        const_iterator find(const data_t &item) const 
        {
            auto it = (*this)->find(item);
            if (it==nullptr) {
                // replace nullptr result with "end" iterator for compliance
                return (*this)->end();
            }
            return it;
        }

        std::uint64_t size() const {
            return (*this)->m_size;
        }
        
        joinable_const_iterator beginJoin(int direction) const {
            return (*this)->beginJoin(direction);
        }

        /**
         * @return position number by iterator (vector element index)
         * NOTICE: this is always position from beginning no matter what direction of iteration
         */
        std::uint64_t getPosition(const joinable_const_iterator &it) const {
            return (*this)->getItemIndex(it.getConstIterator());
        }

        std::uint64_t sizeOf() const {
            return (*this)->sizeOf();
        }
        
		bindex::type getIndexType() const {
			return bindex::type::sorted_vector;
		}

    private:
        DestroyF m_item_destroy_func;
    };

}
