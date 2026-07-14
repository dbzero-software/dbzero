// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <stack>
#include <map>
#include <unordered_set>
#include <cstring>
#include <optional>
#include "v_bdata_block.hpp"
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include <dbzero/core/utils/uuid.hpp>
#include <dbzero/object_model/object_header.hpp>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
    
    enum class BVectorOptions: std::uint16_t
    {
        // Forces all allocations to be constant size (= page size)
        FIXED_BLOCK = 0x0001,
    };
    
    using BVectorFlags = FlagSet<BVectorOptions>;

DB0_PACKED_BEGIN
    template <typename PtrT>
    class DB0_PACKED_ATTR o_bvector: public o_base<o_bvector<PtrT>, 0, true>
    {
        using super_t = o_base<o_bvector<PtrT>, 0, true>;
        friend super_t;

        o_bvector() = default;
        o_bvector(std::uint32_t page_size_hint, BVectorFlags flags = {});

    public:
        // common dbzero object header
        db0::o_unique_header m_header;
        // root node pointer (may be data or pointers' block)
        PtrT m_ptr_root = {};
        // number of items contained
        std::uint64_t m_size = 0;
        // page size hint
        std::uint32_t m_page_size;
        BVectorFlags m_flags;
        
        static std::size_t measure(std::uint32_t page_size_hint, BVectorFlags flags = {});

        template <typename buf_t> static std::size_t safeSizeOf(buf_t buf);

        void incRef(bool is_tag) {
            m_header.incRef(is_tag);
        }
        
        bool hasRefs() const {
            return m_header.hasRefs();
        }
    };
DB0_PACKED_END
    
    /**
     * dbzero scalable vector implementation
     * @tparam ItemT - fixed size item type (simple value or struct without pointers)
     * @tparam PtrT - type for inner pointers (V-Space)
     */
    template <typename ItemT, typename PtrT = Address> class v_bvector
        : public v_object<o_bvector<PtrT> >
    {
        using self_t = v_bvector<ItemT, PtrT>;
        // interface to data v_bdata_block for specific block-class
        static const DataBlockInterfaceArray<ItemT, 13u> m_interface_array;

    public:
        using super_t = v_object<o_bvector<PtrT> >;
        using ptr_t = typename super_t::ptr_t;
        using DataBlockOverlaidType = o_block_data<ItemT, 0>;
        using DataBlockType = v_bdata_block<ItemT, 0>;

        v_bvector() = default;

        /**
         * New, empty instance of the data structure
         */
        v_bvector(Memspace &mem, BVectorFlags flags = {}, AccessFlags access_mode = {})
            : super_t(mem, mem.getPageSize(), flags, access_mode)
            , m_db_shift(data_container::shift(mem.getPageSize()))
            , m_db_mask(data_container::mask(mem.getPageSize()))
            , m_pb_shift(ptr_container::shift(mem.getPageSize()))
            , m_pb_mask(ptr_container::mask(mem.getPageSize()))
        {                 
        }

        v_bvector(mptr ptr, AccessFlags access_mode = {})
            : super_t(ptr, access_mode)
            , m_db_shift(data_container::shift((*this)->m_page_size))
            , m_db_mask(data_container::mask((*this)->m_page_size))
            , m_pb_shift(ptr_container::shift((*this)->m_page_size))
            , m_pb_mask(ptr_container::mask((*this)->m_page_size))
        {
        }

        v_bvector(db0::tag_verified, mptr ptr, std::size_t size_of = 0, AccessFlags access_mode = {})
            : super_t(db0::tag_verified(), ptr, size_of, access_mode)
            , m_db_shift(data_container::shift((*this)->m_page_size))
            , m_db_mask(data_container::mask((*this)->m_page_size))
            , m_pb_shift(ptr_container::shift((*this)->m_page_size))
            , m_pb_mask(ptr_container::mask((*this)->m_page_size))            
        {
        }
        
        v_bvector(const v_bvector &&other)
            : super_t(std::move(other))
            , m_db_shift(other.m_db_shift)
            , m_db_mask(other.m_db_mask)
            , m_pb_shift(other.m_pb_shift)
            , m_pb_mask(other.m_pb_mask)            
        {
        }
        
        void operator=(v_bvector &&other)
        {
            assert(this->m_db_shift == other.m_db_shift);
            assert(this->m_db_mask == other.m_db_mask);
            assert(this->m_pb_shift == other.m_pb_shift);
            assert(this->m_pb_mask == other.m_pb_mask);

            // clean local cached objects first
            this->m_pb_cache.clear();
            this->m_last_block_key = {0, 0};
            this->m_last_block = nullptr;
            super_t::operator=(std::move(other));
        }
        
        // Construct populated with values from a specific sequence
        template <class SequenceT> v_bvector(Memspace &mem, const SequenceT &in, BVectorFlags flags = {}, 
            AccessFlags access_mode = {})
            : v_bvector(mem, flags, access_mode)
        {
            for (const auto &item: in) {
                push_back(item);
            }
        }
        
        template <class SequenceT>
        void init(Memspace &mem, const SequenceT &in, AccessFlags access_mode = {})
        {
            super_t::init(mem, access_mode);
            for (const auto &item: in) {
                push_back(item);
            }            
        }
        
        std::uint16_t initUnique(Memspace &mem, AccessFlags access_mode = {})
        {
            auto page_size = mem.getPageSize();
            auto result = super_t::initUnique(mem, page_size, access_mode);
            this->m_db_shift = data_container::shift(mem.getPageSize());
            this->m_db_mask = data_container::mask(mem.getPageSize());
            this->m_pb_shift = ptr_container::shift(mem.getPageSize());
            this->m_pb_mask = ptr_container::mask(mem.getPageSize());
            return result;
        }

        template <class SequenceT> 
        std::uint16_t initUnique(Memspace &mem, const SequenceT &in, AccessFlags access_mode = {})
        {
            auto result = this->initUnique(mem, access_mode);
            for (const auto &item: in) {
                push_back(item);
            }
            return result;
        }

        /**
         * Grow vector if necessary, modify item in place
         */
        ItemT &modifyItem(std::uint64_t index) 
        {
            if (!(index < size())) {
                growBy((std::uint32_t)(index - size() + 1));
            }
            getDataBlock(getKey(0, index));
            return (*m_last_block).modify().modifyItem((std::size_t)(index & m_db_mask));
        }

        /**
         * Grow vector if necessary, modify item with provided lambda function
         */
        ItemT modifyItem(std::uint64_t index, std::function<ItemT(ItemT &)> f) 
        {
            if (!(index < size())) {
                growBy((std::uint32_t)(index - size() + 1));
            }
            // access within data block element (not thread safe)
            getDataBlock(getKey(0, index));
            return f((*m_last_block).modify().modifyItem((std::size_t)(index & m_db_mask)));
        }

        void setItem(std::uint64_t index, const ItemT &item)
        {
            if (!(index < size())) {
                growBy((std::uint32_t)(index - size() + 1));
            }
            // access within data block element (not thread safe)
            getDataBlock(getKey(0, index));
            (*m_last_block).modify().modifyItem((std::size_t)(index & m_db_mask)) = item;
        }

        // threadsafe
        ItemT getItem(std::uint64_t index) const 
        {
            assert (index < size());
            return getDataBlock(getKey(0,index))->getItem(index & m_db_mask);
        }

        // threadsafe
        ItemT operator[](std::uint64_t index) const {
            return getItem(index);
        }

        /**
         * @return number of items contained (indexed from 0 to size - 1)
         */
        std::uint64_t size() const {
            return (*this)->m_size;
        }

        bool empty() const {
            return ((*this)->m_size == 0);
        }

        /**
         * Reduce size of the vector by 1 item
         */
        ItemT pop_back() 
        {
            assert(!empty());
            ItemT result = getItem(size() - 1);
            pop_back(1);
            return result;
        }

        /**
         * Reduce size of the vector by "count"
         */
        void pop_back(std::uint64_t count)
        {
            if (count > 0) {
                assert(!(size() < count));
                b_key key = getKey(0, size() - 1);
                std::size_t span = getDataBlockSize(key);
                while (count > 0) {
                    // pop full block
                    if (count >= span) {
                        destroyDataBlock(key);
                        count -= span;
                        this->modify().m_size -= span;
                    } else {
                        this->modify().m_size -= count;
                        count = 0;
                        // compact single data block if possible
                        if (height() == 1) {
                            growBlock(evaluateBClass(size()));
                        }
                        break;
                    }
                    // full block span
                    span = (std::size_t)(1 << (m_db_shift - this->getBClass()));
                    --(key.second);
                }
                // invalidate to recalculate
                this->m_b_class = {};
            }
        }

        void destroy()
        {
            destroyAllBlocks();
            m_pb_cache.clear();
            m_last_block_key = {0, 0};
            m_last_block = nullptr;
            super_t::destroy();
        }

        void destroy(Memspace& memspace) 
        {
            destroyAllBlocks();
            m_pb_cache.clear();
            m_last_block_key = {0, 0};
            m_last_block = nullptr;
            super_t::destroy(memspace);
        }

        int height() const {
            return height((*this)->m_size);
        }

        template<typename Iterator_T>
        void push_back(Iterator_T it, Iterator_T end) 
        {
            for (; it != end; ++it) {
                push_back(*it);
            }
        }

        /**
         * Remove all elements
         */
        void clear() {
            this->pop_back(this->size());
        }

        /**
         * Erase specific element numbers
         * @param element_numbers (in ascending order)
         */
        void swapAndPop(const std::vector<uint64_t> &element_numbers)
        {
            if (element_numbers.size() == this->size()) {
                clear();
            } else {
                std::unordered_set<uint64_t> values;
                for (auto item_num: element_numbers) {
                    values.insert(item_num);
                }
                for (auto item_num: element_numbers) {
                    if (item_num >= this->size()) {
                        break;
                    }

                    // erase from back
                    auto buf_size = this->size();
                    unsigned int pop_back_count = 0;
                    while ((buf_size > 0) && (values.find(buf_size - 1) != values.end())) {
                        ++pop_back_count;
                        --buf_size;
                    }

                    if (pop_back_count > 0) {
                        // remove elements from end
                        this->pop_back(pop_back_count);
                    }

                    if (item_num < this->size()) {
                        // take element from end and place it position of the one to be deleted
                        auto element = this->pop_back();
                        this->setItem(item_num, element);
                    }
                }
            }
        }

        void erase(std::uint64_t position)
        {
            // move vector content
            while (position < size() -1) {
                setItem(position, (*this)[position+1]);
                ++position;
            }
            //pop last duplicated element
            pop_back();
        }

        /**
         * Erase elements matching specific value condition
         * @return number of removed elements
         */
        std::uint64_t swapAndPop(std::function<bool(const ItemT &item)> item_func) 
        {
            std::vector<std::uint64_t> element_numbers;
            std::uint64_t index = 0;
            auto it = cbegin(), end = cend();
            while (it!=end) {
                if (item_func(*it)) {
                    element_numbers.emplace_back(index);
                }
                ++it;
                ++index;
            }
            auto result = element_numbers.size();
            if (!element_numbers.empty()) {
                swapAndPop(element_numbers);
            }
            return result;
        }

        template<typename Iterator_T, typename Iterator_T2>
        void reverse_replace(Iterator_T source_begin, Iterator_T source_end,
                Iterator_T2 dest_begin, Iterator_T2 dest_end)
        {
            --source_end;
            --source_begin;
            --dest_end;
            --dest_begin;

            for(; source_begin != source_end && dest_begin != dest_end; --source_end, --dest_end) {
                dest_end.modifyItem() = *source_end;
            }
        }

        template<typename Iterator_T>
        void push_at(const uint64_t start_index, const Iterator_T to_insert_begin, const Iterator_T to_insert_end) 
        {
            // xxxxx - old elements
            // * - new elements
            std::uint64_t insert_length = to_insert_end - to_insert_begin;
            std::uint64_t orig_length = size();

            if (start_index > orig_length) {
                THROWF(db0::InputException) << "start_index = " << start_index << ", vector_length = " << orig_length;
            } else if (start_index == orig_length) // xxxxx***
            {
                // just push back elements one by one at end
                push_back(to_insert_begin, to_insert_end);
            } else {
                auto source_begin_index = orig_length - start_index;

                if (start_index + insert_length > orig_length) // xxx***xx
                {
                    // push back right part of collection that is inserted on the end
                    push_back(to_insert_begin + source_begin_index, to_insert_end);

                    // then push back elements from original part of vector that will be overwritten by new values
                    push_back(begin(start_index), begin(orig_length));

                    // overwrite values in original part of vector with new values
                    reverse_replace(to_insert_begin, to_insert_begin + source_begin_index,
                                    begin(start_index), begin(start_index + source_begin_index));
                } else // x***xxxx
                {
                    // push back elements from original vector that will be overwritten by moved values
                    push_back(begin(orig_length - insert_length), begin(orig_length));

                    // move elements in original part of vector to right bound of original vector
                    reverse_replace(begin(start_index), begin(orig_length - insert_length),
                                    begin(start_index + insert_length), begin(orig_length));

                    // overwrite elements in original part of vector with new values
                    reverse_replace(to_insert_begin, to_insert_end,
                                    begin(start_index), begin(start_index + insert_length));
                }
            }
        }

        void push_back(const ItemT &item) {
            emplace_back(item);
        }

        template <typename... Args> void emplace_back(Args&& ...args) {
            setItem(size(), ItemT(std::forward<Args>(args)...));
        }

        std::size_t getBClass() const
        {
            if (!this->m_b_class) {
                this->m_b_class = evaluateBClass((*this)->m_size);
            }
            return *(this->m_b_class);
        }

        void dump(std::ostream &os) const 
        {
            b_key key = getKey(0,0);
            std::uint64_t index = 0;
            std::uint64_t end_index = size();
            std::size_t block_index = 0;
            const DataBlockType *block_ptr = 0;
            while (index!=end_index) {
                if (!block_ptr) {
                    block_ptr = &getDataBlock(key);
                    block_index = 0;
                }
                if (index > 0) {
                    os << ",";
                }
                // dump item
                (*block_ptr)->getItem(block_index).dump(os);
                ++index;
                ++block_index;
                if (block_index==(1u << (m_db_shift - this->getBClass()))) {
                    block_ptr = 0;
                    block_index = 0;
                    ++key.second;
                }
            }
        }

        /**
         * Clear the internal cache, so that memory usage is kept low
         */
        void cleanup() const 
        {
            progressive_mutex::scoped_unique_lock rw_lock(this->m_mutex);
            m_pb_cache.clear();
            m_last_block_key = { 0, 0 };
            m_last_block = nullptr;
        }

        /**
         * Detach this instance from backend
         */
        void detach() const
        {
            progressive_mutex::scoped_unique_lock rw_lock(this->m_mutex);
            m_pb_cache.clear();
            m_last_block_key = { 0, 0 };
            m_last_block = nullptr;
            // invalidate objects' b-class
            m_b_class = {};
            super_t::detach();
        }
        
        void commit() const
        {
            progressive_mutex::scoped_unique_lock rw_lock(this->m_mutex);
            for (auto &it: m_pb_cache) {
                it.second->commit();
            }
            if (m_last_block) {
                m_last_block->commit();
            }
            super_t::commit();
        }

    private:
        
        void destroyAllBlocks() const
        {
            progressive_mutex::scoped_unique_lock rw_lock(this->m_mutex);
            destroyAllBlocks(rw_lock);
        }

        void destroyAllBlocks(progressive_mutex::scoped_lock &) const 
        {
            std::unordered_set<PtrT> blocks;
            auto f = [&blocks](PtrT ptr, bool) {
                blocks.insert(ptr);
            };
            getEveryBlock(this->const_ref().m_ptr_root, height() -1, size(), f);            
            for (auto ptr : blocks) {
                this->getMemspace().free(ptr);
            }
        }
        
        void getEveryBlock(PtrT ptr, int height, size_t s, std::function<void(PtrT, bool is_data_block)> f) const 
        {
            std::stack<std::tuple<PtrT, int, size_t> > stack;
            stack.emplace(ptr, height, 0);
            std::unordered_set<PtrT> blocks;

            const auto max_index = 1ul << m_pb_shift;
            while (stack.empty() == false) {
                const auto root = std::get<0>(stack.top());
                const auto h = std::get<1>(stack.top());
                auto& i = std::get<2>(stack.top());

                if (!root.isValid()) {
                    stack.pop();
                    continue;
                }

                if (blocks.insert(root).second) {
                    f(root, h == 0);
                }

                if (h > 0) {
                    if (i < max_index) {
                        ptr_block block(this->myPtr(root));
                        stack.emplace(block->getItem(i), h-1, 0);
                        ++i;
                    } else {
                        stack.pop();
                    }
                } else {
                    auto shift = (1ul << m_db_shift);
                    bool end = shift >= s;
                    s = end ? 0 : s-shift;
                    if (s == 0) {
                        return;
                    }
                    stack.pop();
                }
            }
        }

        using ptr_container = o_block_data<PtrT, 0>;
        using ptr_block = v_object<ptr_container>;
        // B-CLASS 0 container
        using data_container = o_block_data<ItemT, 0>;
    
    protected:
        mutable progressive_mutex m_mutex;
        std::uint32_t m_db_shift = 0;
        std::uint32_t m_db_mask = 0;
        std::uint32_t m_pb_shift = 0;
        std::uint32_t m_pb_mask = 0;
        // size class of the data block (0 = full size)        
        mutable std::optional<std::size_t> m_b_class;
        
        // universal block key (height / index)
        using b_key = std::pair<int, int>;

        struct b_key_hash
        {
            std::size_t operator()(const b_key &x) const {
                return std::hash<std::uint64_t>()((std::uint64_t)x.first * 113u + (std::uint64_t)x.second);
            }
        };
        
        // pointer block cache (h > 0)
        mutable std::unordered_map<b_key, std::shared_ptr<ptr_block>, b_key_hash> m_pb_cache;
        // last accessed data block (h = 0)
        mutable b_key m_last_block_key = { 0, 0 };
        mutable std::unique_ptr<DataBlockType> m_last_block;

        /**
         * Evaluate height of the b-vector tree (function of size)
         */
        int height(std::uint64_t size) const 
        {
            int height = 0;
            if(size > 0) {
                ++height;
                --size;
                size >>= (m_db_shift - this->getBClass());
                while (size > 0) {
                    size >>= m_pb_shift;
                    ++height;
                }
            }
            return height;
        }

    public:

        /**
         * Data block factory method (construct within specific memspace)
         * @return new data block instance
         */
        std::unique_ptr<DataBlockType> newDataBlock(Memspace &memspace, std::size_t b_class) const 
        {
            assert(m_interface_array[b_class].getBClass()==b_class);
            return std::unique_ptr<DataBlockType>(reinterpret_cast<DataBlockType*>(
                m_interface_array[b_class].createNewDataBlock(memspace))
            );
        }

    private:

        /**
         * Block factory member (cast from specific b-class)
         */
        std::unique_ptr<DataBlockType> newDataBlock(std::size_t b_class) const {
            return newDataBlock(this->getMemspace(), b_class);
        }

        /**
         * Grow or compact existing data block to reach "new_b_class"
         * this is the case of single block data structure (with small number of elements)
         */
        void growBlock(std::size_t new_b_class) 
        {
            if (this->getBClass() != new_b_class) {
                // grow condition
                assert(!empty());
                assert(height()==1);
                b_key key(0, 0);
                auto new_block = newDataBlock(new_b_class);
                auto &existing_block = getDataBlock(key);
                // copy content

#ifdef  __linux__
	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
                std::memcpy (
                    &((*new_block).modify().modifyItem(0)) ,
                    &(existing_block->getItem(0)) ,
                    std::min(sizeof(ItemT) << (m_db_shift - this->getBClass()), sizeof(ItemT) << (m_db_shift - new_b_class))
                );
#ifdef  __linux__
	#pragma GCC diagnostic pop
#endif
                const_cast<DataBlockType&>(existing_block).destroy();
                // modify class
                this->m_b_class = new_b_class;
                this->modify().m_ptr_root = (*new_block).getAddress();
                this->m_last_block_key = key;
                this->m_last_block = std::move(new_block);
            }
        }

        /**
         * Create new data block, size() not affected
         */
        void push_block() 
        {
            std::uint64_t index = size();
            // h = 0 means data block (0 height)
            b_key key = getKey(0, index);
            auto new_block = newDataBlock(this->getBClass());
            // link to the the pointers' block
            if (height(index + 1) > 1) {
                // b_class = 0 means full page (full size) block
                assert(this->getBClass() == 0);
                ptr_block &block = createPtrBlock(getParentKey(key));
                block.modify().modifyItem(getParentIndex(key)) = (*new_block).getAddress();
            } else {
                // link as the root node
                this->modify().m_ptr_root = (*new_block).getAddress();
            }
        }

        /**
         * Get related parent block key
         */
        b_key getParentKey(const b_key &block_key) const {
            return b_key(block_key.first + 1, (block_key.second >> m_pb_shift));
        }

        /**
         * Get key by plain index value (data item index)
         */
        b_key getKey(int h, std::uint64_t index) const {
            return b_key(h,index >> ((m_pb_shift * h) + m_db_shift));
        }

        /**
         * Derive key (within the h-level parent block)
         */
        b_key getParentKey(int h, const b_key &key) const 
        {
            assert (h >= key.first);
            return b_key(h,key.second >> (m_pb_shift * (h - key.first)));
        }

        /**
         * Range as begin / end index (span of the global index)
         * @param index some element index within data block
         */
        std::pair<std::uint64_t, std::uint64_t> getDataBlockRange(std::uint64_t index) const 
        {
            // scale by b_class value
            index >>= (m_db_shift - this->getBClass());
            return std::make_pair(
                index << (m_db_shift - this->getBClass()), 
                (index + 1) << (m_db_shift - this->getBClass())
            );
        }

        /**
         * Range as begin / end index (span of the global index)
         */
        std::pair<std::uint64_t, std::uint64_t> getDataBlockRange(const b_key &key) const 
        {
            assert(key.first == 0);
            return std::make_pair(
                key.second << (m_db_shift - this->getBClass()),
                (key.second + 1) << (m_db_shift - this->getBClass())
            );
        }

        /**
         * Evaluate within parent node index
         */
        std::size_t getParentIndex(const b_key &key) const {
            return (key.second & m_pb_mask);
        }

        /**
         * Test for existing block key
         */
        bool keyExist(const b_key &key, bool &is_root) const 
        {
            if (empty()) {
                is_root = true;
                return false;
            }
            int _height = height();
            if (key.first >= _height) {
                is_root = true;
                return false;
            } else {
                is_root = (key==b_key(_height - 1,0));
                return !(getKey(key.first,size() - 1).second < key.second);
            }
        }
        
        /**
         * Get existing or create new block of pointers, use cache
         */
        ptr_block &createPtrBlock(const b_key &key, progressive_mutex::scoped_lock &rw_lock) 
        {
            assert(rw_lock.isLocked());
            for (;;) {
                auto it = m_pb_cache.find(key);
                if (it==m_pb_cache.end()) {
                    if (!rw_lock.upgradeToUniqueLock()) {
                        // upgrade to write lock
                        rw_lock.lock();
                        continue;
                    }
                    assert (m_pb_cache.find(key)==m_pb_cache.end());
                    // test for the new root block requested
                    it = m_pb_cache.emplace(key, std::unique_ptr<ptr_block>()).first;
                    bool is_root;
                    if (keyExist(key, is_root)) {
                        // open existing root block
                        if (is_root) {
                            // V-Space reference existing block
                            it->second = std::make_unique<ptr_block>(this->myPtr((*this)->m_ptr_root));
                        } else {
                            // query for existing parent block
                            const ptr_block &parent_block = getPtrBlock(getParentKey(key.first + 1,key),rw_lock);
                            // V-SPACE reference existing block
                            it->second = std::make_unique<ptr_block>(
                                    this->myPtr(parent_block->getItem(getParentIndex(key))));
                        }
                    } else {
                        // new V-Space instance (pointers container)
                        auto block = std::make_shared<ptr_block>(
                            this->getMemspace(), this->getMemspace().getPageSize()
                            );
                        it->second = block;
                        if (is_root) {
                            // link with the old root block
                            (*block).modify().modifyItem(0) = (*this)->m_ptr_root;
                            // modify root pointer of the b-vector
                            this->modify().m_ptr_root = (*block).getAddress();
                        } else {
                            // bind new block
                            ptr_block &parent_block = createPtrBlock(getParentKey(key.first + 1,key), rw_lock);
                            parent_block.modify().modifyItem(getParentIndex(key)) = (*block).getAddress();
                        }
                    }
                }
                return *(it->second);
            }
        }

        /**
         * Get existing or create new block of pointers, use cache
         */
        ptr_block &createPtrBlock(const b_key &key)
        {
            // query cache first
            progressive_mutex::scoped_read_lock rw_lock(this->m_mutex);
            return createPtrBlock(key, rw_lock);
        }
        
        /**
         * Get block pointer / feed cache
         */
        PtrT getBlockPtr(const b_key &block_key, progressive_mutex::scoped_lock &rw_lock) const 
        {
            int h = height();
            assert (block_key.first < h);
            PtrT ptr = (*this)->m_ptr_root;
            // root block key
            --h;
            b_key key = b_key(h,0);
            while(h > block_key.first) {
                const ptr_block &block = getPtrBlock(ptr, key, rw_lock);
                key = getParentKey(--h, block_key);
                ptr = block->getItem(getParentIndex(key));
            }
            return ptr;
        }

        /**
         * Get existing block of pointers, use cache
         */
        ptr_block &modifyPtrBlock(PtrT ptr, const b_key &block_key, progressive_mutex::scoped_lock &rw_lock) 
        {
            assert(block_key.first > 0);
            assert(rw_lock.isLocked());
            for (;;) {
                auto it = m_pb_cache.find(block_key);
                if (it==m_pb_cache.end()) {
                    if (!rw_lock.upgradeToUniqueLock()) {
                        // upgrade to unique
                        rw_lock.lock();
                        continue;
                    }
                    // feed cache
                    it = m_pb_cache.emplace(
                            block_key,
                            std::unique_ptr<ptr_block>(new ptr_block(this->myPtr(ptr)))).first;
                }
                assert(ptr==(*(it->second)).getAddress());
                return *(it->second);
            }
        }

        /**
         * Get existing block of pointers, use cache
         */
        ptr_block &modifyPtrBlock(PtrT ptr, const b_key &block_key) {
            progressive_mutex::scoped_read_lock rw_lock(this->m_mutex);
            return getPtrBlock(ptr, block_key, rw_lock);
        }

        const ptr_block &getPtrBlock(PtrT ptr, const b_key &block_key) const {
            return ((v_bvector&)(*this)).modifyPtrBlock(ptr, block_key);
        }

        const ptr_block &getPtrBlock(PtrT ptr, const b_key &block_key, progressive_mutex::scoped_lock &rw_lock) const {
            return ((v_bvector&)(*this)).modifyPtrBlock(ptr, block_key, rw_lock);
        }

        const ptr_block &getPtrBlock(const b_key &block_key, progressive_mutex::scoped_lock &rw_lock) const 
        {
            // query cache first
            auto it = m_pb_cache.find(block_key);
            if (it == m_pb_cache.end()) {
                return getPtrBlock(getBlockPtr(block_key, rw_lock), block_key, rw_lock);
            } else {
                return *(it->second);
            }
        }

        const DataBlockType &getDataBlock(const b_key &block_key, progressive_mutex::scoped_lock &lock) const 
        {
            PtrT ptr_block = PtrT();
            for (;;) {
                lock.lock();
                if (m_last_block && m_last_block_key==block_key) {
                    return *m_last_block;
                }
                if (!ptr_block.isValid()) {
                    ptr_block = this->getBlockPtr(block_key, lock);
                }
                if (!lock.upgradeToUniqueLock()) {
                    continue;
                }
                m_last_block = fetchDataBlock(ptr_block, this->getBClass());
                m_last_block_key = block_key;
                return *m_last_block;
            }
        }

        /**
         * Get existing data block, use cache
         * NOTE: always block class-0 will be returned (type cast)
         */
        const DataBlockType &getDataBlock(const b_key &block_key) const 
        {
            assert(block_key.first==0);
            progressive_mutex::scoped_lock lock(this->m_mutex);
            return getDataBlock(block_key, lock);
        }

        std::unique_ptr<DataBlockType> fetchDataBlock(const b_key &block_key) const 
        {
            assert(block_key.first==0);
            PtrT ptr_block;
            {
                progressive_mutex::scoped_read_lock rw_lock(this->m_mutex);
                ptr_block = this->getBlockPtr(block_key, rw_lock);
            }
            return fetchDataBlock(ptr_block, this->getBClass());
        }

        std::unique_ptr<DataBlockType> fetchDataBlock(PtrT ptr_block, size_t b_class) const 
        {
            assert(m_interface_array[b_class].getBClass()==b_class);
            return std::unique_ptr<DataBlockType>(reinterpret_cast<DataBlockType *>(
                m_interface_array[b_class].createNewExistingDataBlock(this->myPtr(ptr_block)))
            );
        }

        /**
         * Destroy existing block of pointers
         */
        void destroyPtrBlock(const b_key &key, progressive_mutex::scoped_lock &rw_lock) 
        {
            assert(key.first > 0);
            assert(rw_lock.isUniqueLocked());
            auto it = m_pb_cache.find(key);
            PtrT ptr_child = PtrT();
            if (it!=m_pb_cache.end()) {
                // VS destroy
                if (key.second==0) {
                    ptr_child = (*(it->second))->getItem(0);
                }
                it->second->destroy();
                // clear from cache
                m_pb_cache.erase(it);
            }
            else {
                ptr_block block(this->myPtr(getBlockPtr(key,rw_lock)));
                if (key.second==0) {
                    ptr_child = block->getItem(0);
                }
                block.destroy();
            }
            // root block being destroyed, update root pointer
            if (key.second==0) {
                assert(key.first==(height() - 1));
                this->modify().m_ptr_root = ptr_child;
            }
            else {
                size_t index = getParentIndex(key);
                if ((index==0) || ((index==1) && (key.first==(height() - 2)))) {
                    destroyPtrBlock(getParentKey(key), rw_lock);
                }
            }
        }

        /**
         * Destroy existing block of pointers
         */
        void destroyPtrBlock(const b_key &key) 
        {
            progressive_mutex::scoped_unique_lock lock(this->m_mutex);
            destroyPtrBlock(key, lock);
        }

        /**
         * Destroy existing data block
         */
        void destroyDataBlock(const b_key &key, progressive_mutex::scoped_lock &rw_lock) 
        {
            assert(key.first==0);
            assert(rw_lock.isUniqueLocked());
            {
                getDataBlock(key, rw_lock);
                (*m_last_block).destroy();
                m_last_block = nullptr;
                m_last_block_key = { 0, 0 };
            }
            if (key.second == 0) {
                // root data block has been destroyed
                this->modify().m_ptr_root = PtrT();
            } else {
                // clear pointer block(s)
                std::size_t index = getParentIndex(key);
                if ((index==0) || ((index==1) && (height()==2))) {
                    assert (this->getBClass() == 0);
                    destroyPtrBlock(getParentKey(key), rw_lock);
                }
            }
        }

        /**
         * Destroy existing data block
         */
        void destroyDataBlock(const b_key &key) 
        {
            progressive_mutex::scoped_unique_lock rw_lock(this->m_mutex);
            destroyDataBlock(key, rw_lock);
        }

        /**
         * Actual number of elements contained
         */
        std::size_t getDataBlockSize(const b_key &block_key) const 
        {
            assert(block_key.first==0);
            auto range = getDataBlockRange(block_key);
            return (std::size_t)std::min(
                (std::uint64_t)(1 << (m_db_shift - this->getBClass())), (size() - range.first)
            );
        }

        std::size_t evaluateBClass(std::uint64_t size) const 
        {
            // NOTE: fixed block always evaluates to 0 (full DP) irrespective of size
            if ((*this)->m_flags[BVectorOptions::FIXED_BLOCK]) {
                return 0;
            }
            std::size_t result = 0;            
            std::uint32_t ref_size = (1 << (m_db_shift - 1));
            while ((ref_size >= size) && (ref_size > 0)) {
                ref_size >>= 1;
                ++result;
            }
            if (size == 0) {
                ++result;
            }
            return result;
        }

    public:
        friend class Builder;

        /**
         * Helper class useful for performing multi-stage operations (e.g. growBy large number of elements)
         */
        class Builder 
        {
        public :

            struct BlockData 
            {
            protected :
                friend class Builder;
                PtrT ptr_block;

            public :
                using iterator = typename DataBlockType::iterator;

                const std::pair<std::uint64_t, std::uint64_t> m_range;
                const b_key key;
                const int b_class;

                BlockData(std::pair<std::uint64_t, std::uint64_t> range, b_key key, int b_class)
                    : ptr_block()
                    , m_range(range)
                    , key(key)
                    , b_class(b_class)
                {
                }

                /**
                 * Checks if data block has been initialized or not
                 */
                bool isNull() const {
                    return ptr_block == PtrT();
                }

                /**
                 * @return number of elements allocated for this block
                 */
                std::size_t size() const {
                    return (m_range.second - m_range.first);
                }

                /**
                 * Bind user constructed data block here (must match actual b_class)
                 * @return pair of iterators which can be used to write data there (begin / end iterators)
                 */
                std::pair<iterator, iterator> setBlock(DataBlockType &block) 
                {
                    ptr_block = block.getAddress();
                    iterator it_begin = block.modify().begin();
                    return std::pair<iterator, iterator>(it_begin, it_begin + size());
                }
            };

        private :
            self_t &m_collection;
            std::map<std::uint64_t, BlockData> m_blocks;

        public :

            Builder(self_t &ref)
                : m_collection(ref)
            {
            }

            ~Builder() 
            {
                // this is to make sure either finish or cancel has been completed
                assert(m_blocks.empty());
                if (!m_blocks.empty()) {
                    THROWF(db0::InternalException) << "v_bvector build should be either finished or canceled";
                }
            }

            /**
             * @param range begin / end index in the block
             * @param k block location key
             * @param b_class block size class
             */
            void addBlock(std::pair<std::uint64_t, std::uint64_t> range, b_key k, int b_class) 
            {
                m_blocks.emplace(std::piecewise_construct,
                    std::forward_as_tuple(range.first),
                    std::forward_as_tuple(range, k, b_class)
                );
            }

            /**
             * Before calling finish build you should iterate over all blocks,
             * create them and possibly write some data
             */
            std::map<std::uint64_t, BlockData> &getBlocks() {
                return m_blocks;
            }

            /**
             * This should be explicitly called to finish build
             */
            void finishBuild() 
            {
                for (auto &p: m_blocks) {
                    const BlockData &d_block = p.second;
                    // block must not be null, user should have created it
                    if (d_block.isNull()) {
                        THROWF(db0::InternalException) << "null block, cannot finish";
                    }
                    // Register block with v_bvector's indexing structure
                    // (this is important to perform in address ascending order)
                    if (m_collection.height(p.first + 1) > 1) {
                        // b_class = 0 means full page (full size) block
                        assert(d_block.b_class==0);
                        ptr_block &block = m_collection.createPtrBlock(m_collection.getParentKey(d_block.key));
                        block.modify().modifyItem(m_collection.getParentIndex(d_block.key)) = d_block.ptr_block;
                    } else {
                        // link as the root node
                        m_collection.modify().m_ptr_root = d_block.ptr_block;
                    }
                    // update collection size after block push_back finalized
                    m_collection.modify().m_size += d_block.size();
                }
                m_blocks.clear();
            }

            /**
             * Revert all operations and prepare to close
             */
            void cancelBuild() {
                m_blocks.clear();
            }
        };

        /**
         * Prepare v_bvector to grow (by adding more blocks), to complete the operation must
         * NOTE: this operation is currently only allowed for empty v_bvector (to initially populate with data)
         * 1) create actual data block (write data there)
         * 2) close builder (must report block addresses back to builder)
         */
        /* FIXME: out of service
        void preparePushBack(std::uint64_t count, Builder &builder)
        {
            assert(count > 0);
            assert(empty() && "currently preparePushBack is only supported for empty v_bvector");        
            // evaluate / modify B-CLASS
            this->m_b_class = evaluateBClass(count);
            std::uint64_t current_size = size();
            while (count > 0) {
                // grow by adding new block(s), last block will be smaller size
                uint64_t diff = std::min(count, (std::uint64_t)(1 << (m_db_shift - m_b_class)));
                preparePushBlock(builder, diff, current_size);
                count -= diff;
                // update current size but do not change actual collection size just yet
                current_size += diff;
            }
        }
        */

        /**
         * Performs part of push block operation and allow to complete this later
         * what is performed: collect block information (key, index range)
         * what should be performed to complete: 1) create such data block,
         * 2) bind with data structure (this part of operation is performed by builder)
         * @param size v_bvector size at the moment of adding block
         */
        /* FIXME: out of service
        void preparePushBlock(Builder &builder, std::uint64_t block_size, std::uint64_t size) 
        {
            std::uint64_t index = size;
            auto full_range = getDataBlockRange(index);
            assert((full_range.second - full_range.first) >= block_size);
            std::pair<std::uint64_t, std::uint64_t> range(full_range.first, full_range.first + block_size);
            assert(range.first==index);
            // h = 0 means data block (0 height)
            b_key key = getKey(0, index);
            // add block related information
            builder.addBlock(range, key, this->m_b_class);
        }
        */
        
        /**
         * Feed vector with "count" items (grow it)
         */
        void growBy(unsigned int count)
        {
            assert(count > 0);
            // pad current block with data
            bool is_empty = empty();
            if (is_empty) {
                // evaluate / modify B-CLASS
                this->m_b_class = evaluateBClass(count);
            } else {
                auto range = getDataBlockRange(size() - 1);
                auto diff = std::min(count, (unsigned int)(range.second - size()));
                count -= diff;
                this->modify().m_size += diff;
            }
            while (count > 0) {
                // Grow data block (by class upgrade)
                auto b_class = this->getBClass();
                if (!is_empty && (b_class > 0)) {
                    assert(height() < 2);
                    // destination class
                    growBlock(evaluateBClass(size() + count));
                    // always first block
                    auto range = getDataBlockRange(0);
                    auto diff = std::min(count, (unsigned int)(range.second - size()));
                    count -= diff;
                    this->modify().m_size += diff;
                } else {
                    // grow by adding new block(s)
                    push_block();
                    auto diff = std::min(count, (unsigned int)(1 << (m_db_shift - b_class)));
                    count -= diff;
                    this->modify().m_size += diff;
                    is_empty = false;
                }
            }
        }

        class const_iterator 
        {
        public :
            using value_type = ItemT;
            using difference_type = std::ptrdiff_t;
            using pointer = ItemT*;
            using reference = ItemT&;
            using iterator_category = std::random_access_iterator_tag;

            const_iterator() = default;

            const_iterator(const v_bvector &ref, std::uint64_t index)
                : m_collection_ptr(&ref)
                , m_index(index)
            {
                assert(m_index <= m_collection_ptr->size());
                // avoid unnecessary initializing end iterator
                if (!is_end()) {
                    initDataBlock();
                }
            }
            
            const_iterator(const const_iterator& other) = default;
            
            const ItemT *operator->() const {
                return this->m_item_ptr;
            }

            const ItemT &operator*() const {
                return *(this->m_item_ptr);
            }
            
            const ItemT &operator[](std::uint64_t p_index) const
            {
                if (isInVectorRange(p_index)) {
                    if (isInCurrentBlock(p_index)) {
                        auto diff = p_index - m_index;
                        return *(this->m_item_ptr + diff);
                    } else {
                        const auto &block = m_collection_ptr->getDataBlock(m_index);
                        return *(&block->getItem((m_index & m_collection_ptr->m_db_mask)));
                    }
                } else {
                    throw std::invalid_argument("Index out of bounds");
                }
            }

            /**
             * Moves iterator to specific index
             * @param p_index New index
             * @return true if move was successfull
             */
            bool moveTo(std::uint64_t p_index)
            {
                m_index = p_index;
                if (!isInVectorRange(m_index)) {
                    return false;
                }
                if (isInCurrentBlock(m_index)) {
                    m_item_ptr = &m_current_block->getItem((m_index & m_collection_ptr->m_db_mask));
                }
                else {
                    initDataBlock();
                }
                return true;
            }

            bool isInCurrentBlock(std::uint64_t index) const {
                return (index < m_range.second && index >= m_range.first);
            }

            bool isInVectorRange(std::uint64_t index) const {
                return index < m_collection_ptr->size();
            }

            const_iterator& operator++()
            {
                assert(!is_end());
                ++m_index;
                ++m_item_ptr;
                if (!isInCurrentBlock(m_index) && isInVectorRange(m_index)) {
                    initDataBlock();
                }
                return *this;
            }

            const_iterator& operator--()
            {
                --m_index;
                --m_item_ptr;
                if (!isInCurrentBlock(m_index) && isInVectorRange(m_index)) {
                    initDataBlock();
                }
                return *this;
            }
            
            bool is_end() const 
            {
                assert(m_index <= m_collection_ptr->size());
                return (m_index == m_collection_ptr->size());
            }

            /**
             * Span (number of items)
             */
            std::ptrdiff_t operator-(const const_iterator &it) const {
                return (this->m_index - it.m_index);
            }

            const_iterator& operator+=(std::ptrdiff_t offset)
            {
                m_index += offset;
                assert(m_index <= m_collection_ptr->size());
                m_item_ptr += (size_t)offset;
                if (!isInCurrentBlock(m_index) && isInVectorRange(m_index)) {
                    initDataBlock();
                }
                return *this;
            }

            const_iterator& operator-=(std::ptrdiff_t offset) 
            {
                m_index -= offset;
                m_item_ptr -= (size_t) offset;

                if (!isInCurrentBlock(m_index) && isInVectorRange(m_index)) {
                    initDataBlock();
                }
                return *this;
            }

            const_iterator operator+(std::uint64_t offset) const
            {
                const_iterator it = (*this);
                it += offset;
                return it;
            }

            const_iterator operator-(ptrdiff_t offset) const
            {
                const_iterator it = (*this);
                it -= offset;
                return it;
            }

            bool operator==(const const_iterator &it) const {
                return (m_index==it.m_index);
            }

            bool operator!=(const const_iterator &it) const {
                return (m_index!=it.m_index);
            }

            bool operator<(const const_iterator &it) const {
                return (m_index<it.m_index);
            }

            bool operator>(const const_iterator &it) const {
                return (m_index>it.m_index);
            }

            bool operator<=(const const_iterator &it) const {
                return (m_index<=it.m_index);
            }

            bool operator>=(const const_iterator &it) const {
                return (m_index>=it.m_index);
            }

            std::uint64_t getIndex() const {
                return this->m_index;
            }

        protected:
            const v_bvector *m_collection_ptr = nullptr;
            // data block reference (block to contain current item)
            DataBlockType m_current_block;
            // begin / end index in current block
            std::pair<std::uint64_t, std::uint64_t> m_range;
            // current index
            std::uint64_t m_index = 0;
            // current item
            const ItemT *m_item_ptr = nullptr;

            void initDataBlock()
            {
                m_range = m_collection_ptr->getDataBlockRange(m_index);
                m_range.second = std::min(m_range.second, m_collection_ptr->size());
                // request block containing specific item / index
                m_current_block = *m_collection_ptr->fetchDataBlock(m_index);
                m_item_ptr = &m_current_block->getItem((m_index & m_collection_ptr->m_db_mask));
            }
        };

    private:
        /**
         * Fetch data block containing specific item from backend
         */
        std::unique_ptr<DataBlockType> fetchDataBlock(uint64_t index) const {
            return fetchDataBlock(this->getKey(0, index));
        }

    public :

        const_iterator begin(uint64_t index = 0) const {
            return const_iterator(*this,index);
        }

        const_iterator end() const {
            return const_iterator(*this, size());
        }
        
        class iterator : public const_iterator
        {
        public :
            iterator(const v_bvector &ref, std::uint64_t index)
                : const_iterator(ref, index)
            {
            }
                        
            // Allows to selectively modify items in iterated sequence
            ItemT &modifyItem()
            {
                auto *data_block_ptr = this->m_current_block.getData();
                if (&this->m_current_block.modify() != data_block_ptr) {
                    // since data block has been modified, rebind item pointer
                    this->m_item_ptr = &data_block_ptr->getItem((this->m_index & this->m_collection_ptr->m_db_mask));
                }
                return *const_cast<ItemT*>(this->m_item_ptr);
            }
        };

        iterator begin(std::uint64_t index = 0) {
            return iterator(*this, index);
        }

        iterator end() {
            return iterator(*this, size());
        }
        
        const_iterator cbegin(std::uint64_t index = 0) const {
            return const_iterator(*this, index);
        }

        const_iterator cend() const {
            return const_iterator(*this, size());
        }

        /**
         * Visit all data blocks, run arbitrary visitor function
         * @param range range of the elements contained in this block (begin / end index)
         */
        void forAll(std::function<void(const DataBlockOverlaidType &, std::pair<std::uint64_t, std::uint64_t> range)> f) const
        {
            auto key = getKey(0,0);
            std::uint64_t index = 0;
            std::uint64_t end_index = this->size();
            std::size_t block_size = 1u << (m_db_shift - this->getBClass());
            while (index!=end_index) {
                auto block = fetchDataBlock(key);
                std::uint64_t this_block_end_index = std::min(end_index, index + block_size);
                f(*(*block).getData(), {index, this_block_end_index});
                ++key.second;
                index = this_block_end_index;
            }
        }

    private:
#ifndef NDEBUG
        static std::map<std::pair<const Memspace*, Address>, int> m_instance_log;

        // the code to detect multiple v_bvector instances (not allowed)
        void __add(bool move = false) {
            auto key = std::make_pair(&this->getMemspace(), this->getAddress());
            auto it = m_instance_log.find(key);
            if (it == m_instance_log.end()) {
                m_instance_log[key] = 1;
            } else {
                assert(move && it->second == 1);
                it->second++;
            }
        }

        void __remove() {
            if (!this->getAddress().isValid()) {
                return;
            }
            auto key = std::make_pair(&this->getMemspace(), this->getAddress());
            auto it = m_instance_log.find(key);
            if (it == m_instance_log.end()) {
                assert(false);
            } else {
                it->second--;
                if (it->second == 0) {
                    m_instance_log.erase(it);
                }
            }
        }
#endif
    };
    
    template <typename ItemT, typename PtrT>
    const DataBlockInterfaceArray<ItemT, 13u> v_bvector<ItemT, PtrT>::m_interface_array;
    
#ifndef NDEBUG    
    template <typename ItemT, typename PtrT>
    std::map<std::pair<const Memspace*, Address>, int> v_bvector<ItemT, PtrT>::m_instance_log;
#endif

    template <typename PtrT>
    o_bvector<PtrT>::o_bvector(std::uint32_t page_size_hint, BVectorFlags flags)
        : m_page_size(page_size_hint)
        , m_flags(flags)    
    {
    }        
    
    template <typename PtrT>
    std::size_t o_bvector<PtrT>::measure(std::uint32_t page_size_hint, BVectorFlags flags)
    {
        // size aligned to 1 DP
        if (flags[BVectorOptions::FIXED_BLOCK]) {
            return page_size_hint;
        } else {
            // actual size of members
            return super_t::measureMembers();
        }
    }
    
    template <typename PtrT>
    template <typename buf_t> 
    std::size_t o_bvector<PtrT>::safeSizeOf(buf_t buf)
    {
        auto _buf = buf;
        _buf += super_t::baseSize();
        auto &self = o_bvector::__const_ref(buf);
        // size aligned to 1 DP
        if (self.m_flags[BVectorOptions::FIXED_BLOCK]) {
            buf += self.m_page_size;
            return self.m_page_size;
        } else {
            // actual size of members
            return _buf - buf;
        }
    }

}
