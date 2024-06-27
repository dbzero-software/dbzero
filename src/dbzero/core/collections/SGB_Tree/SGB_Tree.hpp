#pragma once

#include <functional>
#include "sgb_tree_node.hpp"
#include "sgb_types.hpp"

namespace db0

{

    template <typename SGB_Types> class SGB_TreeBase: protected SGB_Types::SG_TreeT
    {
    protected:
        using TypesT = SGB_Types;
        using super_t = typename TypesT::SG_TreeT;

    public:
        using ItemT = typename TypesT::ItemT;
        using ItemCompT = typename TypesT::ItemCompT;
        using CompT = typename TypesT::CompT;
        using AddressT = typename TypesT::AddressT;
        using NodeT = typename TypesT::NodeT;
        using NodePtrT = typename NodeT::ptr_t;
        using node_iterator = typename TypesT::o_sgb_node_t::iterator;
        using node_const_iterator = typename TypesT::o_sgb_node_t::const_iterator;
        using sg_tree_const_iterator = typename super_t::const_iterator;
        using sgb_node_const_sorting_iterator = typename TypesT::o_sgb_node_t::const_sorting_iterator;

        struct ItemIterator: std::pair<node_iterator, sg_tree_const_iterator>
        {
            using parent_t = std::pair<node_iterator, sg_tree_const_iterator>;

            ItemIterator()
                : parent_t(nullptr, {})
            {
            }

            ItemIterator(node_iterator node_it, const sg_tree_const_iterator &tree_it)
                : parent_t(node_it, tree_it)
            {
            }            

            inline operator bool() const {
                return this->first != nullptr;
            }
        };
        
        struct ConstItemIterator: std::pair<node_const_iterator, sg_tree_const_iterator>
        {
            using parent_t = std::pair<node_const_iterator, sg_tree_const_iterator>;

            ConstItemIterator()
                : parent_t(nullptr, {})
            {
            }

            ConstItemIterator(node_const_iterator node_it, const sg_tree_const_iterator &tree_it)
                : parent_t(node_it, tree_it)
            {
            }            

            ConstItemIterator(const ItemIterator &it)
                : parent_t(it.first, it.second)
            {
            }

            inline operator bool() const {
                return this->first != nullptr;
            }
            
            inline unsigned int getIndex() const {
                return this->first - this->second->cbegin();
            }

#ifndef NDEBUG
            bool validate() const {
                assert(this->first == nullptr || (this->first >= this->second->cbegin() && this->first < this->second->cend()));
                return true;
            }
#endif            
        };
        
        /**
         * Creates the 'head' node with a given capacity
         * 
         * @param capacity default capacity of a single node
         * @tparam args optional arguments for the header's constructor
         */
        SGB_TreeBase(Memspace &memspace, std::size_t node_capacity)
            : super_t(memspace, CompT(), node_capacity)
            , m_node_capacity(node_capacity)
        {
        }
        
        SGB_TreeBase(mptr ptr, std::size_t node_capacity)
            : super_t(ptr, CompT())
            , m_node_capacity(node_capacity)
        {
        }
        
        /**
         * Get address of this object
        */
        std::uint64_t getAddress() const {
            return super_t::getAddress();
        }
        
        void insert(const ItemT &item) {
            emplace(item);
        }

        /**
         * Emplace (for update)
        */
        template <typename... Args> ItemIterator emplace(Args&&... args)
        {
            if (super_t::empty()) {
                return this->emplace_to_empty(std::forward<Args>(args)...);
            }

            super_t::modify().m_sgb_size++;
            // assume key is the first element in the args pack
            // finding an existing node which can hold the new item
            auto node = super_t::lower_equal_bound(std::get<0>(std::make_tuple(args...)));
            if (node == super_t::end()) {
                node = super_t::begin();
            }

            if (node->isFull()) {
                // erase the max element and create the new node
                auto max_item_ptr = node->find_max();
                auto new_node = super_t::insert_equal(*max_item_ptr, m_node_capacity);
                node.modify().erase_existing(max_item_ptr);
                // rebalance the nodes
                node.modify().rebalance(new_node.modify());
                // append to either of the nodes
                if (!ItemCompT()(ItemT(std::forward<Args>(args)...), new_node->keyItem())) {
                    return { new_node.modify().append(std::forward<Args>(args)...), new_node }; 
                }
            }
            return { node.modify().append(std::forward<Args>(args)...), node };
        }

        /**
         * Remove element from the collection if it exists
         * 
         * @return true if the element was removed
        */
        template <typename... Args> std::pair<bool, NodeT> erase_equal(Args&&... args)
        {
            // finding an existing node which may contain the element
            auto node = super_t::lower_equal_bound(std::forward<Args>(args)...);
            if (node == super_t::end()) {
                return { false, NodeT() };
            }
            if (!node.modify().erase(std::forward<Args>(args)...)) {
                return { false, NodeT() };
            }
            super_t::modify().m_sgb_size--;
            if (node->empty()) {
                // delete the entire node
                super_t::erase(node);
                return { true, NodeT() };
            }
            return { true, node };
        }
        
        /**
         * Remove element identified by node + within node iterator
         * item gets invalidated and cannot be used
        */
        void erase(ConstItemIterator &item)
        {
            assert(item.validate());
            super_t::modify().m_sgb_size--;
            auto index = item.second->indexOf(item.first);
            // erase by index since item pointer gets modified (due to CoW)
            if (const_cast<sg_tree_const_iterator &>(item.second).modify().erase_existing(index)) {
                // delete the entire node
                super_t::erase(const_cast<sg_tree_const_iterator &>(item.second));
            }
#ifndef NDEBUG
            item.first = nullptr;
#endif                        
        }

        class const_iterator
        {
        public:
            const_iterator(const sg_tree_const_iterator &tree_it, const sg_tree_const_iterator &tree_end)
                : m_tree_it(tree_it)
                , m_tree_end(tree_end)
            {
                if (m_tree_it != m_tree_end) {
                    m_node_it = m_tree_it->cbegin_sorted();                    
                }
            }
            
            const_iterator &operator++()
            {                
                assert(!is_end());
                ++m_node_it;
                if (m_node_it.is_end()) {
                    ++m_tree_it;
                    if (m_tree_it != m_tree_end) {
                        m_node_it = m_tree_it->cbegin_sorted();
                    }
                }
                return *this;
            }

            ItemT operator*() const {
                return *m_node_it;
            }

            const ItemT *operator->() const {
                return m_node_it.get_ptr();
            }

            bool is_end() const {
                return m_tree_it == m_tree_end || m_node_it.is_end();
            }

        private:
            sg_tree_const_iterator m_tree_it, m_tree_end;
            sgb_node_const_sorting_iterator m_node_it;
        };

        const_iterator cbegin() const {
            return const_iterator(super_t::begin(), super_t::end());
        }

        /// Partially sorted iterator (nodes are sorted but individual elements are only heap-ordered)
        class unsorted_const_iterator
        {
        public:
            // move to first or next node, return false if at end
            using NextF = std::function<bool(sg_tree_const_iterator &)>;

            unsorted_const_iterator(const sg_tree_const_iterator &tree_it, NextF next, bool reversed = false)
                : m_item_it(nullptr, tree_it)
                , m_next(next)
                , m_reversed(reversed)
            {
                if (m_next(m_item_it.second)) {
                    if (m_reversed) {
                        m_item_it.first = m_item_it.second->cend();
                        m_node_end = m_item_it.second->cbegin();
                        --m_item_it.first;
                    } else {
                        m_item_it.first = m_item_it.second->cbegin();
                        m_node_end = m_item_it.second->cend();
                    }
                } else {
                    m_is_end = true;
                }
            }

            unsorted_const_iterator &operator++()
            {
                assert(!is_end());
                if (!m_reversed) {
                    ++m_item_it.first;
                }
                if (m_item_it.first == m_node_end) {
                    if (!m_next(m_item_it.second)) {
                        m_is_end = true;
                        return *this;
                    }
                    if (m_reversed) {
                        m_item_it.first = m_item_it.second->cend();
                        m_node_end = m_item_it.second->cbegin();                         
                    } else {
                        m_item_it.first = m_item_it.second->cbegin();
                        m_node_end = m_item_it.second->cend();
                    }
                }
                if (m_reversed) {
                    --m_item_it.first;
                }
                return *this;
            }

            ItemT operator*() const {
                return *m_item_it.first;
            }

            const ItemT *operator->() const {
                return m_item_it.first.get_ptr();
            }
            
            bool is_end() const {
                return m_is_end;
            }
            
            inline const ConstItemIterator &get() const {
                return m_item_it;
            }

            /**
             * Retrieve the underlying item iterator for modification
             * any external modification invalidates 'this' instance
            */
            ConstItemIterator &getMutable() {
                return m_item_it;
            }

            sg_tree_const_iterator &node() {
                return m_item_it.second;
            }

            const sg_tree_const_iterator &node() const {
                return m_item_it.second;
            }

        protected:
            friend SGB_TreeBase;
            ConstItemIterator m_item_it;
            node_const_iterator m_node_end;

            unsorted_const_iterator(const ConstItemIterator &item_it, NextF next, bool reversed, bool is_end)
                : m_item_it(item_it)
                , m_next(next)
                , m_is_end(is_end)
                , m_reversed(reversed)                
            {
                if (!is_end) {
                    if (m_reversed) {
                        m_node_end = m_item_it.second->cbegin();
                    } else {
                        m_node_end = m_item_it.second->cend();
                    }
                }
            }

        private:
            NextF m_next;
            bool m_is_end = false;
            bool m_reversed;
        };
        
        void erase(unsorted_const_iterator &item)
        {
            assert(!item.is_end());
            super_t::modify().m_sgb_size--;
            if (item.m_item_it.second.modify().erase_existing(item.m_item_it.first)) {
                // delete the entire node
                super_t::erase(item.m_item_it.second);
            }
        }
        
        /// Create reversed, partially sorted iterator (nodes are sorted but individual elements are only heap-ordered)
        unsorted_const_iterator rbegin_unsorted() const
        {            
            return unsorted_const_iterator(super_t::end(), [this](sg_tree_const_iterator &it) {
                if (it == super_t::begin()) {
                    return false;
                }
                --it;
                return true;
            }, true);
        }

        /**
         * Returns the largest element which is not greater than the given key
         * 
         * @return iterator pair or nullptr (first) if not found
        */
        template <typename KeyT> ConstItemIterator lower_equal_bound(const KeyT &key) const
        {
            auto node = super_t::lower_equal_bound(key);
            if (node == super_t::end()) {
                return { nullptr, sg_tree_const_iterator() };
            }
            return { node->lower_equal_bound(key), node };
        }

        template <typename KeyT> ConstItemIterator find_equal(const KeyT &key) const
        {
            auto node = super_t::lower_equal_bound(key);
            if (node == super_t::end()) {
                return { nullptr, sg_tree_const_iterator() };
            }
            return { node->find_equal(key), node };
        }

        template <typename KeyT> ConstItemIterator upper_equal_bound(const KeyT &key) const
        {
            if (super_t::empty()) {
                return { nullptr, sg_tree_const_iterator() };
            }
            auto node = super_t::upper_equal_bound(key);
            if (node == super_t::end()) {
                node = super_t::begin();
            } else {
                if (node->keyEqual(key) || node == super_t::begin()) {
                    return { node->begin(), node };
                }
                // find value in the preceeding node                
                --node;                
            }
            return { node->upper_equal_bound(key), node };
        }

        /**
         * Retrieve the unordered (partially sorted) slice of elements starting from the "first" element
         */
        unsorted_const_iterator upper_slice(const ConstItemIterator &first) const
        {
            auto next_ = [this](sg_tree_const_iterator &it) -> bool {
                ++it;
                return it != super_t::end();
            };
            
            return unsorted_const_iterator(first, next_, false, !first.first);
        }

        /**
         * Try retrieving the max element from the collection
        */
        ConstItemIterator find_max() const
        {
            if (super_t::empty()) {
                return { nullptr, sg_tree_const_iterator() };
            }
            auto node = super_t::end();
            --node;
            return { node->find_max(), node };
        }

        /**
         * A helper function to retrieve the mutable item's iterator from the const iterator
        */
        static node_iterator modify(ConstItemIterator &item)
        {
            auto item_index = item.getIndex();
            // note that the unerlying buffer might've changed on modify
            item.first = &item.second.modify().at(item_index);
            return const_cast<node_iterator>(item.first);
        }

        using WindowT = std::array<ConstItemIterator, 3>;

        /**
         * A helper function to modify the central element from the window
         * and correct the neighoring elements if affected
        */
        static node_iterator modify(WindowT &item)
        {            
            unsigned int index_0 = (item[0].second == item[1].second) ? item[0].getIndex() : std::numeric_limits<unsigned int>::max();
            unsigned int index_2 = (item[2].second == item[1].second) ? item[2].getIndex() : std::numeric_limits<unsigned int>::max();            
            auto result = modify(item[1]);
            if (index_0 != std::numeric_limits<unsigned int>::max()) {
                item[0].second = item[1].second;
                item[0].first = &item[0].second->at(index_0);
            }
            if (index_2 != std::numeric_limits<unsigned int>::max()) {
                item[2].second = item[1].second;
                item[2].first = &item[2].second->at(index_2);
            }            
            return result;
        }

        AddressT size() const {
            return super_t::getData()->m_sgb_size;
        }

        bool empty() const {
            return super_t::empty();
        }
        
        /**
         * Finds a lower-equal element and +/- 1 neighbor window
         * the lower-equal element is always the middle one (index = 1)
         * Note that either of the elements may be nullptr (non existing)
         * 
         * @return true if at least one element was found (@index = 1)
        */
        template <typename KeyT> bool lower_equal_window(const KeyT &key, WindowT &result) const
        {
            auto node = super_t::lower_equal_bound(key);
            if (node == super_t::end()) {
                result[0].first = result[1].first = result[2].first = nullptr;
                // nothing found
                return false;
            }
            
            result[1].second = node;
            result[1].first = node->lower_equal_window(key, result[0].first, result[2].first);
            if (!result[0].first) {
                // try finding in the preceeding SG-node
                if (node != super_t::begin()) {
                    auto it = node;
                    --it;
                    result[0] = { it->find_max(), it };
                }
            }
            
            if (!result[2].first) {
                // try finding in the succeeding SG-node
                auto it = node;
                ++it;
                if (it != super_t::end()) {
                    result[2] = { it->find_min(), it };
                }
            }
            return true;
        }

        sg_tree_const_iterator cbegin_nodes() const {
            return super_t::begin();
        }

        sg_tree_const_iterator cend_nodes() const {
            return super_t::end();
        }

        void commit() {
            super_t::commit();
        }
        
        void detach() {
            super_t::detach();
        }

    protected:
        const std::size_t m_node_capacity;

        template <typename... Args> ItemIterator emplace_to_empty(Args&&... args)
        {
            super_t::modify().m_sgb_size++;
            // create the root node which shares the same allocation as the 'head' node
            // obtain mutable mem lock first
            auto mem_lock = this->get_v_ptr().modifyMappedRange();            
            // calculate residual capacity
            auto residual_capacity = (*this)->sizeOf() - (*this)->trueSizeOf();            
            // use the remaining capacity to initialize the root node
            auto offset = (*this)->trueSizeOf();
            // a mapped root address
            auto root_address = MappedAddress { this->getAddress() + offset, mem_lock.getSubrange(offset) };
            // create from the mapped address (no new alloc required)
            auto new_node = super_t::insert_equal(ItemT(std::forward<Args>(args)...), residual_capacity, std::move(root_address));
            return { new_node->begin(), new_node };
        }
                
    };

    /**
     * @tparam ItemT The type of the item to be stored in the tree
     * @tparam ItemCompT The type of the comparator to be used for item comparison
     * @tparam ItemEqualT The type of the comparator to be used for item equality
     * @tparam CapacityT The type of the capacity to be used for the tree
     * @tparam AddressT The type of the address to be used for the tree
     * @tparam HeaderT optional fixed-size header to be associated with each node / block (default is none / null), helpful when implementing extensions
    */
    template <typename ItemT, typename ItemCompT = std::less<ItemT>, typename ItemEqualT = std::equal_to<ItemT>,
        typename CapacityT = std::uint16_t, typename AddressT = std::uint32_t, typename HeaderT = o_null, typename TreeHeaderT = o_null>
    class SGB_Tree: public SGB_TreeBase<sgb_types<ItemT, ItemCompT, ItemEqualT, CapacityT, AddressT, HeaderT, TreeHeaderT> >
    {
        using super_t = SGB_TreeBase<sgb_types<ItemT, ItemCompT, ItemEqualT, CapacityT, AddressT, HeaderT, TreeHeaderT> >;
    public:
        SGB_Tree(Memspace &memspace, std::size_t node_capacity)
            : super_t(memspace, node_capacity)
        {
        }
        
        SGB_Tree(mptr ptr, std::size_t node_capacity)
            : super_t(ptr, node_capacity)
        {
        }
    };

}