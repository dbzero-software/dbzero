// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/memory/mptr.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/intrusive/sgtree.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
    
    // SG-Tree node / head node pointers
DB0_PACKED_BEGIN
    template <class PtrT = Address> struct DB0_PACKED_ATTR tree_ptr_set
    {        
        PtrT parent = {};
        PtrT left = {};
        PtrT right = {};

        tree_ptr_set() = default;

        inline PtrT getLeft() const {
            return left;
        }

        inline PtrT getRight() const {
            return right;
        }

        inline PtrT getParent() const {
            return parent;
        }
    };
DB0_PACKED_END
    
    /**
     * Base container for SG-Tree nodes
     */
DB0_PACKED_BEGIN
    template <class ptr_set_t = tree_ptr_set<Address> > struct DB0_PACKED_ATTR sg_node_base
        : public o_fixed<sg_node_base<ptr_set_t> >
    {
    public :
        typedef o_fixed<sg_node_base<ptr_set_t> > super_t;        
        ptr_set_t ptr_set;

        sg_node_base(const sg_node_base &other)
            : ptr_set(other.ptr_set)
        {
        }

        sg_node_base() = default;

        const ptr_set_t &getPointers() const;
    };

DB0_PACKED_END

    template <class ptr_set_t>
    const ptr_set_t &sg_node_base<ptr_set_t>::getPointers() const{
        return ptr_set;
    }

    /**
     * NOTICE: sg_tree object is the head node at the same time
     */
DB0_PACKED_BEGIN
    template <std::size_t match_size = 0, class ptr_set_t = tree_ptr_set<Address> > struct DB0_PACKED_ATTR sg_tree_data
        : public o_fixed<sg_tree_data<match_size, ptr_set_t> >
    {
    public:
        typedef o_fixed<sg_tree_data<match_size, ptr_set_t> > super_t;
        
        ptr_set_t ptr_set;
        std::uint32_t size = 0;
        std::uint32_t max_tree_size = 0;
        // TODO: Unhack this
        // \/\/\/ This is to ensure head node has the same size as other nodes,
        //        which is needed to avoid problems caused by v_sgtree::head()
        //        casting sg_tree_data to node_t, which then attempts to lock
        //        more memory than it has allocated.
        static constexpr size_t size_until_now = true_size_of<ptr_set_t>() + true_size_of<std::uint32_t>() * 2;
        std::byte _padding[match_size > size_until_now ? match_size - size_until_now : 1];

        sg_tree_data() = default;

        const ptr_set_t &getPointers() const;
        std::uint32_t getSize() const;
        std::uint32_t getMaxTreeSize() const;
    };
DB0_PACKED_END
    
    template <size_t match_size, class ptr_set_t>
    const ptr_set_t &sg_tree_data<match_size, ptr_set_t>::getPointers() const{
        return ptr_set;
    }

    template <size_t match_size, class ptr_set_t>
    std::uint32_t sg_tree_data<match_size, ptr_set_t>::getSize() const{
        return size;
    }

    template <size_t match_size, class ptr_set_t>
    std::uint32_t sg_tree_data<match_size, ptr_set_t>::getMaxTreeSize() const{
        return max_tree_size;
    }
    
    /**
     * node_t - intrusive node type (intrusive_node derived)
     * must provide following dependent types :
     * traits_t - intrusive NodeTraits compatible type
     * comp_t - node ptr comparer
     */
    template <class node_t, class alpha_t = intrusive::detail::h_alpha_sqrt2_t> class v_sgtree
        : public node_t::tree_base_t
    {
    public:
        using super = typename node_t::tree_base_t;
        using c_type = typename super::ContainerT;
        using comp_t = typename node_t::comp_t;
        using node_ptr_t = typename node_t::ptr_t;
        using ptr_t = typename super::ptr_t;

        v_sgtree() = default;

        /// The SG tree instance object is the 'head' node (created with either default or user arguments)
        template <typename... Args> v_sgtree(db0::Memspace &memspace, comp_t cmp = comp_t(), Args&&... args)
            : super(memspace, std::forward<Args>(args)...)
            , _comp(cmp)
        {
            // link to self
            this->modify().ptr_set.left = this->getAddress();
            this->modify().ptr_set.right = this->getAddress();
        }

        v_sgtree(const ptr_t &ptr, comp_t cmp = comp_t())
            : super(ptr)
            , _comp(cmp)
        {
        }

        v_sgtree(db0::mptr _ptr, comp_t cmp = comp_t())
            : super(_ptr)
            , _comp(cmp)
        {
        }
        
        v_sgtree(db0::Memspace &memspace, const v_sgtree &other)
            : v_sgtree(memspace)
        {
            for (auto it = other.begin(), end = other.end();it != end; ++it) {
                node_t new_node(typename node_t::tag_copy(), memspace, other.getMemspace(), it);
                SG_Tree::insert_equal_upper_bound(
                    this->head(), new_node, this->_comp, this->modify().size++, _alpha
                );
            }
            this->updateMaxTreeSize();
        }
        
        /**
         * Create new, empty V-Space instance of the SG-Tree, no comparator required
         * @return address of the created instance
         */
        static Address createNew(db0::Memspace &memspace)
        {
            super sg_tree(memspace);
            // link to self
            sg_tree.modify().ptr_set.left = sg_tree.getAddress();
            sg_tree.modify().ptr_set.right = sg_tree.getAddress();
            return sg_tree.getAddress();
        }
        
        class iterator : public node_ptr_t
        {
        public :
            iterator() = default;

            iterator(Memspace &memspace, Address address, MemLock &&mem_lock)
                : node_ptr_t(memspace, address, std::move(mem_lock))
            {
            }

            iterator(const node_ptr_t &ptr)
                : node_ptr_t(ptr)
            {
            }

            iterator(db0::mptr ptr)
                : node_ptr_t(ptr)
            {
            }

            iterator operator++(int)
            {
                iterator result = (*this);
                (*this) = _Tree::next_node(*this);
                return result;
            }

            iterator &operator++()
            {
                (*this) = _Tree::next_node(*this);
                return *this;
            }

            iterator &operator--()
            {
                (*this) = _Tree::prev_node(*this);
                return *this;
            }

            iterator operator--(int)
            {
                iterator result = (*this);
                (*this) = _Tree::prev_node(*this);
                return result;
            }
        };

        // stl compatibility
        using const_iterator = iterator;

        iterator begin() {
            return _Tree::begin_node(node_ptr_t(*this));
        }

        iterator end() {
            return _Tree::end_node(node_ptr_t(*this));
        }

        iterator begin() const {            
            return _Tree::begin_node(node_ptr_t(*this));
        }

        iterator end() const {
            return _Tree::end_node(node_ptr_t(*this));
        }

        bool empty() const {
            return (_Tree::begin_node(node_ptr_t(*this))) == _Tree::end_node(node_ptr_t(*this));
        }
        
        // This method allows constructing an iterator from a previously saved address
        iterator beginFromAddress(Address address) const {
            return node_ptr_t(this->getMemspace(), address);
        }

        std::uint32_t size() const {
            return (*this)->size;
        }

        template <typename... Args> std::pair<iterator, bool> emplace(Args&&... args){
            return insert_unique(std::forward<Args>(args)...);
        }
        
        template <typename... Args> std::pair<iterator, bool> emplace(const std::string &key, Args&&... args){
            return insert_unique(key.c_str(),std::forward<Args>(args)...);
        }

        /**
         * KeyInitializer - node key initializer type
         * args - data initializers
         */
        template <typename KeyInitializer, typename... Args>
        iterator insert_equal(const KeyInitializer &key, Args&&... args)
        {
            std::size_t depth;
            link_data ld;
            SG_Tree::link_equal_upper_bound(
                this->head(), key, this->_comp, ld, depth
            );
            node_t new_node(this->getMemspace(), key, std::forward<Args>(args)...);
            SG_Tree::link(this->head(), new_node, ld);
            SG_Tree::rebalance_after_insertion(new_node, depth, this->modify().size++, _alpha);
            this->updateMaxTreeSize();
            return new_node;
        }

        /**
         * hint - hint node to speedup insert
         */
        template <class KeyInitializer, typename... Args> iterator insert_equal(
            iterator hint, const KeyInitializer &key, Args&&... args)
        {
            std::size_t depth;
            link_data ld;
            SG_Tree::link_equal (
                this->head(), hint, key, this->_comp, ld, depth
            );
            node_t new_node(this->getMemspace(), key, std::forward<Args>(args)...);
            SG_Tree::link(this->head(), new_node, ld);
            SG_Tree::rebalance_after_insertion(new_node, depth, ++this->modify().size, _alpha);
            this->updateMaxTreeSize();
            return new_node;
        }

        /**
         * insert some existing node object
         * NOTICE: potential benefit of near allocations not exploited
         */
        void insert_equal(iterator &new_node)
        {
            SG_Tree::insert_equal_upper_bound(
                this->head(), new_node, this->_comp, this->modify().size++, _alpha
            );
            this->updateMaxTreeSize();
        }

        /**
         * data - used as the checked insertion key
         */
        template <class KeyInitializer, typename... Args> std::pair<iterator, bool> insert_unique (
            const KeyInitializer &key, Args&&... args)
        {
            typename SG_Tree::insert_commit_data commit_data;
            std::pair<iterator, bool> result = SG_Tree::insert_unique_check(
                this->head(), key, _comp, commit_data
            );
            if (!result.second) {
                // node already exists
                return result;
            }
            // allocate / initialize new SG-Tree node
            node_t new_node(this->getMemspace(), key, std::forward<Args>(args)...);
            SG_Tree::insert_unique_commit(
                this->head(), new_node, commit_data, this->modify().size++, _alpha
            );
            this->updateMaxTreeSize();
            return std::make_pair(new_node, true);
        }
        
        /**
         * Find by node initializer / key
         */
        template <class KeyInitializer> iterator find(const KeyInitializer &key) const {
            return SG_Tree::find(this->head(), key, _comp);
        }

        /**
         * Find low-bound node by initializer / key
         */
        template <class KeyT> iterator lower_bound(const KeyT &key) const {
            return SG_Tree::lower_bound(this->head(), key, _comp);
        }
        
        template <class KeyT> iterator lower_equal_bound(const KeyT &key) const {
            return SG_Tree::lower_equal_bound(this->head(), key, _comp);
        }
                
        /**
         * Find upper-bound node by initializer / key
         */
        template <class KeyInitializer> iterator upper_bound(const KeyInitializer &key) const {
            return SG_Tree::upper_bound(this->head(), key, _comp);
        }

        template <class KeyInitializer> iterator upper_equal_bound(const KeyInitializer &key) const {
            return SG_Tree::upper_equal_bound(this->head(), key, _comp);
        }

        /**
         * Unlink node, don't destroy
         */
        void unlink(node_ptr_t &node)
        {
            if (SG_Tree::erase(
                this->head(), node, --this->modify().size, (*this)->max_tree_size, _alpha))
            {
                this->modify().max_tree_size = (*this)->size;
            }
        }
        
        /**
         * Static version of unlink, requires log(N) scan for the head-node
         */
        static void static_unlink(node_ptr_t &node,comp_t _comp = comp_t())
        {
            node_ptr_t header = SG_Tree::get_header(node);
            v_sgtree _tree((ptr_t&)(header),_comp);
            SG_Tree::erase(
                header, node, _tree.modify().size--, *((std::uint32_t*)&_tree.modify().max_tree_size), _tree._alpha
            );
        }

        void erase(node_ptr_t &node)
        {
            unlink (node);
            // VSPACE dispose node
            node.destroy();
        }

        /**
         * Static version of erase, requires log(N) scan for the head-node
         */
        static void static_erase(node_ptr_t &node,comp_t _comp = comp_t())
        {
            node_ptr_t header = SG_Tree::get_header(node);
            v_sgtree _tree((ptr_t&)(header),_comp);
            SG_Tree::erase(
                    header, node, _tree.modify().size--, *((std::uint32_t*)&_tree.modify().max_tree_size), _tree._alpha
            );
            // VSPACE dispose node
            node.destroy();
        }

        /**
         * Unlink node from "this" tree & insert into the "sg_dest"
         */
        void swap(iterator &it, v_sgtree &sg_dest)
        {
            SG_Tree::erase (
                    this->header_node, it, this->modify().size--, *((std::uint32_t*)&this->modify().max_tree_size)
            );
            sg_dest.insert_equal(it);
        }

        void clear() {
            if (destroyHeadNode(this->head())) {
                this->modify().ptr_set.left = this->getAddress();
                this->modify().ptr_set.right = this->getAddress();
                // unlink head node from the root element
                this->modify().ptr_set.parent = {};
                this->modify().size = 0;
            }
        }

        /**
         * Destroy SG-Tree and all its nodes (v-objects)
         */
        void destroy()
        {
            // destroy SG-Tree starting from the "head" element
            destroyHeadNode(this->head());
            // destroy SG-Tree v-object itself
            super::destroy();
        }

        /**
         * Measure tree height
         * @return tree height (maximum distance from leaf to root)
         */
        std::uint64_t height() const {
            return SG_Tree::get_height(this->head());
        }

    private :
        using node_traits_t = typename node_t::traits_t;
        // intrusive SG tree algorithm providers
        using _Tree = typename intrusive::detail::tree_algorithms<node_traits_t>;
        using SG_Tree = typename intrusive::sgtree_algorithms<node_traits_t>;
        using link_data = typename SG_Tree::link_data;
        alpha_t _alpha;
        // node comparer
        comp_t _comp;

#ifdef  __linux__
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif
        node_ptr_t &head() {
            // cast to head
            return reinterpret_cast<node_ptr_t&>(*this);
        }
        
        const node_ptr_t &head() const {
            // cast to head
            return reinterpret_cast<const node_ptr_t&>(*this);
        }

#ifdef  __linux__
    #pragma GCC diagnostic pop
#endif

        bool destroyHeadNode(const node_ptr_t &head_node) const
        {
            auto ptr_root = head_node->ptr_set.parent;
            if (!ptr_root.isValid() || (ptr_root == head_node.getAddress())) {
                return false;
            }

            node_t _root(this->getMemspace().myPtr(ptr_root));
            destroyNode(_root, head_node.getAddress());
            return true;
        }

        void destroyNode(node_t &node, Address ptr_head) const
        {
            auto ptr_left = node->ptr_set.left;
            if (ptr_left.isValid() && (ptr_left != ptr_head)) {
                node_t _left(this->getMemspace().myPtr(ptr_left));
                destroyNode(_left, ptr_head);
            }
            auto ptr_right = node->ptr_set.right;
            if (ptr_right.isValid() && ptr_right!=ptr_head) {
                node_t _right(this->getMemspace().myPtr(ptr_right));
                destroyNode(_right,ptr_head);
            }
            // destroy node itself
            node.destroy();
        }

        void updateMaxTreeSize()
        {
            if ((*this)->size > (*this)->max_tree_size) {
                this->modify().max_tree_size = (*this)->size;
            }            
        }

        // join API
    public :
        using join_stack = typename SG_Tree::join_stack;

        /**
         * Initialize join stack for joinBackward / joinBound
         */
        bool beginJoinBackward(join_stack &it) const
        {
            it.clear();
            SG_Tree::beginJoinBackward(this->head(),it);
            return (!it.empty());
        }

        template <class KeyT> bool join(join_stack &it,const KeyT &key, int direction) const
        {
            if (direction > 0) {
                // initialize join stack
                if (it.empty())
                {
                    SG_Tree::beginJoinForward(this->head(), it, key, _comp);
                    if (it.empty())
                    {
                        return false;
                    }
                }
                return SG_Tree::joinForward(it,key,_comp);
            } else {
                // initialize join stack
                if (it.empty())
                {
                    SG_Tree::beginJoinBackward(this->head(),it,key,_comp);
                    if (it.empty())
                    {
                        return false;
                    }
                }
                return SG_Tree::joinBackward(it,key,_comp);
            }
        }

        /**
         * Join / use specialized comparer
         */
        template <class KeyT,class NodePtrKeyComp> bool join(join_stack &it, const KeyT &key,
            NodePtrKeyComp key_comp, int direction) const
        {
            if (direction > 0) {
                // initialize join stack
                if (it.empty())
                {
                    SG_Tree::beginJoinForward(this->head(), it, key, key_comp);
                    if (it.empty())
                    {
                        return false;
                    }
                }
                return SG_Tree::joinForward(it, key, key_comp);
            } else {
                // initialize join stack
                if (it.empty())
                {
                    SG_Tree::beginJoinBackward(this->head(), it, key, key_comp);
                    if (it.empty())
                    {
                        return false;
                    }
                }
                return SG_Tree::joinBackward(it, key, key_comp);
            }
        }
        
        template <class KeyT> void joinBound(join_stack &it, const KeyT &key) const
        {
            // initialize join stack
            if (it.empty())
            {
                SG_Tree::beginJoinBackward(this->head(),it);
            }
            SG_Tree::joinBound(it,key,_comp);
        }

        /// joinBound implementation with dedicated key comparator
        template <class KeyT, typename NodePtrKeyComp> void joinBound(join_stack &it, const KeyT &key,
            NodePtrKeyComp key_comp) const
        {
            // initialize join stack
            if (it.empty()) {
                SG_Tree::beginJoinBackward(this->head(),it);
            }
            SG_Tree::joinBound(it,key, key_comp);
        }
    };

} 
