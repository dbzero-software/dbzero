// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
    
namespace db0 

{
	
    template <typename node_t, typename ptr_t> class base_traits_t
    {
    public :
        using node = node_t;
        using node_ptr = ptr_t;
        using const_node_ptr = ptr_t;
        
        static node_ptr get_null() {
            return node_ptr();
        }
        
        static node_ptr get_parent(const node_ptr &n)
        {
            if (!!n) {
                // NOTE: Address::fromOffset is in case ptr_set is of a regular numeric type
                return node_ptr(n.getMemspace().myPtr(Address::fromOffset(n->ptr_set.parent)));
            } else {
                return get_null();
            }
        }
        
        static void set_parent(node_ptr &n, const node_ptr &parent) {
            n.modify().ptr_set.parent = parent.getAddress();
        }
        
        static node_ptr get_left(const node_ptr &n)
        {
            if (!!n) {
                // NOTE: Address::fromOffset is in case ptr_set is of a regular numeric type
                return node_ptr(n.getMemspace().myPtr(Address::fromOffset(n->ptr_set.left)));
            } else {
                return get_null();
            }		
        }
        
        static void set_left(node_ptr &n, const node_ptr &left) {
            n.modify().ptr_set.left = left.getAddress();
        }
        
        static node_ptr get_right(const node_ptr &n)
        {
            if (!!n) {
                // NOTE: Address::fromOffset is in case ptr_set is of a regular numeric type
                return node_ptr(n.getMemspace().myPtr(Address::fromOffset(n->ptr_set.right)));
            } else {
                return get_null();
            }		
        }
        
        static void set_right(node_ptr &n, const node_ptr &right) {
            n.modify().ptr_set.right = right.getAddress();
        }
        
        static void checkIntegrity(const node_ptr &n) {
            n->checkIntegrity(n.getMemspace());
        }
    };
    
}