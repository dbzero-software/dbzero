// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "ObjectImplBase.hpp"
#include "o_object.hpp"

namespace db0::object_model

{
    
    class Object: public ObjectImplBase<o_object, Object>
    {
        // GC0 specific declarations
        GC0_Declare
    public:
        static constexpr unsigned char REALM_ID = o_object::REALM_ID;
        using super_t = ObjectImplBase<o_object, Object>;
        
        template <typename... Args>
        Object(Args&&... args)
            : super_t(std::forward<Args>(args)...)
        {
        }
        
        // Convert singleton into a regular instance
        void unSingleton(FixtureLock &);
        bool isSingleton() const;

        // Assign language specific value as a field (to already initialized or uninitialized instance)
        // NOTE: if lang_value is nullptr then the member is removed
        void set(FixtureLock &, const char *field_name, ObjectPtr lang_value);
        void remove(FixtureLock &, const char *field_name);
        
        // Destroys an existing instance and constructs a "null" placeholder
        void dropInstance(FixtureLock &);

    protected:
        friend super_t;

        bool tryFindMemberAt(std::pair<FieldID, unsigned int> field_info,
            std::pair<StorageClass, Value> &result, std::pair<bool, bool> &find_result) const;
        
        void getFieldLayoutImpl(FieldLayout &layout) const;
        void getMembersImpl(std::unordered_set<std::string> &) const;
        bool tryEqualToImpl(const ObjectImplBase<o_object, Object> &, bool &result) const;
        
        // Set or update member in a pos_vt
        void setPosVT(FixtureLock &, FieldID, unsigned int pos, unsigned int fidelity, StorageClass, Value);
        void setIndexVT(FixtureLock &, FieldID, unsigned int index_vt_pos, unsigned int fidelity,
            StorageClass, Value);        
        
        // Set with a specific location (pos_vt, index_vt, kv-index)
        void setWithLoc(FixtureLock &, FieldID, const void *, unsigned int pos, unsigned int fidelity, 
            StorageClass, Value);
        
        // Add a new value
        void addToPosVT(FixtureLock &, FieldID, unsigned int pos, unsigned int fidelity, StorageClass, Value);
        void addToIndexVT(FixtureLock &, FieldID, unsigned int index_vt_pos, unsigned int fidelity, StorageClass, Value);
        
        void addWithLoc(FixtureLock &, FieldID, const void *, unsigned int pos, unsigned int fidelity,
            StorageClass, Value);
        
        void dropMembers(db0::swine_ptr<Fixture> &, Class &) const;
        
        bool tryUnrefWithLoc(FixtureLock &, FieldID, const void *, unsigned int pos, StorageClass,
            unsigned int fidelity);            
        bool tryFindMemberSlot(const std::pair<FieldID, unsigned int> &field_info, unsigned int &pos,
            std::pair<FieldInfo, const void *> &result) const;
        
        /**
         * If the KV_Index does not exist yet, create it and add the first value
         * otherwise return instance of an existing KV_Index
        */
        KV_Index *addKV_First(const XValue &);

        KV_Index *tryGetKV_Index() const;
        
        bool hasKV_Index() const;

        void addToKVIndex(FixtureLock &, FieldID, unsigned int fidelity, StorageClass, Value);
        void unrefKVIndexValue(FixtureLock &, FieldID, StorageClass, unsigned int fidelity);   
        // Set or update member in kv-index
        void setKVIndexValue(FixtureLock &, FieldID, unsigned int fidelity, StorageClass, Value);
        
        bool forAllImpl(std::function<bool(const std::string &, const XValue &, unsigned int offset)>) const;
    };
    
}
