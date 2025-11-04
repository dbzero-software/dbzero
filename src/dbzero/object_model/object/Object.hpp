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
        
        // Assign language specific value as a field (to already initialized or uninitialized instance)
        // NOTE: if lang_value is nullptr then the member is removed
        void set(FixtureLock &, const char *field_name, ObjectPtr lang_value);
        void remove(FixtureLock &, const char *field_name);

    protected:
        void dropMembers(db0::swine_ptr<Fixture> &, Class &) const;
        
        bool tryUnrefWithLoc(FixtureLock &, FieldID, const void *, unsigned int pos, StorageClass,
            unsigned int fidelity);
        
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
    };
    
}
