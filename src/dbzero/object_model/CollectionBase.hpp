#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/object_model/value/TypeUtils.hpp>


namespace db0::object_model
{
    
    template <typename T, typename BaseT, StorageClass _CLS>
    class VBVectorBase: public db0::ObjectBase<T, BaseT, _CLS>
    {
            using super_t = db0::ObjectBase<T, BaseT, _CLS>;
            using LangToolkit = db0::python::PyToolkit;
            using ObjectPtr = typename LangToolkit::ObjectPtr;
            using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
            using const_iterator = typename v_bvector<o_typed_item>::const_iterator;
        public:
            void append(FixtureLock &, ObjectPtr lang_value) {
                using TypeId = db0::bindings::TypeId;
            
                // recognize type ID from language specific object
                auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
                auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
                BaseT::push_back(
                    createListItem<LangToolkit>(*fixture, type_id, lang_value, storage_class)
                );
            }
    };

}