#include "ObjectBase.hpp"
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/core/vspace/v_object.hpp>

namespace db0::object_model

{

    class [[gnu::packed]] o_common_base: public db0::o_base<o_common_base, 0, false>
    {
    public:
        // common object header
        o_unique_header m_header;
    };
    
    // common base for ObjectBase derived classes
    class CommonBase: public ObjectBase<CommonBase, db0::v_object<o_common_base>, StorageClass::UNDEFINED>
    {
    public:
    };

}