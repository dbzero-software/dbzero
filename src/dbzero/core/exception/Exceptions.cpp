#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    InternalException::InternalException(int err_id)
        : CriticalException(err_id)
    {
    }

    InputException::InputException(int err_id)
        : RecoverableException(err_id)
    {
    }

    KeyNotFoundException::KeyNotFoundException(int err_id)
        : InputException(err_id)
    {
    }

    IOException::IOException(int err_id)
        : RecoverableException(err_id)
    {
    }

    CriticalException::CriticalException (int err_id)
        : AbstractException(err_id)
    {
    }

    RecoverableException::RecoverableException (int err_id)
        : AbstractException(err_id)
    {
    }

    OutOfDiskSpaceException::OutOfDiskSpaceException()
        : CriticalException(exception_id)
    {        
    }

}