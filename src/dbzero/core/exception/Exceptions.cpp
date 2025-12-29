// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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

    CriticalException::CriticalException(int err_id)
        : AbstractException(err_id)
    {
    }

    RecoverableException::RecoverableException(int err_id)
        : AbstractException(err_id)
    {
    }

    OutOfDiskSpaceException::OutOfDiskSpaceException()
        : CriticalException(exception_id)
    {        
    }

    MemoryException::MemoryException()
        : CriticalException(exception_id)
    {        
    }
    
    ClassNotFoundException::ClassNotFoundException()
        : CriticalException(exception_id)
    {        
    }

    AccessTypeException::AccessTypeException()
        : CriticalException(exception_id)
    {        
    }
    
    BadAddressException::BadAddressException()
        : CriticalException(exception_id)
    {        
    }
    
    IndexException::IndexException()
        : CriticalException(exception_id)
    {        
    }

    CacheException::CacheException()
        : CriticalException(exception_id)
    {        
    }

    PrefixNotFoundException::PrefixNotFoundException()
        : RecoverableException(exception_id)
    {
    }

}