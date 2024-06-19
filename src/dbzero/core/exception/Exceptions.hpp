#pragma once

#include <dbzero/core/exception/AbstractException.hpp>

namespace db0

{

    namespace EXCEPTION_ID_PREFIX  {

        enum : int {
            // common exceptions
            BASIC = 0x00000000,
            // application specific exceptions
            APP = 0x00000100,
            NETWORK = 0x08000000
        };

    }

    class CriticalException : public AbstractException {
    public:	
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x00caffee;

        CriticalException(int err_id = exception_id);
    };
        
    class RecoverableException : public AbstractException {
    public:	
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x0000beef;

        RecoverableException(int err_id);
        virtual ~RecoverableException() = default;
    };

    class InternalException : public CriticalException {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x01;

        InternalException(int err_id = exception_id);
    };
        
    class InputException : public RecoverableException {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x03;

        InputException(int err_id = exception_id);
        virtual ~InputException() = default;
    };
        
    class KeyNotFoundException : public InputException {
    public :
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x09;

        KeyNotFoundException(int err_id = exception_id);
    };

    class IOException : public RecoverableException {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x02;

        IOException(int err_id = exception_id);
    };

    class OutOfDiskSpaceException : public CriticalException
    {
    public:
        static constexpr int exception_id = EXCEPTION_ID_PREFIX::BASIC | 0x04;

        OutOfDiskSpaceException();
    };

}