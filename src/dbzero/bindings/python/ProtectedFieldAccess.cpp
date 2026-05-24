// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/bindings/python/ProtectedFieldAccess.hpp>

#include <dbzero/bindings/python/DataMasking.hpp>
#include <dbzero/core/memory/config.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/workspace/Fixture.hpp>

#include <memory>
#include <sstream>

namespace db0::python
{
    using namespace db0::object_model;

    namespace
    {
        bool getProtectedFieldAccessContext(
            const Class &type, const db0::swine_ptr<Fixture> &fixture,
            std::shared_ptr<DataMaskingState> &maskingState, long long &accountId
        )
        {
            if (!type.isProtectFields()) {
                return false;
            }

            if (!Settings::m_data_masking_enabled) {
                PyErr_SetString(PyExc_RuntimeError, "data masking is not initialized for protected fields");
                return false;
            }

            maskingState = fixture->getMaskingState();
            if (!maskingState) {
                PyErr_SetString(PyExc_RuntimeError, "data masking is not initialized for protected fields");
                return false;
            }

            PyObject *pyAccountId = nullptr;
            if (PyContextVar_Get(maskingState->contextVar, NULL, &pyAccountId) < 0) {
                PyErr_SetString(PyExc_RuntimeError, "unable to read data masking account context");
                return false;
            }
            if (!pyAccountId) {
                PyErr_SetString(PyExc_RuntimeError, "data masking account context is not set");
                return false;
            }

            accountId = PyLong_AsLongLong(pyAccountId);
            Py_DECREF(pyAccountId);
            if (PyErr_Occurred()) {
                PyErr_SetString(PyExc_TypeError, "data masking account context must be an int");
                return false;
            }

            if (accountId < -2) {
                PyErr_SetString(PyExc_RuntimeError, "invalid data masking account id");
                return false;
            }

            return true;
        }

        void setProtectedFieldPermissionError(FieldMaskOptions accessOption)
        {
            std::stringstream message;
            message << "data masking denies " << accessOption
                << " access to protected field";
            PyErr_SetString(PyExc_PermissionError, message.str().c_str());
        }
    }

    bool checkProtectedFieldAccess(
        const Class &type, const db0::swine_ptr<Fixture> &fixture, FieldMaskOptions accessOption,
        const MemberLoc &memberLoc, const char *fieldName
    )
    {
        if (!type.isProtectFields()) {
            return true;
        }

        std::shared_ptr<DataMaskingState> maskingState;
        long long accountId = 0;
        if (!getProtectedFieldAccessContext(type, fixture, maskingState, accountId)) {
            return !PyErr_Occurred();
        }

        if (maskingState->mode == DataMaskingMode::DEBUG) {
            if (accountId == -2) {
                return true;
            }
            if (accountId == -1) {
                return accessOption == FieldMaskOptions::READ;
            }
        }

        if (accountId < 0) {
            return false;
        }

        auto mask = type.tryGetFieldAccessByMemberLoc(static_cast<std::uint64_t>(accountId), memberLoc);
        if (!mask && fieldName) {
            mask = type.tryGetFieldAccessByName(static_cast<std::uint64_t>(accountId), fieldName);
        }
        return mask && (*mask)[accessOption];
    }

    PyObject *checkProtectedFieldReadAccess(
        const Class &type, const db0::swine_ptr<Fixture> &fixture, const MemberLoc &memberLoc, const char *fieldName
    )
    {
        auto accessOption = FieldMaskOptions::READ;
        if (checkProtectedFieldAccess(type, fixture, accessOption, memberLoc, fieldName)) {
            return nullptr;
        }
        if (PyErr_Occurred()) {
            return nullptr;
        }

        auto maskingState = fixture->getMaskingState();
        if (maskingState->hasMissingValuePlaceholder) {
            Py_INCREF(maskingState->missingValuePlaceholder);
            return maskingState->missingValuePlaceholder;
        }

        setProtectedFieldPermissionError(accessOption);
        return nullptr;
    }

    bool checkProtectedFieldMutateAccess(
        const Class &type, const db0::swine_ptr<Fixture> &fixture, FieldMaskOptions accessOption, const char *fieldName
    )
    {
        if (!type.isProtectFields()) {
            return true;
        }

        auto memberLoc = type.findField(fieldName);
        if (checkProtectedFieldAccess(type, fixture, accessOption, memberLoc, fieldName)) {
            return true;
        }
        if (PyErr_Occurred()) {
            return false;
        }

        setProtectedFieldPermissionError(accessOption);
        return false;
    }
}
