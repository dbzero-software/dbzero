/////////////////////////////////////////////////////////////////////////////
// Original work:
// (C) Copyright Ion Gaztanaga 2006-2007
// Distributed under the Boost Software License, Version 1.0.
//    (See THIRD_PARTY_LICENSES/BOOST_LICENSE_1_0 or
//     http://www.boost.org/LICENSE_1_0.txt)
//
// This file may contain modifications by DBZero Software sp. z o.o.
// Any modifications are Copyright (c) 2025 DBZero Software sp. z o.o.
// and licensed under LGPL-2.1.
//
// SPDX-License-Identifier: BSL-1.0 AND LGPL-2.1
/////////////////////////////////////////////////////////////////////////////

#pragma once

namespace intrusive {

//!This enumeration defines the type of value_traits that can be defined
//!for Boost.Intrusive containers
enum link_mode_type{
   //!If this linking policy is specified in a value_traits class
   //!as the link_mode, containers
   //!configured with such value_traits won't set the hooks
   //!of the erased values to a default state. Containers also won't
   //!check that the hooks of the new values are default initialized.
   normal_link,

   //!If this linking policy is specified in a value_traits class
   //!as the link_mode, containers
   //!configured with such value_traits will set the hooks
   //!of the erased values to a default state. Containers also will
   //!check that the hooks of the new values are default initialized.
   safe_link,

   //!Same as "safe_link" but the user type is an auto-unlink
   //!type, so the containers with constant-time size features won't be
   //!compatible with value_traits configured with this policy.
   //!Containers also know that the a value can be silently erased from
   //!the container without using any function provided by the containers.
   auto_unlink
};

} 


