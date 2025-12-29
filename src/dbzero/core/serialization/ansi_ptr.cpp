// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ansi_ptr.hpp"

using namespace std;

namespace db0

{

    template <> ansi_ci_char_table ansi_char_traits<ansi_ci_char_table>::c_tables = 
        ansi_ci_char_table ();
    template <> ansi_char_table ansi_char_traits<ansi_char_table>::c_tables = 
        ansi_char_table ();
        
    ansi_char_table::ansi_char_table () {
        for (int i=0;i<0x100;++i) {
            lc_table[i] = (char)tolower(i);
            uc_table[i] = (char)toupper(i);
            ranks[i] = (unsigned char)i;
        }
    }
        
    ansi_ci_char_table::ansi_ci_char_table() {
        for (int i=0;i<0x100;++i) {
            lc_table[i] = (char)tolower(i);
            uc_table[i] = (char)toupper(i);
            ranks[i] = (unsigned char)i;
        }
        for (int i=0;i<0x100;++i) {
            ranks[i] = ranks[(size_t)lc_table[i]];
        }
    }

	ostream &operator<<(ostream &os,const ansi_ptr &ansi_str) {
		ansi_str.dump(os);
		return os;
	}

	ostream &operator<<(ostream &os,const ansi_cs_ptr &ansi_str) {
		ansi_str.dump(os);
		return os;
	}
    
}

