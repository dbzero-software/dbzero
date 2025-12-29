// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

// Copyright (C) 2020-2022 Infortex.  All rights reserved.
// The information and source code contained herein is the exclusive
// property of Infortex sp. z o.o. ( https://itx.pl ) and may not be disclosed, examined
// or reproduced in whole or in part without explicit written authorization
// from the company.
	
#pragma once

#include <cstring>
#include <iostream>
#include "str_ptr.hpp"

namespace db0

{

	// ANSI - string iterator
	class ansi_iterator
    {
	public :
		ansi_iterator(const char *c)
			: c((char*)c)
		{
		}

		char get_char() const {
			return *c;
		}

		void set_char (char new_c) {
			*c = new_c;
		}

		ansi_iterator operator++(int) {
			ansi_iterator result = *this;
			++c;
			return result;
		}

		ansi_iterator &operator++() {
			++c;
			return *this;
		}

		ansi_iterator operator--(int) {
			ansi_iterator result = *this;
			--c;
			return result;
		}

		bool operator==(const ansi_iterator &it) const {
			return (this->c==it.c);
		}

		bool operator!=(const ansi_iterator &it) const {
			return (this->c!=it.c);
		}

		ansi_iterator operator--() {
			--c;
			return *this;
		}

		char operator*() const {
			return *c;
		}

		operator const char*() const {
			return (const char*)c;
		}

		static void encode (char c,std::ostream &os) {
			os << c;
		}

		bool isValid () const {
			return (c!=0);
		}

		const unsigned char* get_uraw() const {
			return (const unsigned char *) c;
		}

		unsigned char* get_uraw() {
			return (unsigned char *) c;
		}

		const char* get_raw() const {
			return c;
		}

		char* get_raw() {
			return c;
		}

		ansi_iterator operator+(unsigned int offset) const {
			return ansi_iterator(c + offset);
		}

		ansi_iterator &operator+=(unsigned int offset) {
			c += offset;
			return *this;
		}

		ansi_iterator &operator-=(unsigned int offset) {
			c -= offset;
			return *this;
		}

	private :
		char *c;
	};

    // case sensitive characters table
	class ansi_char_table {
	public :
		char lc_table[0x100]; // lower case eqivalents
		char uc_table[0x100]; // upper case equvalents
		unsigned char ranks[0x100]; // character ranks
		ansi_char_table ();
	};

    // case - insensitive characters table
	class ansi_ci_char_table {
	public :
		char lc_table[0x100]; // lower case eqivalents
		char uc_table[0x100]; // upper case equvalents
		unsigned char ranks[0x100]; // character ranks
		ansi_ci_char_table ();
	};

	template <class _ct = ansi_ci_char_table> class ansi_char_traits
    {
	public :
		// whitespace character set
		class white_set {
		public :
			bool operator()(wchar_t c) const {
				return ((c==' ') || (c=='\r') || (c=='\n') || (c=='\t'));
			}
			bool operator()(char c) const {
				return ((c==' ') || (c=='\r') || (c=='\n') || (c=='\t'));
			}
		};

		// newline character set
		class nl_set {
		public :
			bool operator()(wchar_t c) const {
				return ((c=='\r') || (c=='\n'));
			}
			bool operator()(char c) const {
				return ((c=='\r') || (c=='\n'));
			}
		};

		// decimal digits
		class digit_set {
		public :
			bool operator()(wchar_t c) const {
				return ((c>='0') && (c<='9'));
			}
			bool operator()(char c) const {
				return ((c>='0') && (c<='9'));
			}
		};

		// number characters
		class number_set {
		public :
			digit_set d_set;
			bool operator()(wchar_t  c) const {
				return (d_set(c) || (c=='.'));
			}
			bool operator()(char c) const {
				return (d_set(c) || (c=='.'));
			}
		};

		// custom character set
		class char_set {
		public :
			bool set[0x100];
			char_set (const std::string &str_set) {
				memset(this->set,0,sizeof(this->set));
				init(str_set);
			}

			// extend existing char-set
			char_set (const char_set &c_set,const std::string &str_set) {
				memcpy (this->set,c_set.set,sizeof(this->set));
				init(str_set);
			}

			bool operator()(wchar_t c) const {
				if (c < 0x100) {
					return this->set[c];
				}
				else {
					return false;
				}
			}

			bool operator()(char c) const {

				return (c>=0 ? this->set[ static_cast<unsigned char>( c ) ] : false);
			}

		private :
			void init (const std::string &str_set) {
				_ct c_table;
				std::string::const_iterator c = str_set.begin();
				while (c!=str_set.end()) {
					if (*c >=0 ){
						auto char_index = static_cast<unsigned char>( *c );
						this->set[ static_cast<unsigned char>( c_table.lc_table[ char_index ] ) ] = true;
						this->set[ static_cast<unsigned char>( c_table.uc_table[ char_index ] ) ] = true;
					}
					++c;
				}
			}
		};

		static size_t char_rank (char c) {
			return c_tables.ranks[(unsigned char)c];
		}

		static bool eq (char c0,char c1) {
			return (c_tables.ranks[(unsigned char)c0]==c_tables.ranks[(unsigned char)c1]);
		}

		static bool lt (char c0,char c1) {
			return (c_tables.ranks[(unsigned char)c0] < c_tables.ranks[(unsigned char)c1]);
		}

		static bool gt (char c0,char c1) {
			return (c_tables.ranks[(unsigned char)c0] > c_tables.ranks[(unsigned char)c1]);
		}

		static char to_upper (char c) {
			return c_tables.uc_table[(unsigned char)c];
		}

		static char to_lower (char c) {
			return c_tables.lc_table[(unsigned char)c];
		}

		static bool is_upper (char c) {
			return (c == c_tables.uc_table[(unsigned char)c]);
		}

		static bool is_lower (char c) {
			return (c == c_tables.lc_table[(unsigned char)c]);
		}

	private :
		static _ct c_tables;
	};

	using ansi_ptr = str_ptr<ansi_iterator,ansi_char_traits<ansi_ci_char_table> >;
	using ansi_cs_ptr = str_ptr<ansi_iterator,ansi_char_traits<ansi_char_table> >;
	
	std::ostream &operator<<(std::ostream &os,const db0::ansi_ptr &ansi_str);
	std::ostream &operator<<(std::ostream &os,const db0::ansi_cs_ptr &ansi_str);
    
} 
