// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

// Copyright (C) 2020-2022 Infortex.  All rights reserved.
// The information and source code contained herein is the exclusive
// property of Infortex sp. z o.o. ( https://itx.pl ) and may not be disclosed, examined
// or reproduced in whole or in part without explicit written authorization
// from the company.

#pragma once

#include <cstddef>
#include <iterator>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    /**
     * Fixed size character encoded string referencer
     * iterator - character iterator (single db0::byte / multi db0::byte / wide)
     * _ct - character traits, required static operations :
     * 		 eq, lt, gt (character comparison)
     * 		 to_lower , to_upper - case conversion
     * iterator must be character convertible
     */
	template <class iterator_,class _ct_> class str_ptr
    {
		typedef str_ptr<iterator_, _ct_> self;
	public :
		using iterator = iterator_;
		using _ct = _ct_;

		/**
         * Format adding delimiter if not empty
         */
		class formatter {
		protected :
			self str;
			std::string del;
		public :
			formatter (self str, const std::string &del)
					: str(str)
					, del(del)
			{
			}

			friend std::ostream &operator<<(std::ostream &os, const formatter &f) {
				if (!f.str.empty()) {
					os << f.str << f.del;
				}
				return os;
			}
		};

		/**
		 * Construct null instance
		 */
		str_ptr()
			: _begin(nullptr)
			, _end(nullptr)
		{
		}

		str_ptr(const iterator &begin, const iterator &end)
			: _begin(begin)
			, _end(end)
		{
		}

		/**
         * from the 2 arbitrary iterators
         */
		template <typename T> str_ptr(T begin, T end)
			: _begin(begin)
			, _end(end)
		{
		}

		/**
         * initialize over null-terminated string
         * @param str if null then valid null instance will be created
         */
		str_ptr(const char *str)
			: _begin(str)
			, _end(str?end_of(str):nullptr)
		{
		}

		explicit str_ptr(const std::string &s)
			: _begin(s.c_str())
			, _end(s.c_str() + s.size())
		{
		}

		// evaluated
		int length() const {
			int length = 0;
            if (!isNull()) {
                for (iterator it = begin(), itend = end(); it != itend; ++it) ++length;
            }
			return length;
		}

		bool shorter_than(int len) const
        {
		    if (!isNull()) {
                // check raw number of bytes first
                if ((_end.get_uraw() - _begin.get_uraw()) < (size_t) len) {
                    return true;
                }
                int _len = 0;
                for (iterator it = begin(), itend = end(); it != itend; ++it) {
                    if (++_len >= len) {
                        return false;
                    }
                }
            }
			return true;
		}

		std::string toString() const {
		    auto *raw_begin = _begin.get_raw();
		    auto *raw_end = _end.get_raw();
            if (raw_begin && raw_end) {
                return std::string(_begin.get_raw(), _end.get_raw() - _begin.get_raw());
            } else {
                // even though this is a null object we'll convert to empty string
                return "";
            }
		}

		// get string ( prefix ) up to the specified length
		self get_prefix(int len) const {
			iterator c = begin();
			while ((len--) > 0 && c!=end()) {
				++c;
			}
			return self(begin(), c);
		}

		// reference as null terminated string ( throws )
		const char *c_str () const {
			// not a null terminated string
			if (*_end!=0) {
				THROW(InputException, "not a null terminated string");
			}
			return _begin.get_raw();
		}

		// actual number of bytes used ( terminating character not included )
		std::size_t size() const {
			return _end.get_raw() - _begin.get_raw();
		}

		// reference raw bytes ( for read / copy )
		const unsigned char *raw_bytes() const {
			return _begin.get_uraw();
		}

		unsigned char *raw_bytes() {
			return _begin.get_uraw();
		}

		const char *get_raw() const {
			return _begin.get_raw();
		}

		bool operator==(const str_ptr<iterator,_ct> &str) const {
			iterator it = begin();
			iterator str_it = str.begin();
			for (;;) {
				if (it==end()) {
					return (str_it==str.end());
				}
				if (str_it==str.end()) {
					return false;
				}
				if (!_ct::eq(*it,*str_it)) {
					return false;
				}
				++it;
				++str_it;
			}
			return true; // never reached
		}

		bool operator!=(const str_ptr<iterator,_ct> &str) const {
			return !(*this == str);
		}

		bool operator<(const str_ptr<iterator,_ct> &str) const {
			iterator it = begin();
			iterator str_it = str.begin();
			for (;;) {
				if (it==end()) {
					return (str_it!=str.end());
				}
				if (str_it==str.end()) {
					return false;
				}
				if (_ct::lt(*it,*str_it)) {
					return true;
				}
				if (_ct::gt(*it,*str_it)) {
					return false;
				}
				++it;
				++str_it;
			}
			return true; // never reached
		}

		bool operator>(const str_ptr<iterator,_ct> &str) const {
			iterator it = begin();
			iterator str_it = str.begin();
			for (;;) {
				if (it==end()) {
					return false;
				}
				if (str_it==str.end()) {
					return true;
				}
				if (_ct::gt(*it,*str_it)) {
					return true;
				}
				if (_ct::lt(*it,*str_it)) {
					return false;
				}
				++it;
				++str_it;
			}
			return true; // never reached
		}

		iterator begin() const {
			return _begin;
		}

		iterator end() const {
			return _end;
		}

		void make_upper() {
			for (iterator it = begin(),itend = end();it!=itend;++it) {
				if (!_ct::is_upper(*it)) {
					it.set_char(_ct::to_upper(*it)); // assign
				}
			}
		}

		void make_lower() {
			for (iterator it = begin(),itend = end();it!=itend;++it) {
				if (!_ct::is_lower(*it)) {
					it.set_char(_ct::to_lower(*it)); // assign
				}
			}
		}

		/**
         * divide into 1 or more tokens, using specified tokenizer (function object)
         * del_result - optional vector of delimiters, each token has exactly 1 corresponding preceeding delimiter
         */
		template <class tokenizer_t> void tokenize (tokenizer_t tok, std::vector<str_ptr<iterator,_ct> > &result,
													std::vector<str_ptr<iterator,_ct> > *del_result = 0) const
		{
			// tokenizer states :
			// 0 - initial
			// 1 - token being parsed
			// 2 - delimiter being parsed
			int state = 0;
			for (iterator c = begin(),itend = end();c!=itend;++c) {
				switch (state) {
					case 0 : {
						// delimiter
						if (tok(*c)) {
							if (del_result) {
								del_result->push_back(str_ptr<iterator,_ct>(c,c));
								// delimiter corresponding token
								result.push_back(str_ptr<iterator,_ct>(end(),end()));
							}
							state = 2;
						}
						else {
							result.push_back(str_ptr<iterator,_ct>(c,c));
							// token corresponding delimiter
							if (del_result) {
								del_result->push_back(str_ptr<iterator,_ct>(end(),end()));
							}
							state = 1;
						}
					}
						break;

					case 1 : {
						// delimiter, complete token
						if (tok(*c)) {
							result.back()._end = c;
							if (del_result) {
								del_result->push_back(str_ptr<iterator,_ct>(c,c));
								// delimiter corresponding token
								result.push_back(str_ptr<iterator,_ct>(end(),end()));
							}
							state = 2;
						}
					}
						break;

					case 2 : {
						// token, complete delimiter
						if (!tok(*c)) {
							if (del_result) {
								// delimiter corresponding token
								result.back()._begin = c;
								del_result->back()._end = c;
							}
							else {
								result.push_back(str_ptr<iterator,_ct>(c,c));
							}
							state = 1;
						}
					}
						break;
				}
			}
			// finalize
			switch (state) {
				case 0 :
					break;

				case 1 : {
					result.back()._end = end();
				}
					break;

				case 2 : {
					if (del_result) {
						del_result->back()._end = end();
					}
				}
					break;
			}
		}

		bool empty() const {
			return begin()==end();
		}

		/**
         * normalize text case, store original case (as bits) with provided bit collector
         * _bitset - any bool / bit container (bit_vector / vector<bool> compliant)
         * not filled if all bits would be "0"
         */
		template <class _bitset> void normalize_case (_bitset &result) {
			int state = 0;
			int count = 0;
			iterator it = begin();
			while (it!=end()) {
				if (!_ct::is_lower(*it)) {
					it.set_char(_ct::to_lower(*it)); // assign
					if (state==0) {
						while (count > 0) {
							result.push_back(false);
							--count;
						}
						state = 1;
					}
					result.push_back(true); // case modified
				}
				else {
					if (state==0) {
						++count;
					}
					else {
						result.push_back(false); // case not modified
					}
				}
				++it;
			}
		}

		/**
         * restore original char case
         * char_case - bit vector / container obtained from normalize_case
         */
		template <class _bitset> void restore_case(const _bitset &char_case) {
			iterator it = begin();
			typename _bitset::iterator _bit = char_case.begin();
			while (it!=end() && _bit!=char_case.end()) {
				if (*_bit) {
					it.set_char(_ct::to_upper(*it));
				}
				++it;
				++_bit;
			}
		}

		/**
         * restore original case (bitreader specialization)
         * char_case - bit vector / container obtained from normalize_case
         */
		template <class _bitreader> void restore_case_bitreader(_bitreader &char_case) {
			if (char_case.empty()) {
				return;
			}
			iterator it = begin();
			while (it!=end()) {
				if (char_case.get_bit()) {
					it.set_char(_ct::to_upper(*it));
				}
				if (!char_case++) {
					return;
				}
				++it;
			}
		}

		/**
         * enc_t - ANIS encoder / decoder ( see iso_encoder for sample )
         * encode UTF-8 into ANSI encoded stream
         */
		template <class enc_t> void ansi_encode(enc_t _enc,std::ostream &os) const {
			iterator it = begin();
			while (it!=end()) {
				os << _enc.encode(*it);
				++it;
			}
		}

		/**
         * try encode, stop on first bad character (flag set)
         */
		template <class enc_t> void ansi_encode(enc_t _enc,std::ostream &os,bool &bad_char) const {
			bad_char = false;
			iterator it = begin();
			while (it!=end() && !bad_char) {
				os << _enc.try_encode(*it,bad_char);
				++it;
			}
		}

		template <class enc_t> std::string ansi_encode(enc_t enc) const {
			std::stringstream _str;
			ansi_encode (enc,_str);
			return _str.str();
		}

		/**
         * try encode, stop on first bad character ( flag set )
         */
		template <class enc_t> std::string ansi_encode(enc_t enc,bool &bad_char) const {
			std::stringstream _str;
			ansi_encode (enc,_str,bad_char);
			return _str.str();
		}

		/**
         * c_sequence - input ANSI characters sequence
         * enc_t - ANSI / UNICODE decoder
         */
		template <class enc_t,class c_sequence> static void ansi_decode(enc_t enc,c_sequence begin,c_sequence end,
																		std::ostream &os)
		{
			while (begin < end) {
				iterator::encode (enc.decode(*begin),os);
				++begin;
			}
		}

		template <class enc_t,class c_sequence> static std::string ansi_decode(enc_t enc,c_sequence begin,c_sequence end) {
			std::stringstream _str;
			ansi_decode(enc,begin,end,_str);
			return _str.str();
		}

		/**
         * dump raw bytes
         */
		void dump(std::ostream &os) const {
			const char *c0 = _begin.get_raw();
			const char *c1 = _end.get_raw();
			if (c0 && c1) {
				while (c0 < c1) {
					os << (*c0);
					++c0;
				}
			}
		}

		template <class set_t> str_ptr trimLeft(const set_t &_set) const {
			iterator it = begin();
			while (it!=end() && _set(*it)) {
				++it;
			}
			return str_ptr(it,end());
		}

		template <class set_t> str_ptr trimRight(const set_t &_set) const {
			if (empty()) {
				return *this;
			}
			else {
				iterator it = end();
				--it;
				while (_set(*it) && it!=begin()) {
					--it;
				}
				if (_set(*it)) {
					return str_ptr(begin(),it);
				}
				else {
					++it;
					return str_ptr(begin(),it);
				}
			}
		}

		template <class set_t> str_ptr trimBoth(const set_t &_set) const {
			return trimRight(_set).trimLeft(_set);
		}

		/**
         * check content for correct encoding
         * @return position of the first error found
         */
		iterator checkEncoding() const {
			iterator it = _begin;
			do {} while (it!=_end && it.check_char());
			return it;
		}

		/**
         * write fixed encoding into specified output stream
         * replace bad characters with "fix_char" ( empty by default )
         */
		void fixEncoding(std::ostream &os,str_ptr fix_char = str_ptr()) const {
			str_ptr str = *this;
			iterator fix_it = str.begin();
			while (fix_it!=str.end()) {
				fix_it = str.checkEncoding();
				os << str_ptr(str.begin(),fix_it);
				// fix invalid character
				if (fix_it!=str.end()) {
					os << fix_char;
					str = str_ptr(fix_it.fixChar(str.end()),str.end());
					fix_it = str.begin();
				}
			}
		}

		std::size_t getHash() const {
			std::size_t hash = 0;
			if (!isNull()) {
                iterator it = begin(), it_end = end();
                for (; it != it_end; ++it) {
                    hash = hash * 101 + _ct::char_rank(*it);
                }
            }
			return hash;
		}

		formatter delimited(const std::string &del) const {
			return formatter(*this, del);
		}

		/**
         * Check for special null value (instantiated with default constructor)
         * null is always empty but empty may not be null
         * @return true is this is null instance
         */
		bool isNull() const;

	private :
		iterator _begin;
		iterator _end;

		static const char *end_of(const char *str) {
			if (str) {
				while (*str) {
					++str;
				}
			}
			return str;
		}
	};

	class utf8_iterator : public std::iterator<std::bidirectional_iterator_tag, wchar_t> {
	public :
		utf8_iterator (const char *c)
				: c((unsigned char*)c)
		{
		}

		/**
         * check for valid character, move iterator to the next character
         * @return false if check failed, iterator not moved
         */
		bool check_char() {
			if (*c & 0x80) {
				unsigned char _c = *c;
				unsigned char _class = 0;
				while (_c & 0x80) {
					++_class;
					_c <<= 1;
				}
				if ((_class < 2) || (_class > 6)) {
					return false;
				}
				_c >>= _class;
				unsigned char *c_temp = c;
				while (_class > 1) {
					++c_temp;
					if ((*c_temp & 0xc0)!=0x80) {
						return false;
					}
					--_class;
				}
				c = c_temp;
			}
			++c;
			return true;
		}

		wchar_t get_char() const {
			if (!(*c & 0x80)) {
				return *c; // ANSI character
			}
			unsigned char _c = *c;
			unsigned char _class = 0;
			while (_c & 0x80) {
				++_class;
				_c <<= 1;
			}
			if ((_class < 2) || (_class > 6)) {
				THROW (InputException, "utf-8 decode fault");
			}
			_c >>= _class;
			wchar_t w_char = _c;
			unsigned char *c_temp = c;
			while (_class > 1) {
				++c_temp;
				w_char <<= 6;
				if ((*c_temp & 0xc0)!=0x80) {
					THROW (InputException, "utf-8 decode fault");
				}
				w_char |= (*c_temp) & 0x3f;
				--_class;
			}
			return w_char;
		}

		/**
         * character class must be preserved
         */
		void set_char (const wchar_t &new_c) {
			if (!(*c & 0x80)) {
				// not ANSI character
				if (new_c > 0x80) {
					THROW (InputException, "set_char failed");
				}
				*c = (unsigned char)new_c;
				return;
			}
			unsigned char _c = *c;
			unsigned char _class = 0;
			while (_c & 0x80) {
				++_class;
				_c <<= 1;
			}
			if ((_class < 2) || (_class > 6)) {
				THROW (InputException, "utf-8 decode fault");
			}
			// encode new utf-8 character in place of old utf-8 character ( same class character )
			encode (new_c, c, c + _class, _class);
		}

		const unsigned char* get_uraw() const {
			return c;
		}

		unsigned char* get_uraw() {
			return c;
		}

		const char* get_raw() const {
			return (const char *) c;
		}

		char* get_raw() {
			return (char *) c;
		}

		utf8_iterator operator++(int) {
			utf8_iterator result = *this;
			// unicode character
			if (*c & 0x80) {
				unsigned char _c = (*c);
				while (_c & 0x80) {
					++c;
					_c <<= 1;
				}
			}
				// ANSI character
			else {
				++c;
			}
			return result;
		}

		/**
         * copy & move position by single UTF-8 character
         */
		void copy_to(std::ostream &os) {
			// unicode character
			if (*c & 0x80) {
				unsigned char _c = (*c);
				while (_c & 0x80) {
					os << (char)(*c);
					++c;
					_c <<= 1;
				}
			}
				// ANSI character
			else {
				os << (char)(*c);
				++c;
			}
		}

		utf8_iterator &operator++() {
			// unicode character
			if (*c & 0x80) {
				unsigned char _c = (*c);
				while (_c & 0x80) {
					++c;
					_c <<= 1;
				}
			}
				// ANSI character
			else {
				++c;
			}
			return *this;
		}

		utf8_iterator operator--(int) {
			utf8_iterator result = *this;
			--c;
			while ((*c & 0xc0)==0x80) {
				--c;
			}
			return result;
		}

		bool operator==(const utf8_iterator &it) const {
			return (this->c==it.c);
		}

		bool operator!=(const utf8_iterator &it) const {
			return (this->c!=it.c);
		}

		utf8_iterator operator--() {
			--c;
			while ((*c & 0xc0)==0x80) {
				--c;
			}
			return *this;
		}

		wchar_t operator*() const {
			return get_char();
		}

		/**
         * encode single UTF-8 character
         */
		static void encode(wchar_t c,std::ostream &os) {
			// ANSI character
			if (c < 0x80) {
				os << (char)c;
				return;
			}
			unsigned char buf[6];
			unsigned char _class = get_class(c);
			encode (c,buf,buf + _class,_class);
			const char *_buf = (const char*)buf;
			while (_class > 0) {
				os << *_buf;
				++_buf;
				--_class;
			}
		}

		bool isValid () const {
			return (c!=0);
		}

		/**
         * skip single invalid character
         * @return iterator pointing at the next character ( or end )
         */
		utf8_iterator fixChar(const utf8_iterator &end) const {
			const unsigned char *end_c = end.get_uraw();
			if (c==end_c) {
				return end;
			}
			unsigned char *cc = c;
			++cc;
			// stop at utf8 sequence start character or ANSI character
			while ((cc!=end_c) && (*cc & 0xc0)!=0xc0 && !(*cc < 0x80)) {
				++cc;
			}
			return utf8_iterator((const char*)cc);
		}

	protected :
		unsigned char *c;

		/**
         * encode utf-8 character @ specified location
         */
		static void encode(wchar_t c,unsigned char *begin,unsigned char *end,unsigned char _class) {
			do {
				--end;
				if (end==begin) {
					if (c > utf_mask.char_mask[_class]) {
						THROW (InputException, "utf-8 character encode failed");
					}
					*end = (unsigned char)c | utf_mask.utf_mask[_class];
				}
				else {
					*end = ((unsigned char)c & 0x3f) | 0x80; // 6 bits + uft-8 bit
					c >>= 6;
				}
			}
			while (end!=begin);
		}

		/**
         * evaluate UTF-8 character class
         */
		static unsigned char get_class(wchar_t c) {
			c >>= 7;
			// 1-st class character
			if (c==0) {
				return 1;
			}
			c >>= 4;
			// 2-nd class character
			if (c==0) {
				return 2;
			}
			c >>= 5;
			if (c==0) {
				return 3;
			}
			c >>= 5;
			if (c==0) {
				return 4;
			}
			c >>= 5;
			if (c==0) {
				return 5;
			}
			c >>= 5;
			if (c==0) {
				return 6;
			}
			THROW (InputException, "bad UNICODE character");
		}

		class utf_masks {
		public :
			unsigned char char_mask[7]; // character part mask ( by class )
			unsigned char utf_mask[7]; // utf mask ( by class )
			utf_masks ();
		};

		static utf_masks utf_mask;
	};

	class unicode_char_tables {
	public :
		// lower case eqivalents
		wchar_t lc_table[0x200];
		// upper case eqivalents
		wchar_t uc_table[0x200];
		// character ranks
		wchar_t ranks[0x200];

		wchar_t getRank(const wchar_t &c) {
			return (c < 0x200)?ranks[c]:c;
		}

		unicode_char_tables(bool case_sensitive);
	};
    
    /**
     * case sensitive unicode
     */
	class unicode_cs_char_traits {
	public :

		/**
         * get character's rank as used by comparators
         */
		static size_t char_rank(const wchar_t &c) {
			return c_tables.getRank(c);
		}

		static bool eq(const wchar_t &c0,const wchar_t &c1) {
			return (c0==c1);
		}

		static bool lt(const wchar_t &c0,const wchar_t &c1) {
			return (c_tables.getRank(c0) < c_tables.getRank(c1));
		}

		static bool gt(const wchar_t &c0,const wchar_t &c1) {
			return (c_tables.getRank(c0) > c_tables.getRank(c1));
		}

		static wchar_t to_upper(const wchar_t &c) {
			return (c < 0x200)?c_tables.uc_table[c]:c;
		}

		static wchar_t to_lower(const wchar_t &c) {
			return (c < 0x200)?c_tables.lc_table[c]:c;
		}

		static bool is_upper(const wchar_t &c) {
			return (c < 0x200)?(c == c_tables.uc_table[c]):true;
		}

		static bool is_lower(const wchar_t &c) {
			return (c < 0x200)?(c == c_tables.lc_table[c]):true;
		}

	private :
		static unicode_char_tables c_tables;
	};

    // case insensitive unicode
	class unicode_char_traits 
    {
	public :
		class white_set {
		public :
			bool operator()(wchar_t c) const {
				switch (c) {
					case ' ' : // ansi space character
					case '\r' : // carriage return
					case '\n' : // line feed
					case '\t' : // tab
					case 0xa0 : // non-breaking space ( nbsp )
					case 0xad : // soft hyphen
					case 0x200b : { // zero width space
						return true;
					}
						break;

					default : {
						return false;
					}
						break;
				}
			}
		};

		/**
         * non-printable characters
         */
		class shy_set {
		public :
			bool operator()(wchar_t c) const {
				switch (c) {
					case 0xad : // soft hyphen
					case 0x200b : { // zero width space
						return true;
					}
						break;

					default : {
						return false;
					}
						break;
				}
			}
		};

		/**
         * get character's rank as used by comparators
         */
		static size_t char_rank(const wchar_t &c) {
			return c_tables.getRank(c);
		}

		static bool eq(const wchar_t &c0,const wchar_t &c1) {
			return (c_tables.getRank(c0)==c_tables.getRank(c1));
		}

		static bool lt(const wchar_t &c0,const wchar_t &c1) {
			return (c_tables.getRank(c0) < c_tables.getRank(c1));
		}

		static bool gt(const wchar_t &c0,const wchar_t &c1) {
			return (c_tables.getRank(c0) > c_tables.getRank(c1));
		}

		static wchar_t to_upper(const wchar_t &c) {
			return (c < 0x200)?c_tables.uc_table[c]:c;
		}

		static wchar_t to_lower(const wchar_t &c) {
			return (c < 0x200)?c_tables.lc_table[c]:c;
		}

		static bool is_upper(const wchar_t &c) {
			return (c < 0x200)?(c == c_tables.uc_table[c]):true;
		}

		static bool is_lower(const wchar_t &c) {
			return (c < 0x200)?(c == c_tables.lc_table[c]):true;
		}

	private :
		static unicode_char_tables c_tables;
	};

	using utf8_ptr = str_ptr<utf8_iterator,unicode_cs_char_traits>;
    // case-insensitive UTF-8 pointer
	using utf8_ci_ptr = str_ptr<utf8_iterator,unicode_char_traits>;
	
	class unicode_char_set {
	public :
		bool char_set[0x200];
		unicode_char_set (utf8_ptr str_char_set);
		bool operator()(wchar_t c) const {
			return (c < 0x200)?char_set[c]:false;
		}

		/**
         * unicode alpha character set (latin)
         */
		static unicode_char_set latin_alpha_set();
	};

	std::ostream &operator<<(std::ostream &os,const db0::utf8_ptr &utf8_str);
	std::ostream &operator<<(std::ostream &os,const db0::utf8_ci_ptr &utf8_str);

    template <class iterator_, class _ct_>
    inline bool db0::str_ptr<iterator_, _ct_>::isNull() const {
        return (_begin.get_raw()==0 || _end.get_raw()==0);
    }
    
} 
