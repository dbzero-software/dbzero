# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from typing import Iterator

_NORMALIZE_TRANSLATION_SOURCE = 'ĄĆĘŁŃÓŚŻŹ'
_NORMALIZE_TRANSLATION_MAPPINGS = 'ACELNOSZZ'
_NORMALIZE_TRANSLATION_TABLE = str.maketrans(_NORMALIZE_TRANSLATION_SOURCE, _NORMALIZE_TRANSLATION_MAPPINGS)

_EXTRA_SPLIT_DELIMITERS = '.,;:-!?\t\n"\''
_EXTRA_SPLIT_TABLE = str.maketrans(_EXTRA_SPLIT_DELIMITERS, ' ' * len(_EXTRA_SPLIT_DELIMITERS))

def taggify(input_text: str, max_len: int = 3, min_len: int = 1, suffix: bool = False) -> Iterator[str]:
    """Tokenize an arbitrary string and convert it into an iterable of normalized tags.

    This function breaks text into tokens, removes whitespaces and delimiters, and 
    normalizes to uppercase Latin characters. Tags are constructed by taking a prefix 
    (or suffix) of up to a specific length from each token.

    Parameters
    ----------
    input_text : str
        The arbitrary input text (unicode) to tokenize.
    max_len : int, default 3
        The maximum prefix length (must be > 0).
    min_len : int, default 1
        The minimum prefix length to be returned. Tokens shorter than this are filtered out.
    suffix : bool, default False
        If True, take suffix instead of prefix from tokens.

    Returns
    -------
    Iterator[str]
        An iterator of unique tags.

    Examples
    --------
    Basic usage with maximum length:
    
    >>> list(dbzero.taggify("--Mińsk Mazowiecki", max_len=4))
    ['MINS', 'MAZO']

    Filtering by maximum length:
    
    >>> list(dbzero.taggify("Markowski, Marek", max_len=3))
    ['MAR']

    Filtering by minimum length:
    
    >>> list(dbzero.taggify("A.Kowalski", min_len=3))
    ['KOW']

    Unlimited length tags:
    
    >>> list(dbzero.taggify("A.Kowalski", max_len=None))
    ['A', 'KOWALSKI']

    Notes
    -----
    Text Processing Steps:
    
    1. The input is split into tokens by whitespace and delimiters (.,;:-!?\\t\\n"')
    2. Non-alphanumeric characters are removed from each token
    3. Tokens shorter than `min_len` are filtered out
    4. Letters are converted to upper-case
    5. Diacritic characters are transliterated to Latin counterparts (Ą→A, Ć→C, etc.)
    6. A slice of `max_len` characters is taken from each token
    7. Only unique tags are yielded
    """

    yielded_tags = set()
    for token in input_text.translate(_EXTRA_SPLIT_TABLE).split():
        stripped_token = ''.join(filter(str.isalnum, token))
        if len(stripped_token) >= min_len:
            # Only allow tokens longer than 'min_len'
            stripped_token = stripped_token[-max_len:] if suffix else stripped_token[:max_len]
            normalized_token = stripped_token.upper().translate(_NORMALIZE_TRANSLATION_TABLE)
            if normalized_token not in yielded_tags:
                # Tags must be unique
                yielded_tags.add(normalized_token)
                yield normalized_token
