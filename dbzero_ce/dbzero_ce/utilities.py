from typing import Iterator

_NORMALIZE_TRANSLATION_SOURCE = 'ĄĆĘŁŃÓŚŻŹ'
_NORMALIZE_TRANSLATION_MAPPINGS = 'ACELNOSZZ'
_NORMALIZE_TRANSLATION_TABLE = str.maketrans(_NORMALIZE_TRANSLATION_SOURCE, _NORMALIZE_TRANSLATION_MAPPINGS)

_EXTRA_SPLIT_DELIMITERS = '.,;:-!?\t\n"\''
_EXTRA_SPLIT_TABLE = str.maketrans(_EXTRA_SPLIT_DELIMITERS, ' ' * len(_EXTRA_SPLIT_DELIMITERS))

def taggify(input_text: str, max_len = 3, min_len = 1) -> Iterator[str]:
    """
    This function tokenizes an arbitrary string and breaks it into an iterable of tags - which are constructed by taking a prefix of up to
    a specific length, removing any whitespaces and delimiters and normalizing to uppercase and latin characters.
    Parameters:
    input_text: the arbitrary input text (unicode)
    max_len: the maximum prefix length (must be > 0), if None then return unlimited length tags
    min_len: the minimum prefix length to be returned
    Returns:
    An iterable of unique tags (see example below)

    Example:
    # returns ["MINS", "MAZO"]
    db0.taggify("--Mińsk Mazowiecki", max_len = 4)

    # returns ["MAR"]
    db0.taggify("Markowski, Marek", max_len = 3)

    # returns ["KOW"]
    db0.taggify("A.Kowalski", min_len = 3)

    # returns ["A", "KOWALSKI"]
    db0.taggify("A.Kowalski", max_len = None)

    How input text is processed:
    * The input is split into tokens by whitespace (and other delimiters, like '.')
    * Non-alphanumeric characters are removed
    * Tokens shorter than 'min_len' are filtered out
    * Letters are converted to upper-case. Diacritic characters are transliterated to latin counterparts.
    * Slice of 'max_len' characters is returned for each token
    """

    yielded_tags = set()
    for token in input_text.translate(_EXTRA_SPLIT_TABLE).split():
        stripped_token = ''.join(filter(str.isalnum, token))
        if len(stripped_token) >= min_len:
            # Only allow tokens longer than 'min_len'
            normalized_token = stripped_token[:max_len].upper().translate(_NORMALIZE_TRANSLATION_TABLE)
            if normalized_token not in yielded_tags:
                # Tags must be unique
                yielded_tags.add(normalized_token)
                yield normalized_token
