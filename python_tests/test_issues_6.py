# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestSingleton
from typing import Iterable, Iterator, List, Set
import random
import string


_NORMALIZE_TRANSLATION_SOURCE = 'ĄĆĘŁŃÓŚŻŹ'
_NORMALIZE_TRANSLATION_MAPPINGS = 'ACELNOSZZ'
_NORMALIZE_TRANSLATION_TABLE = str.maketrans(_NORMALIZE_TRANSLATION_SOURCE, _NORMALIZE_TRANSLATION_MAPPINGS)
def normalize_string(input_string: str) -> str:
    """
    Make normalized representation of given input string.
    All letters are converted to upper-case. Characters other than letters and digits are removed.
    Polish diacritic characters are transliterated to latin counterparts
    """
    result = ''.join(filter(str.isalnum, input_string.upper()))
    return result.translate(_NORMALIZE_TRANSLATION_TABLE)


def normalize_tokens(input_string: str) -> Iterator[str]:
    """
    Split input string into tokens by whitespace and apply normalization function
    """
    return (normalize_string(token) for token in input_string.split())


_YIELD_TAGS_LENGTH = 4
def yield_tags(input_phrase: str) -> Iterator[str]:
    """
    Generate tags from a given phrase.
    A phrase is split into tokens by whitespace and following rules are applied:
    * Non-alphanumeric characters are removed
    * Letters are converted to upper-case
    * Slice of max first 4 characters is returned for each token
    """
    return (token[:_YIELD_TAGS_LENGTH] for token in normalize_tokens(input_phrase))


@db0.memo
class Client():
    def __init__(
            self,
            client_id: int,
            first_name: str,
            last_name: str,
            email: str,
            phone_number: str           
    ):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.phone_number = phone_number
        self.default_address = None
        self._ix_addresses = db0.index()
        self._shopping_cart: None
        self._ix_orders = db0.index()
        self._ix_schedule_from = db0.index()
        self._ix_schedule_to = db0.index()        
        self._lp_balance = 0
        self.lp_history = []
        self.client_id = client_id

        db0.tags(self).add(self.get_tags())
        db0.tags(self).add(self.get_client_id_tags())

    def get_client_id_tags(self):
        client_id_str = str(self.client_id)
        if len(client_id_str) < 3:
            return client_id_str.zfill(3)
        return [client_id_str[:3], client_id_str[-3:]]
    
    def get_tags(self) -> Iterator:
        yield from yield_tags(self.first_name)
        yield from yield_tags(self.last_name)
        yield from yield_tags(self.email)
        yield self.phone_number.get_tag()
        if self.default_address is not None:
            yield from self.default_address.get_tags()
    

def test_signup_clients_issue_1(db0_no_autocommit):
    """
    Test was failing with: db0::SlabManager::FindResult db0::SlabManager::find(uint32_t): Assertion `false' failed.
    null address accessed from Dict::getItem
    Resolution: problem with upgrading WideLock to a newer version (residual lock not refreshed)
    """
    px_name = db0.get_current_prefix().name
    def rand_string(max_len):
        str_len = random.randint(1, max_len)
        return ''.join(random.choice(string.ascii_letters) for i in range(str_len))
    
    db0.set_cache_size(64 << 20)
    clients = db0.dict()
    root = MemoTestSingleton(clients)
    
    first_names = ["John", "Jane", "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", 
                   "Ivan", "Judy", "Mallory", "Niaj", "Olivia", "Peggy", "Rupert", "Sybil", "Trent"]
    
    last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", 
                  "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", 
                  "Thompson", "Garcia", "Martinez", "Robinson"]
    
    email_domains = ["example.com", "test.com", "demo.com", "sample.com", 
                     "mail.com", "web.com", "service.com", "company.com", "business.com", "org.com", "net.com",
                    "info.com", "support.com", "contact.com", "hello.com", "world.com"]
    
    from .data_for_tests import test_logins, test_large_ints
    
    def validate(clients):
        num_clients = 0
        for k, v in clients.items():
            num_clients += 1

    commit_interval = 10
    # NOTE: 780 works fine, something breaks at 790
    for count in range(790):
        rand_int = test_large_ints[count % len(test_large_ints)]
        rand_int *= 33
        first_name = first_names[rand_int % len(first_names)]
        rand_int += 17
        last_name = last_names[rand_int % len(last_names)]
        rand_int += 7
        email = f"{first_name}_{test_logins[rand_int % len(test_logins)]}@{email_domains[rand_int % len(email_domains)]}"        
        phone_number = f"+{rand_int * 100}"

        clients[email] = Client(count + 1, first_name, last_name, email, phone_number)
        
        if count % commit_interval == 0:
            if count == 780 and 'D' in db0.build_flags():
                db0.dbg_start_logs()
            db0.commit()
    
    db0.close()
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    # validate snapshots from disk    
    with db0.snapshot({px_name: 66}) as snap:
        root = snap.fetch(MemoTestSingleton)        
        validate(root.value)
        