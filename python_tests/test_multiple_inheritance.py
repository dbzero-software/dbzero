# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0


@db0.memo
class ProductBase:
    def __init__(self, name, price):
        self.name = name
        self.price = price
        
    def get_int_price(self):
        return int(self.price)
    
    
class HasComponents:
    def __init__(self, components):
        self.components = components
    
    def get_last_component(self):
        if self.components:
            return self.components[-1]
        return None

@db0.memo
class ComplexProduct(ProductBase, HasComponents):
    def __init__(self, name, price, components):
        ProductBase.__init__(self, name, price)
        HasComponents.__init__(self, components)
    
    
def test_multi_base_memo_class_init(db0_fixture):
    obj_1 = ComplexProduct("Widget", 19.99, ["Part A", "Part B"])
    assert obj_1.name == "Widget"
    assert obj_1.price == 19.99
    assert obj_1.components == ["Part A", "Part B"]
    
    
def test_multi_base_memo_class_call_base_members(db0_fixture):
    obj_1 = ComplexProduct("Widget", 19.99, ["Part A", "Part B"])
    assert obj_1.get_int_price() == 19
    assert obj_1.get_last_component() == "Part B"
        
        
def test_multi_base_memo_class_find(db0_fixture):
    obj_1 = ComplexProduct("Widget", 19.99, ["Part A", "Part B"])
    db0.tags(obj_1).add("test_tag")
    assert next(iter(db0.find(ComplexProduct, "test_tag"))) is obj_1
    