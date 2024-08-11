import pytest
from ops import add,substract,multiply,divide

def test_add():
    assert add(2, 3) == 5

def test_multiply():
    assert multiply(2, 3) == 6

def test_divide():
    assert divide(10, 5) == 2

def test_substract():
    assert substract(10, 5) == 5

#def test_multiply_wrong():
    #assert multiply(2, 3) == 6

