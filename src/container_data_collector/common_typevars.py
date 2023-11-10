"""Common TypeVarS for generic classes and functions.

Inner — a type for the inner container, in which elements are directly put.

Outer — a type for the outer container for collection and grouping elements.
If the number of groups is greater than zero, the Outer is a mapping container
(usually a plain dict), according to the keys which the outermost grouping
occurs. Otherwise, the Outer is the Inner.
"""

from typing import TypeVar


Inner = TypeVar("Inner")
Outer = TypeVar("Outer")
