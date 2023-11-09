"""Shared context for tree traversal."""

from collections.abc import Hashable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from container_data_collector.vector_partial import (
    FuncWithFixedArgType,
    VectorPartial,
)


TopContainer = TypeVar("TopContainer")
BottomContainer = TypeVar("BottomContainer")

@dataclass(slots=True, eq=False, match_args=False)
class Context(Generic[TopContainer, BottomContainer]):
    """Structure that joins two functions:
      - an element inserter;
      - a group factory.
    
    It describes the current state of collection of elements and grouping them
    during tree traversal.
    """

    _element_inserter: VectorPartial[BottomContainer, None]
    _group_factory: VectorPartial[TopContainer, BottomContainer]

    def __init__(self, /, *,
                 inserter: FuncWithFixedArgType[BottomContainer, None],
                 n_elements: int,
                 group_factory: FuncWithFixedArgType[TopContainer, BottomContainer],
                 n_groups: int,
                 top_container: TopContainer) -> None:
        self._element_inserter = VectorPartial(inserter, n_args=n_elements+1)
        self._group_factory = VectorPartial(group_factory, n_args=n_groups+1)
        if not n_groups:
            self._element_inserter.insert(top_container, pos=1)
        else:
            self._group_factory.insert(top_container, pos=1)

    def apply_element(self, e: Any, /, *, pos: int) -> None:
        """Apply the passed element to the function-inserter. Then if
        the function can be called, it will be.

        Raise 'ValueError', if 'pos' is 0.
        """
        Context._check_pos(pos)
        self._element_inserter.insert(e, pos=pos+1)
        if self._element_inserter.can_return:
            self._element_inserter()
            self._element_inserter.remove(pos + 1)

    def create_group(self, key: Hashable, /, *, pos: int) -> None:
        """Apply the passed key to the group factory function. Then if
        the function can be called, it will be, and the a new bottom container
        will be apply to the function-inserter as the first argument.

        Raise 'ValueError', if 'pos' is 0.
        """
        Context._check_pos(pos)
        self._group_factory.insert(key, pos=pos+1)
        if self._group_factory.can_return:
            bottom_container = self._group_factory()
            self._group_factory.remove(pos + 1)
            self._element_inserter.insert(bottom_container, pos=1)

    @staticmethod
    def _check_pos(pos: int) -> None:
        if pos == 0:
            msg = "Manual applying a container to the function is not allowed."
            hint = f"{pos=} is forbidden."
            raise ValueError(msg + "\n" + hint)
