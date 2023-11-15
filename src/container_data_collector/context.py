"""Shared context for tree traversal."""

from collections.abc import Hashable
from dataclasses import dataclass
from typing import Any, Generic

from container_data_collector.common_typevars import Inner, Outer
from container_data_collector.vector_partial import VectorPartial


@dataclass(slots=True, eq=False, match_args=False)
class Context(Generic[Outer, Inner]):
    """Structure that joins two functions:
      - an element inserter;
      - a group factory.
    
    It describes the current state of collection of elements and grouping them
    during tree traversal.
    """

    _element_inserter: VectorPartial[Inner, None]
    _group_factory: VectorPartial[Outer, Inner]

    def __init__(self, /, *,
                 top_container: Outer,
                 inserter: VectorPartial[Inner, None],
                 group_factory: VectorPartial[Outer, Inner]) -> None:
        self._element_inserter = inserter
        self._group_factory = group_factory
        if group_factory.args_count == 1:
            self._element_inserter.insert(top_container, pos=1)
        else:
            self._group_factory.insert(top_container, pos=1)

    def apply_element(self, e: Any, /, *, pos: int) -> None:
        """Apply the passed element to the function-inserter. Then if
        the function can be called, it will be.

        Raise `ValueError`, if `pos` is 0.
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

        Raise `ValueError`, if `pos` is 0.
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
