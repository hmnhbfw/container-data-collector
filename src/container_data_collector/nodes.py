"""Nodes of the query tree."""

from collections.abc import Callable, Container, Hashable, Iterable
from dataclasses import KW_ONLY, dataclass, field
from enum import IntEnum, auto, unique
from typing import Any, Generic, Protocol

from container_data_collector.common_typevars import Inner, Outer
from container_data_collector.context import Context


@unique
class State(IntEnum):
    """State at which node processing is completed."""
    SUCCESS = auto()
    REJECT = auto()


class Node(Protocol[Outer, Inner]):
    """Atomic unit of the query tree. Each node takes an object processed by
    previous node and the current context to know how process this object.
    The result of the processing is some State, that can trigger some events
    in the previous node.
    """
    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        ...


_DATACLASS_KWARGS = {
    # NOTE: frozen=True causes exception raising , probably, due to some runtime bug
    # a sample of code is below:
    #
    # from dataclasses import dataclass
    # from typing import Generic, TypeVar
    #
    # T = TypeVar("T")
    # @dataclass(frozen=True, slots=True)
    # class Test(Generic[T]):
    #     x: T
    #
    # Test[int](3)
    #
    # Traceback (most recent call last):
    #   File "....py", line 9, in <module>
    #     Test[int](3)
    #   File "...\Python\Python311\Lib\typing.py", line 1270, in __call__
    #     result.__orig_class__ = self
    #     ^^^^^^^^^^^^^^^^^^^^^
    #   File "<string>", line 5, in __setattr__
    # TypeError: super(type, obj): obj must be an instance or subtype of type
    #
    # "frozen": True,
    "slots": True,
    "eq": False,
    "match_args": False,
}


@dataclass(**_DATACLASS_KWARGS)
class Element(Generic[Outer, Inner]):
    """Node that represents one of the elements collecting during tree traversal.
    Position tells the inserter function what position the element has in it.
    Next node may point to additional filters, e.g., can this element be used or not.
    """
    pos: int
    _: KW_ONLY
    next_node: Node[Outer, Inner] | None = field(default=None)

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        if self.next_node:
            match self.next_node.process(obj, context):
                case State.SUCCESS:
                    pass
                case State.REJECT as reject:
                    return reject

        context.apply_element(obj, pos=self.pos)
        return State.SUCCESS


@dataclass(**_DATACLASS_KWARGS)
class Group(Generic[Outer, Inner]):
    """Node that represents one of the keys by which the elements will be grouped.
    Level tells what level of grouping this key is related. Factory tells how
    to make the key hashable if it is not. Next node may point to additional
    filters, e.g., can this key be used or not.
    """
    level: int
    _: KW_ONLY
    factory: Callable[[Any], Hashable] | None = field(default=None)
    next_node: Node[Outer, Inner] | None = field(default=None)

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        if self.factory is not None:
            obj = self.factory(obj)
        if self.next_node:
            match self.next_node.process(obj, context):
                case State.SUCCESS:
                    pass
                case State.REJECT as reject:
                    return reject

        context.create_group(obj, pos=self.level)
        return State.SUCCESS


@dataclass(**_DATACLASS_KWARGS, kw_only=True)
class Include(Generic[Outer, Inner]):
    """Node that takes an object from the previous node and checks it.
    If it is not contained in the 'include' container or it doesn't pass
    the check of the 'validator' callable object, the whole branch until
    the closest FromList node will be rejected.

    If both 'include' and 'validator' are not defined, the node processing
    has no effect.
    """
    include: Container[Any] | None = field(default=None)
    validator: Callable[[Any], bool] | None = field(default=None)

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        if self.include is not None and obj not in self.include:
            return State.REJECT
        if self.validator is not None and not self.validator(obj):
            return State.REJECT
        return State.SUCCESS


@dataclass(**_DATACLASS_KWARGS, kw_only=True)
class Exclude(Generic[Outer, Inner]):
    """Node that takes an object from the previous node and checks it.
    If it is contained in the 'exclude' container or it passes the check of
    the 'invalidator' callable object, the whole branch until the closest
    FromList node will be rejected.

    If both 'exclude' and 'invalidator' are not defined, the node processing
    has no effect.
    """
    exclude: Container[Any] | None = field(default=None)
    invalidator: Callable[[Any], bool] | None = field(default=None)

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        if self.exclude is not None and obj in self.exclude:
            return State.REJECT
        if self.invalidator is not None and self.invalidator(obj):
            return State.REJECT
        return State.SUCCESS


@dataclass(**_DATACLASS_KWARGS, kw_only=True)
class At(Generic[Outer, Inner]):
    """Node that takes an object from the previous node and treats it as
    a Mapping object, gets the value by key and propogates this value to
    the next nodes.
    """
    next_nodes: Iterable[Node[Outer, Inner]]
    key: Hashable

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        value = obj[self.key]
        for node in self.next_nodes:
            match node.process(value, context):
                case State.SUCCESS:
                    continue
                case State.REJECT as reject:
                    return reject
        return State.SUCCESS


class KeyExistence(IntEnum):
    """Stub to make an emphasis that a key doesn't exist."""
    NONE = auto()


@dataclass(**_DATACLASS_KWARGS, kw_only=True)
class FromList(Generic[Outer, Inner]):
    """Node that takes an object from the previous node and treats it in two
    possible ways:
      - if there is no key, then the object is treated as an Iterable object,
    iterates over it, and propogates each result to the next nodes.
      - if there is a key, then the object is treated as a Mapping object,
    gets the value by key, and then treats the value as an Iterable object, and
    so forth (see the previous way).
    """
    next_nodes: Iterable[Node[Outer, Inner]]
    key: Hashable | KeyExistence = field(default=KeyExistence.NONE)

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        if self.key is not KeyExistence.NONE:
            obj = obj[self.key]
        for value in obj:
            for node in self.next_nodes:
                node.process(value, context)
        return State.SUCCESS
