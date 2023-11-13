"""Nodes of the query tree."""

from collections.abc import Callable, Container, Hashable
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

    def attach(self, *args: "Node[Outer, Inner]") -> None:
        ...


@dataclass(slots=True, eq=False, match_args=False)
class PseudoRoot(Generic[Outer, Inner]):
    """Pseudo root node of the query tree."""
    next_nodes: list[Node[Outer, Inner]] = field(default_factory=list, init=False)

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        for node in self.next_nodes:
            match node.process(obj, context):
                case State.SUCCESS:
                    continue
                case State.REJECT as reject:
                    return reject
        return State.SUCCESS

    def attach(self, *args: "Node[Outer, Inner]") -> None:
        self.next_nodes.extend(args)


@dataclass(slots=True, eq=False, match_args=False)
class Element(Generic[Outer, Inner]):
    """Node that represents one of the elements collecting during tree traversal.
    Position tells the inserter function what position the element has in it.
    Next node may point to additional filters, e.g., can this element be used or not.
    """
    pos: int
    _: KW_ONLY
    next_node: Node[Outer, Inner] | None = field(default=None, init=False)

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        if self.next_node:
            match self.next_node.process(obj, context):
                case State.SUCCESS:
                    pass
                case State.REJECT as reject:
                    return reject

        context.apply_element(obj, pos=self.pos)
        return State.SUCCESS

    def attach(self, *args: Node[Outer, Inner]) -> None:
        if len(args) != 1:
            raise ValueError("The Element node can have only one node.")
        self.next_node = args[0]


@dataclass(slots=True, eq=False, match_args=False)
class Group(Generic[Outer, Inner]):
    """Node that represents one of the keys by which the elements will be grouped.
    Level tells what level of grouping this key is related. Factory tells how
    to make the key hashable if it is not. Next node may point to additional
    filters, e.g., can this key be used or not.
    """
    level: int
    _: KW_ONLY
    factory: Callable[[Any], Hashable] | None = field(default=None)
    next_node: Node[Outer, Inner] | None = field(default=None, init=False)

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

    def attach(self, *args: Node[Outer, Inner]) -> None:
        if len(args) != 1:
            raise ValueError("The Group node can have only one node.")
        self.next_node = args[0]


@dataclass(slots=True, eq=False, match_args=False, kw_only=True)
class Include(Generic[Outer, Inner]):
    """Node that takes an object from the previous node and checks it.
    If it is not contained in the 'any_of' container or it doesn't pass
    the check of the 'validator' callable object, the whole branch until
    the closest FromList node will be rejected.

    If both 'any_of' and 'validator' are not defined, the node processing
    has no effect.
    """
    any_of: Container[Any] | None = field(default=None)
    validator: Callable[[Any], bool] | None = field(default=None)

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        if self.any_of is not None and obj not in self.any_of:
            return State.REJECT
        if self.validator is not None and not self.validator(obj):
            return State.REJECT
        return State.SUCCESS

    def attach(self, *args: Node[Outer, Inner]) -> None:
        class_name = self.__class__.__name__
        msg = f"The terminal node '{class_name}' is a leaf, so it can't have any attached nodes."
        raise NotImplementedError(msg)


@dataclass(slots=True, eq=False, match_args=False, kw_only=True)
class Exclude(Generic[Outer, Inner]):
    """Node that takes an object from the previous node and checks it.
    If it is contained in the 'any_of' container or it passes the check of
    the 'invalidator' callable object, the whole branch until the closest
    FromList node will be rejected.

    If both 'any_of' and 'invalidator' are not defined, the node processing
    has no effect.
    """
    any_of: Container[Any] | None = field(default=None)
    invalidator: Callable[[Any], bool] | None = field(default=None)

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        if self.any_of is not None and obj in self.any_of:
            return State.REJECT
        if self.invalidator is not None and self.invalidator(obj):
            return State.REJECT
        return State.SUCCESS

    def attach(self, *args: Node[Outer, Inner]) -> None:
        class_name = self.__class__.__name__
        msg = f"The terminal node '{class_name}' is a leaf, so it can't have any attached nodes."
        raise NotImplementedError(msg)


@dataclass(slots=True, eq=False, match_args=False, kw_only=True)
class At(Generic[Outer, Inner]):
    """Node that takes an object from the previous node and treats it as
    a Mapping object, gets the value by key and propogates this value to
    the next nodes.
    """
    next_nodes: list[Node[Outer, Inner]] = field(default_factory=list, init=False)
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

    def attach(self, *args: Node[Outer, Inner]) -> None:
        self.next_nodes.extend(args)


class KeyExistence(IntEnum):
    """Stub to make an emphasis that a key doesn't exist."""
    NONE = auto()


@dataclass(slots=True, eq=False, match_args=False, kw_only=True)
class FromList(Generic[Outer, Inner]):
    """Node that takes an object from the previous node and treats it in two
    possible ways:
      - if there is no key, then the object is treated as an Iterable object,
    iterates over it, and propogates each result to the next nodes.
      - if there is a key, then the object is treated as a Mapping object,
    gets the value by key, and then treats the value as an Iterable object, and
    so forth (see the previous way).
    """
    next_nodes: list[Node[Outer, Inner]] = field(default_factory=list, init=False)
    key: Hashable | KeyExistence = field(default=KeyExistence.NONE)

    def process(self, obj: Any, context: Context[Outer, Inner]) -> State:
        if self.key is not KeyExistence.NONE:
            obj = obj[self.key]
        for value in obj:
            for node in self.next_nodes:
                node.process(value, context)
        return State.SUCCESS

    def attach(self, *args: Node[Outer, Inner]) -> None:
        self.next_nodes.extend(args)
