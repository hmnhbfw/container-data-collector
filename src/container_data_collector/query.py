"""The Query `Tree`.

A simplified BNF of the Query `Tree` is presented below. (It only doesn't
specify what parameters the functions have and the correct number of these
functions.):

```
QueryTree ::= Element | At | ForEach | Branches

At ::= AtCall "." AtBranch
AtCall ::= "at(...)"
AtBranch ::= Group | Filter | QueryTree

ForEach ::= ForEachCall "." QueryTree
ForEachCall ::= "for_each()"

Branches ::= "branches(" BranchList ")"
BranchList ::= BranchListWithoutForEach
                | At "," AtOnlyBranchList
                | AtOnlyBranchList "," At "," AtOnlyBranchList
                | AtOnlyBranchList "," At
AtOnlyBranchList ::= AtOnly
                    | AtOnly "," AtOnlyBranchList

AtOnly ::= AtCall "." AtOnlyBranch
AtOnlyBranch ::= Element | Group | Filter | AtOnly | BranchesWithoutForEach

BranchesWithoutForEach ::= "branches(" BranchListWithoutForEach ")"
BranchListWithoutForEach ::= AtOnlyBranchList "," AtOnlyBranchList

Filter ::= Include | Exclude
Include ::= "include(...)"
Exclude ::= "exclude(...)"

Element ::= ElementCall | ElementCall "." Filter
ElementCall ::= "element(...)"

Group ::= GroupCall | GroupCall "." Filter
GroupCall ::= "group(...)"
```
"""

from collections.abc import Callable, Hashable
from dataclasses import KW_ONLY, dataclass, field
from enum import IntFlag, unique
from typing import Any, Container, Generic, Self, TypeAlias, TypeVar, final

from container_data_collector.nodes import (
    At,
    Element,
    Exclude,
    ForEach,
    Group,
    Include,
    Node,
    PseudoRoot,
)


@final
@dataclass(slots=True, eq=False, match_args=False)
class Tree:
    """Query `Tree` that represents a query by which collecting and grouping of
    elements takes place.
    """
    root: Node
    _: KW_ONLY
    n_elements: int
    n_groups: int


@final
@unique
class _TreePriority(IntFlag):
    """Key by which subtrees are sorted in a list of branches."""
    DEFAULT = 0
    FILTER = 1
    GROUP = 2
    ELEMENT = 4
    FOR_EACH = 8


@dataclass(slots=True, eq=False, match_args=False)
class TreeFactory:
    """Factory that generate a query tree from the current query chain builder
    and the upstream query chain builder, if the latest one exists.
    """
    _builder: "_ChainBuilder"

    def __call__(self, upstream_chain: "_ChainBuilder | None" = None) -> Tree:
        if not upstream_chain:
            return self._builder.build()
        upstream_chain.connect_with(self._builder)
        return upstream_chain.build()


@final
class _Trunk:
    pass

@final
class _Branch:
    pass

_TreePartT = TypeVar("_TreePartT", _Trunk, _Branch)

class _Terminal(TreeFactory, Generic[_TreePartT]):
    def __lt__(self, rhs: Self) -> bool:
        return self._builder.priority < rhs._builder.priority

    @classmethod
    def connect_branches(cls, upstream_chain: "_ChainBuilder",
                         first: Self, second: Self, *args: Self) -> None:
        """Connect the current node of the `upstream_chain` builder with passed
        `_Terminal` branches.
        
        Raise `RuntimeError`, if at least two branches have a `ForEach` node.
        """
        factories = [first, second, *args]
        factories.sort()
        if factories[-2]._builder.priority >= _TreePriority.FOR_EACH:
            msg = "There cannot be ForEach nodes in different branches simultaneously."
            raise RuntimeError(msg)

        upstream_chain.connect_with(*(factory._builder for factory in factories))


@final
@dataclass(slots=True, eq=False, match_args=False, kw_only=True)
class _ChainBuilder:
    _root: Node
    _current: Node
    _priority: _TreePriority = field(default=_TreePriority.DEFAULT)
    _elements: list[int] = field(default_factory=list)
    _groups: list[int] = field(default_factory=list)

    @classmethod
    def create(cls) -> Self:
        """Create the builder with a `PseudoRoot` node in the root."""
        root = PseudoRoot()
        return cls(_root=root, _current=root)

    @property
    def root(self) -> Node:
        """Return the root of the builder."""
        return self._root

    @property
    def priority(self) -> _TreePriority:
        """Return the current priority of the builder."""
        return self._priority

    @property
    def elements(self) -> list[int]:
        """Return a list of the positions of the elements."""
        return self._elements

    @property
    def groups(self) -> list[int]:
        """Return a list of the positions of the groups."""
        return self._groups

    def build(self) -> Tree:
        """Build the completed Tree.
        
        The `Tree` must have at least one `Element` node. Positions of `Element` and
        `Group` nodes, if the last ones are presented, must have end-to-end
        numbering and start with one. Otherwise, `RuntimeError` will be raised.
        """
        _check_positions_order(self._groups, "groups")
        _check_positions_order(self._elements, "elements")
        if not self._elements:
            msg = "There must be at leats one element in the query tree."
            raise RuntimeError(msg)

        return Tree(self._root, n_elements=len(self._elements), n_groups=len(self._groups))

    def connect_with(self, *args: Self) -> None:
        """Connect the current builder's node with the roots of the passed
        builders, and also update its other properties. 
        """
        if not args:
            return None
        if len(args) == 1:
            builder = args[0]
            self._priority |= builder.priority
            self._elements.extend(builder.elements)
            self._groups.extend(builder.groups)
            return None

        nodes = list[Node]()
        priority = _TreePriority.DEFAULT
        elements = list[int]()
        groups = list[int]()
        for builder in args:
            nodes.append(builder.root)
            priority |= builder.priority
            elements.extend(builder.elements)
            groups.extend(builder.groups)

        self._current.connect_with(*nodes)
        self._priority |= priority
        self._elements.extend(elements)
        self._groups.extend(groups)
        return None

    def element(self, pos: int = 1) -> None:
        """Create `Element` node and add it to the end of the builder chain."""
        node = Element(pos)
        self._current.connect_with(node)
        self._current = node
        self._priority |= _TreePriority.ELEMENT
        self._elements.append(pos)

    def group(self, level: int = 1, /, *, factory: Callable[[Any], Hashable] | None = None) -> None:
        """Create `Group` node and add it to the end of the builder chain."""
        node = Group(level, factory=factory)
        self._current.connect_with(node)
        self._current = node
        self._priority |= _TreePriority.GROUP
        self._groups.append(level)

    def include(self, /, *,
                any_of: Container[Any] | None = None,
                validator: Callable[[Any], bool] | None = None) -> None:
        """Create `Include` node and add it to the end of the builder chain."""
        node = Include(any_of=any_of, validator=validator)
        self._current.connect_with(node)
        self._current = node
        self._priority |= _TreePriority.FILTER

    def exclude(self, /, *,
                any_of: Container[Any] | None = None,
                invalidator: Callable[[Any], bool] | None = None) -> None:
        """Create `Exclude` node and add it to the end of the builder chain."""
        node = Exclude(any_of=any_of, invalidator=invalidator)
        self._current.connect_with(node)
        self._current = node
        self._priority |= _TreePriority.FILTER

    def at(self, key: Hashable) -> None:
        """Create `At` node and add it to the end of the builder chain."""
        node = At(key=key)
        self._current.connect_with(node)
        self._current = node

    def for_each(self) -> None:
        """Create `ForEach` node and add it to the end of the builder chain."""
        node = ForEach()
        self._current.connect_with(node)
        self._current = node
        self._priority |= _TreePriority.FOR_EACH

    def branches(self,
                 first: _Terminal[_Branch],
                 second: _Terminal[_Branch],
                 *args: _Terminal[_Branch]) -> None:
        """Connect the current builder's node with the roots of the passed
        branches."""
        _Terminal.connect_branches(self, first, second, *args)


def _check_positions_order(xs: list[int], xs_name: str) -> None:
    xs.sort()
    pos = 1
    missing_intervals = list[tuple[int, int]]()
    missing_positions = 0
    for x in xs:
        if x != pos:
            missing_intervals.append((pos, x - 1))
            missing_positions += x - pos
            pos = x
        pos += 1

    if missing_intervals:
        verb, s = ("is", "") if missing_positions == 1 else ("are", "s")
        msg = f"In the {xs_name}: there {verb} {missing_positions} missing position{s}."
        missings = [
            (
                str(interval[0])
                if interval[0] == interval[1]
                else f"{interval[0]}..{interval[1]}"
            )
            for interval in missing_intervals
        ]
        hint = f"Missing: {', '.join(missings)}."
        raise RuntimeError(msg + "\n" + hint)


@final
class _WithFilter(_Terminal[_TreePartT]):
    def include(self, /, *,
                any_of: Container[Any] | None = None,
                validator: Callable[[Any], bool] | None = None
                ) -> _Terminal[_TreePartT]:
        """The Query `Tree` node that correspondes to a filter, that takes
        an object, and checks if it is contained in the `any_of` container or
        it passes the `validator`. Otherwise, the whole branch until the closest
        ForEach node will be rejected.

        It always loses against `exclude` if it processes the same object.

        This method is one of the terminal methods of the Query `Tree` builder.
        """
        self._builder.include(any_of=any_of, validator=validator)
        return _Terminal(self._builder)

    def exclude(self, /, *,
                any_of: Container[Any] | None = None,
                invalidator: Callable[[Any], bool] | None = None
                ) -> _Terminal[_TreePartT]:
        """The Query `Tree` node that correspondes to a filter, that takes
        an object, and checks if it is not contained in the `any_of` container
        or it doesn't pass the `invalidator`. Otherwise, the whole branch until
        the closest ForEach node will be rejected.

        It always wins against `include` if it processes the same object.

        This method is one of the terminal methods of the Query `Tree` builder.
        """
        self._builder.exclude(any_of=any_of, invalidator=invalidator)
        return _Terminal(self._builder)


@final
class _AtChain(TreeFactory, Generic[_TreePartT]):
    def element(self, pos: int = 1) -> _WithFilter[_TreePartT]:
        """The Query `Tree` node that corresponds to getting the current value
        and applying it to the inserter partial function by the specified
        position.
        
        The position must be greater than zero, and if in the Query
        `Tree`, there are two elements: with position `1` and with the largest
        position `N`, then also there must be elements with position `2`, `3`,
        and so on up to `N-1`.

        This method can be treated as the terminal method of the Query `Tree`
        builder.

        Possible next methods: `include`, `exclude`.
        """
        self._builder.element(pos)
        return _WithFilter(self._builder)

    def group(self, level: int = 1,
              /, *,
              factory: Callable[[Any], Hashable] | None = None
              ) -> _WithFilter[_TreePartT]:
        """The Query `Tree` node that corresponds to getting the current value
        and grouping collecting elements by it. The level of the group is
        determined by value of the `level`.
        
        The `level` must be greater than zero, and if in the Query
        `Tree`, there are two groups: with level `1` and with the largest
        level `N`, then also there must be groups with level `2`, `3`,
        and so on up to `N-1`.

        This method can be treated as the terminal method of the Query `Tree`
        builder.

        Possible next methods: `include`, `exclude`.
        """
        self._builder.group(level, factory=factory)
        return _WithFilter(self._builder)

    def include(self, /, *,
                any_of: Container[Any] | None = None,
                validator: Callable[[Any], bool] | None = None
                ) -> _Terminal[_TreePartT]:
        """The Query `Tree` node that correspondes to a filter, that takes
        an object, and checks if it is contained in the `any_of` container or
        it passes the `validator`. Otherwise, the whole branch until the closest
        ForEach node will be rejected.

        It always loses against `exclude` if it processes the same object.

        This method is one of the terminal methods of the Query `Tree` builder.
        """
        self._builder.include(any_of=any_of, validator=validator)
        return _Terminal(self._builder)

    def exclude(self, /, *,
                any_of: Container[Any] | None = None,
                invalidator: Callable[[Any], bool] | None = None
                ) -> _Terminal[_TreePartT]:
        """The Query `Tree` node that correspondes to a filter, that takes
        an object, and checks if it is not contained in the `any_of` container
        or it doesn't pass the `invalidator`. Otherwise, the whole branch until
        the closest ForEach node will be rejected.

        It always wins against `include` if it processes the same object.

        This method is one of the terminal methods of the Query `Tree` builder.
        """
        self._builder.exclude(any_of=any_of, invalidator=invalidator)
        return _Terminal(self._builder)

    def at(self, key: Hashable) -> Self:
        """The Query `Tree` node that corresponds to a lookup by key in
        a `Mapping` container.

        Possible next methods: `element`, `group`, `include`, `exclude`, `at`,
        `for_each`, `branches`.
        """
        self._builder.at(key)
        return _AtChain(self._builder)

    def for_each(self) -> "_ForEachChain[_TreePartT]":
        """The Query `Tree` node that corresponds to an `Iterable` object, therefore,
        when getting subobjects by iteration over this `Iterable` object, all these
        subobjects will be processed one-by-one by the next node.

        Possible next methods: `element`, `at`, `for_each`, `branches`.
        """
        self._builder.for_each()
        return _ForEachChain(self._builder)

    def branches(self,
                 first: _Terminal[_Branch],
                 second: _Terminal[_Branch],
                 *args: _Terminal[_Branch]
                 ) -> _Terminal[_Branch]:
        """The way to make two or more branches in the Query `Tree`. The order, in
        which these branches list, is not important, but there shouldn't be two
        `for_each` in the different branches. All branches have to be started
        with `Branch.at(<some hashable key>)`.

        This method is one of the terminal methods of the Query `Tree` builder.
        """
        self._builder.branches(first, second, *args)
        return _Terminal(self._builder)


@final
class _ForEachChain(TreeFactory, Generic[_TreePartT]):
    def element(self, pos: int = 1) -> _WithFilter[_TreePartT]:
        """The Query `Tree` node that corresponds to getting the current value
        and applying it to the inserter partial function by the specified
        position.
        
        The position must be greater than zero, and if in the Query
        `Tree`, there are two elements: with position `1` and with the largest
        position `N`, then also there must be elements with position `2`, `3`,
        and so on up to `N-1`.

        This method can be treated as the terminal method of the Query `Tree`
        builder.

        Possible next methods: `include`, `exclude`.
        """
        self._builder.element(pos)
        return _WithFilter(self._builder)

    def at(self, key: Hashable) -> _AtChain[_TreePartT]:
        """The Query `Tree` node that corresponds to a lookup by key in
        a `Mapping` container.

        Possible next methods: `element`, `group`, `include`, `exclude`, `at`,
        `for_each`, `branches`.
        """
        self._builder.at(key)
        return _AtChain(self._builder)

    def for_each(self) -> Self:
        """The Query `Tree` node that corresponds to an `Iterable` object, therefore,
        when getting subobjects by iteration over this `Iterable` object, all these
        subobjects will be processed one-by-one by the next node.

        Possible next methods: `element`, `at`, `for_each`, `branches`.
        """
        self._builder.for_each()
        return _ForEachChain(self._builder)

    def branches(self,
                 first: _Terminal[_Branch],
                 second: _Terminal[_Branch],
                 *args: _Terminal[_Branch]
                 ) -> _Terminal[_Branch]:
        """The way to make two or more branches in the Query `Tree`. The order, in
        which these branches list, is not important, but there shouldn't be two
        `for_each` in the different branches. All branches have to be started
        with `Branch.at(<some hashable key>)`.

        This method is one of the terminal methods of the Query `Tree` builder.
        """
        self._builder.branches(first, second, *args)
        return _Terminal(self._builder)


class _ChainEntryPointBase(Generic[_TreePartT]):
    @classmethod
    def at(cls, key: Hashable) -> _AtChain[_TreePartT]:
        """The Query `Tree` node that corresponds to a lookup by key in
        a `Mapping` container.

        Possible next methods: `element`, `group`, `include`, `exclude`, `at`,
        `for_each`, `branches`.
        """
        builder = _ChainBuilder.create()
        builder.at(key)
        return _AtChain(builder)


@final
class _ChainEntryPoint(_ChainEntryPointBase[_TreePartT]):
    @classmethod
    def element(cls, pos: int = 1) -> _WithFilter[_Trunk]:
        """The Query `Tree` node that corresponds to getting the current value
        and applying it to the inserter partial function by the specified
        position.
        
        The position must be greater than zero, and if in the Query
        `Tree`, there are two elements: with position `1` and with the largest
        position `N`, then also there must be elements with position `2`, `3`,
        and so on up to `N-1`.

        This method can be treated as the terminal method of the Query `Tree`
        builder.

        Possible next methods: `include`, `exclude`.
        """
        builder = _ChainBuilder.create()
        builder.element(pos)
        return _WithFilter(builder)

    @classmethod
    def for_each(cls) -> _ForEachChain[_Trunk]:
        """The Query `Tree` node that corresponds to an `Iterable` object, therefore,
        when getting subobjects by iteration over this `Iterable` object, all these
        subobjects will be processed one-by-one by the next node.

        Possible next methods: `element`, `at`, `for_each`, `branches`.
        """
        builder = _ChainBuilder.create()
        builder.for_each()
        return _ForEachChain(builder)

    @classmethod
    def branches(cls,
                 first: _Terminal[_Branch],
                 second: _Terminal[_Branch],
                 *args: _Terminal[_Branch]) -> TreeFactory:
        """The way to make two or more branches in the Query `Tree`. The order, in
        which these branches list, is not important, but there shouldn't be two
        `for_each` in the different branches. All branches have to be started
        with `Branch.at(<some hashable key>)`.

        This method is one of the terminal methods of the Query `Tree` builder.
        """
        builder = _ChainBuilder.create()
        builder.branches(first, second, *args)
        return TreeFactory(builder)


Query: TypeAlias = _ChainEntryPoint[_Trunk]
Branch: TypeAlias = _ChainEntryPointBase[_Branch]
