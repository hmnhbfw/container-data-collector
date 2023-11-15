"""The `Collector` class and its factories for more convenient work."""

import copy
from collections.abc import Callable, Hashable, Iterable
from dataclasses import dataclass
from typing import Any, Concatenate, Generic, final

from container_data_collector.common_typevars import Inner, Inner_contra, Outer
from container_data_collector.context import Context
from container_data_collector.query import Tree, TreeFactory
from container_data_collector.vector_partial import VectorPartial


@dataclass(slots=True, eq=False, match_args=False)
class _Collector(Generic[Outer, Inner]):
    _query_tree: Tree
    _outer_factory: Callable[[], Outer]
    _element_inserter: VectorPartial[Inner, None]
    _group_factory: VectorPartial[Outer, Inner]

    def collect(self, objects: Iterable[Any]) -> Outer:
        """Take all elements of the `Iterable` object and collect it according to
        the structure of the query tree, then returning the result.
        """
        result = self._outer_factory()
        context = Context[Outer, Inner](
            top_container=result,
            inserter=copy.copy(self._element_inserter),
            group_factory=copy.copy(self._group_factory),
        )

        for obj in objects:
            self._query_tree.root.process(obj, context)
        return result


@final
class PlainCollector(Generic[Inner_contra]):
    """This `Collector` factory is for those collectors that do no need to work
    with groups. If the query tree doesn't have any group elements, this factory
    should be used.
    """

    @classmethod
    def compile(cls, query: TreeFactory,
                /, *,
                inner_factory: Callable[[], Inner],
                inserter: Callable[Concatenate[Inner_contra, ...], None],
                ) -> _Collector[Inner, Inner]:
        """Return the `Collector` instance.
        
        The `query` argument is a structure of the query tree. It describes
        how the `Collector` should traverse passed objects during collecting.
        The query tree must have zero or only one path from the root to some
        leaf, that has `for_each` node. The query tree must have at least
        one `element` node. Integer numbers, that passed into `element` and
        `group` nodes, must have end-to-end numbering and start with one, e.g.
        1, 2, 3, if there are only three positions. Otherwise, it raises
        'RuntimeError'.

        The query has to be started with `Query.<suitable method>`.

        The `inserter` argument is about how collecting elements have to be
        inserted. This `Callable` object must have the first argument as some
        container in which the elements will be inserted, and the rest of
        the arguments are the elements themselves, which positions have to be
        the same as in the `element` nodes. The `Callable` object can't have any
        default positional arguments and non-default keyword arguments.
        Otherwise, it raises `TypeError`.

        In the other hand, the `inner_factory` argument says how to get
        the container in which the elements will be inserted in the way
        the `inserter` describes it. It have to be a plain producer-like
        `Callable` object, that receives no arguments and returns the container.
        """
        def _group_factory_0(c: Inner) -> Inner:
            return c

        tree = query()
        element_inserter = VectorPartial(inserter, n_args=tree.n_elements+1)
        if tree.n_groups:
            msg = "There cannot be any groups in the query using ListCollector."
            hint = f"Instead, {tree.n_groups} {'is' if tree.n_groups == 1 else 'are'} given."
            raise ValueError(msg + "\n" + hint)

        group_factory = VectorPartial(_group_factory_0, n_args=1)
        return _Collector(tree, inner_factory, element_inserter, group_factory)


@final
class GroupCollector(Generic[Inner_contra]):
    """This `Collector` factory is for those collectors that do need to work with
    groups. If the query tree has any group elements, this factory should be used.
    """

    @classmethod
    def compile(cls, query: TreeFactory,
                /, *,
                inner_factory: Callable[[], Inner],
                inserter: Callable[Concatenate[Inner_contra, ...], None],
                ) -> _Collector[dict[Hashable, Any], Inner]:
        """Return the `Collector` instance.
        
        The `query` argument is a structure of the query tree. It describes
        how the `Collector` should traverse passed objects during collecting.
        The query tree must have zero or only one path from the root to some
        leaf, that has `for_each` node. The query tree must have at least
        one `element` node. Integer numbers, that passed into `element` and
        `group` nodes, must have end-to-end numbering and start with one, e.g.
        1, 2, 3, if there are only three positions. Otherwise, it raises
        'RuntimeError'.

        The query has to be started with `Query.<suitable method>`.

        The `inserter` argument is about how collecting elements have to be
        inserted. This `Callable` object must have the first argument as some
        container in which the elements will be inserted, and the rest of
        the arguments are the elements themselves, which positions have to be
        the same as in the `element` nodes. The `Callable` object can't have any
        default positional arguments and non-default keyword arguments.
        Otherwise, it raises `TypeError`.

        In the other hand, the `inner_factory` argument says how to get
        the container in which the elements will be inserted in the way
        the `inserter` describes it. It have to be a plain producer-like
        `Callable` object, that receives no arguments and returns the container.
        """
        def _group_factory_1(c: dict[Hashable, Any], key_1: Hashable) -> Inner:
            return c.setdefault(key_1, inner_factory())

        def _group_factory_2(c: dict[Hashable, Any], key_1: Hashable, key_2: Hashable) -> Inner:
            return c.setdefault(
                    key_1, {}
                ).setdefault(key_2, inner_factory())

        def _group_factory_3(c: dict[Hashable, Any],
                            key_1: Hashable, key_2: Hashable, key_3: Hashable) -> Inner:
            return c.setdefault(
                    key_1, {}
                ).setdefault(
                    key_2, {}
                ).setdefault(key_3, inner_factory())

        def _group_factory_4(c: dict[Hashable, Any],
                            key_1: Hashable, key_2: Hashable, key_3: Hashable,
                            key_4: Hashable) -> Inner:
            return c.setdefault(
                    key_1, {}
                ).setdefault(
                    key_2, {}
                ).setdefault(
                    key_3, {}
                ).setdefault(key_4, inner_factory())

        def _group_factory_5(c: dict[Hashable, Any],
                            key_1: Hashable, key_2: Hashable, key_3: Hashable,
                            key_4: Hashable, key_5: Hashable) -> Inner:
            return c.setdefault(
                    key_1, {}
                ).setdefault(
                    key_2, {}
                ).setdefault(
                    key_3, {}
                ).setdefault(
                    key_4, {}
                ).setdefault(key_5, inner_factory())

        def _group_factory_n(c: dict[Hashable, Any], *args: Hashable) -> Inner:
            assert len(args) > 5
            for key in args[:-1]:
                c = c.setdefault(key, {})
            return c.setdefault(args[-1], inner_factory())

        tree = query()
        element_inserter = VectorPartial(inserter, n_args=tree.n_elements+1)
        if not tree.n_groups:
            msg = "There must be at least one group in the query using GroupCollector."
            raise ValueError(msg)

        match tree.n_groups:
            case 1 as n:
                group_factory = VectorPartial(_group_factory_1, n_args=n+1)
            case 2 as n:
                group_factory = VectorPartial(_group_factory_2, n_args=n+1)
            case 3 as n:
                group_factory = VectorPartial(_group_factory_3, n_args=n+1)
            case 4 as n:
                group_factory = VectorPartial(_group_factory_4, n_args=n+1)
            case 5 as n:
                group_factory = VectorPartial(_group_factory_5, n_args=n+1)
            case n:
                group_factory = VectorPartial(_group_factory_n, n_args=n+1)

        return _Collector(tree, dict, element_inserter, group_factory)
