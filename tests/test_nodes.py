import random

import pytest

from container_data_collector.nodes import (
    At,
    Context,
    Element,
    Exclude,
    ForEach,
    Group,
    Include,
    State,
)
from container_data_collector.vector_partial import VectorPartial


def list_append(c: list[int], e: int) -> None:
    c.append(e)


def test_wrong_element():
    with pytest.raises(ValueError):
        Element(0)

    with pytest.raises(ValueError):
        Element(-1)

    with pytest.raises(ValueError):
        Element(random.randint(-100, -2))


def test_element():
    Outer = list[int]
    Inner = list[int]

    answer = [random.randint(-100, 100) for _ in range(10)]

    result: Outer = []
    context = Context[Outer, Inner](
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    element = Element(1)

    for obj in answer:
        assert element.process(obj, context).state is State.SUCCESS
    assert result == answer


def test_element_with_exclude():
    Outer = list[int]
    Inner = list[int]

    banned = {random.randint(-100, 100) for _ in range(5)}
    data: Outer = list(banned) + [random.randint(-100, 100) for _ in range(5)]
    random.shuffle(data)
    answer = list(filter(
        lambda x: x not in banned,
        data
    ))

    result: Outer = []
    context = Context[Outer, Inner](
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    exclude = Exclude(any_of=banned)
    element = Element(1)
    element.connect_with(exclude)

    for obj in data:
        res = element.process(obj, context)
        if obj not in banned:
            assert res.state is State.SUCCESS
        else:
            assert res.state is State.REJECT
    assert result == answer


def test_element_with_invalidator():
    Outer = list[int]
    Inner = list[int]

    answer = list(filter(
        lambda x: x % 2 == 0,
        [random.randint(-100, 100) for _ in range(10)]
    ))

    result: Outer = []
    context = Context[Outer, Inner](
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    exclude = Exclude(invalidator=lambda x: x % 2 != 0)
    element = Element(1)
    element.connect_with(exclude)

    for obj in answer:
        res = element.process(obj, context)
        if obj % 2 == 0:
            assert res.state is State.SUCCESS
        else:
            assert res.state is State.REJECT
    assert result == answer


def test_element_with_exclude_and_invalidator():
    Outer = list[int]
    Inner = list[int]

    banned = {random.randint(-100, 100) for _ in range(5)}
    data: Outer= list(banned) + [random.randint(-100, 100) for _ in range(5)]
    random.shuffle(data)
    answer = list(filter(
        lambda x: x not in banned and x % 2 == 0,
        data
    ))

    result: Outer = []
    context = Context[Outer, Inner](
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    exclude = Exclude(any_of=banned, invalidator=lambda x: x % 2 != 0)
    element = Element(1)
    element.connect_with(exclude)

    for obj in data:
        res = element.process(obj, context)
        if obj not in banned and obj % 2 == 0:
            assert res.state is State.SUCCESS
        else:
            assert res.state is State.REJECT
    assert result == answer


def test_element_with_include():
    Outer = list[int]
    Inner = list[int]

    allowed = {random.randint(-100, 100) for _ in range(5)}
    data: Outer = list(allowed) + [random.randint(-100, 100) for _ in range(5)]
    random.shuffle(data)
    answer = list(filter(
        lambda x: x in allowed,
        data
    ))

    result: Outer = []
    context = Context[Outer, Inner](
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    include = Include(any_of=allowed)
    element = Element(1)
    element.connect_with(include)

    for obj in data:
        res = element.process(obj, context)
        if obj in allowed:
            assert res.state is State.SUCCESS
        else:
            assert res.state is State.REJECT
    assert result == answer


def test_element_with_validator():
    Outer = list[int]
    Inner = list[int]

    answer = list(filter(
        lambda x: x % 2 == 0,
        [random.randint(-100, 100) for _ in range(10)]
    ))

    result: Outer = []
    context = Context[Outer, Inner](
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    include = Include(validator=lambda x: x % 2 == 0)
    element = Element(1)
    element.connect_with(include)

    for obj in answer:
        res = element.process(obj, context)
        if obj % 2 == 0:
            assert res.state is State.SUCCESS
        else:
            assert res.state is State.REJECT
    assert result == answer


def test_element_with_include_and_validator():
    Outer = list[int]
    Inner = list[int]

    allowed = {random.randint(-100, 100) for _ in range(5)}
    data: Outer = list(allowed) + [random.randint(-100, 100) for _ in range(5)]
    random.shuffle(data)
    answer = list(filter(
        lambda x: x in allowed and x % 2 == 0,
        data
    ))

    result: Outer = []
    context = Context[Outer, Inner](
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    include = Include(any_of=allowed, validator=lambda x: x % 2 == 0)
    element = Element(1)
    element.connect_with(include)

    for obj in data:
        res = element.process(obj, context)
        if obj in allowed and obj % 2 == 0:
            assert res.state is State.SUCCESS
        else:
            assert res.state is State.REJECT
    assert result == answer


def list_append_2(c: list[tuple[int, int]], e1: int, e2: int) -> None:
    c.append((e1, e2))


def test_wrong_group():
    with pytest.raises(ValueError):
        Group(0)

    with pytest.raises(ValueError):
        Group(-1)

    with pytest.raises(ValueError):
        Group(random.randint(-100, -2))


def test_group():
    Outer = dict[str, dict[int, list[tuple[int, int]]]]
    Inner = list[tuple[int, int]]

    data = [random.randint(-100, 100) for _ in range(10)]
    answer: Outer = {}
    for x in data:
        answer.setdefault(str(x), {}).setdefault(x, []).append((x, x))

    result: Outer = {}
    context = Context[Outer, Inner](
        top_container=result,
        inserter=VectorPartial(list_append_2, n_args=3),
        group_factory=VectorPartial(
            lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []),
            n_args=3),
    )
    e1 = Element(1)
    e2 = Element(2)
    g1 = Group(1, factory=str)
    g2 = Group(2)

    for x in data:
        g1.process(x, context)
        e2.process(x, context)
        g2.process(x, context)
        e1.process(x, context)
    assert result == answer


def test_at():
    Outer = dict[str, dict[int, list[tuple[int, int]]]]
    Inner = list[tuple[int, int]]

    data = random.randint(-100, 100)
    answer: Outer = {
        str(data): {
            data: [
                (data, data)
            ]
        }
    }

    result: Outer = {}
    context = Context[Outer, Inner](
        top_container=result,
        inserter=VectorPartial(list_append_2, n_args=3),
        group_factory=VectorPartial(
            lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []),
            n_args=3),
    )
    e1 = Element(1)
    e2 = Element(2)
    g1 = Group(1, factory=str)
    g2 = Group(2)
    at = At(key="data")
    at.connect_with(g2, e1, g1, e2)

    at.process({"data": data}, context)
    assert result == answer


def test_from_list_without_key():
    Outer = dict[str, dict[int, list[tuple[int, int]]]]
    Inner = list[tuple[int, int]]

    data = [random.randint(-100, 100) for _ in range(10)]
    answer: Outer = {}
    for x in data:
        answer.setdefault(str(x), {}).setdefault(x, []).append((x, x))

    result: Outer = {}
    context = Context[Outer, Inner](
        top_container=result,
        inserter=VectorPartial(list_append_2, n_args=3),
        group_factory=VectorPartial(
            lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []),
            n_args=3),
    )
    e1 = Element(1)
    e2 = Element(2)
    g1 = Group(1, factory=str)
    g2 = Group(2)
    for_each = ForEach()
    for_each.connect_with(g1, g2, e1, e2)

    for_each.process(data, context)
    assert result == answer
