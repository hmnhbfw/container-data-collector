import random
from typing import Any

import pytest

from container_data_collector.nodes import (
    At,
    Context,
    Element,
    Exclude,
    FromList,
    Group,
    Include,
    State,
)
from container_data_collector.vector_partial import VectorPartial


def list_append(c: list[int], e: int) -> None:
    c.append(e)


def test_wrong_element():
    with pytest.raises(ValueError):
        Element[Any, Any](0)

    with pytest.raises(ValueError):
        Element[Any, Any](-1)

    with pytest.raises(ValueError):
        Element[Any, Any](random.randint(-100, -2))


def test_element():
    Outer = list[int]
    Inner = list[int]

    answer = [random.randint(-100, 100) for _ in range(10)]

    result: Outer = []
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    element = Element[Outer, Inner](1)

    for obj in answer:
        assert element.process(obj, context) is State.SUCCESS
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
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    exclude = Exclude[Outer, Inner](any_of=banned)
    element = Element[Outer, Inner](1)
    element.attach(exclude)

    for obj in data:
        state = element.process(obj, context)
        if obj not in banned:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
    assert result == answer


def test_element_with_invalidator():
    Outer = list[int]
    Inner = list[int]

    answer = list(filter(
        lambda x: x % 2 == 0,
        [random.randint(-100, 100) for _ in range(10)]
    ))

    result: Outer = []
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    exclude = Exclude[Outer, Inner](invalidator=lambda x: x % 2 != 0)
    element = Element[Outer, Inner](1)
    element.attach(exclude)

    for obj in answer:
        state = element.process(obj, context)
        if obj % 2 == 0:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
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
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    exclude = Exclude[Outer, Inner](any_of=banned, invalidator=lambda x: x % 2 != 0)
    element = Element[Outer, Inner](1)
    element.attach(exclude)

    for obj in data:
        state = element.process(obj, context)
        if obj not in banned and obj % 2 == 0:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
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
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    include = Include[Outer, Inner](any_of=allowed)
    element = Element[Outer, Inner](1)
    element.attach(include)

    for obj in data:
        state = element.process(obj, context)
        if obj in allowed:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
    assert result == answer


def test_element_with_validator():
    Outer = list[int]
    Inner = list[int]

    answer = list(filter(
        lambda x: x % 2 == 0,
        [random.randint(-100, 100) for _ in range(10)]
    ))

    result: Outer = []
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    include = Include[Outer, Inner](validator=lambda x: x % 2 == 0)
    element = Element[Outer, Inner](1)
    element.attach(include)

    for obj in answer:
        state = element.process(obj, context)
        if obj % 2 == 0:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
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
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )
    include = Include[Outer, Inner](any_of=allowed, validator=lambda x: x % 2 == 0)
    element = Element[Outer, Inner](1)
    element.attach(include)

    for obj in data:
        state = element.process(obj, context)
        if obj in allowed and obj % 2 == 0:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
    assert result == answer


def list_append_2(c: list[tuple[int, int]], e1: int, e2: int) -> None:
    c.append((e1, e2))


def test_wrong_group():
    with pytest.raises(ValueError):
        Group[Any, Any](0)

    with pytest.raises(ValueError):
        Group[Any, Any](-1)

    with pytest.raises(ValueError):
        Group[Any, Any](random.randint(-100, -2))


def test_group():
    Outer = dict[str, dict[int, list[tuple[int, int]]]]
    Inner = list[tuple[int, int]]

    data = [random.randint(-100, 100) for _ in range(10)]
    answer: Outer = {}
    for x in data:
        answer.setdefault(str(x), {}).setdefault(x, []).append((x, x))

    result: Outer = {}
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append_2, n_args=3),
        group_factory=VectorPartial(
            lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []),
            n_args=3),
    )
    e1 = Element[Outer, Inner](1)
    e2 = Element[Outer, Inner](2)
    g1 = Group[Outer, Inner](1, factory=str)
    g2 = Group[Outer, Inner](2)

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
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append_2, n_args=3),
        group_factory=VectorPartial(
            lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []),
            n_args=3),
    )
    e1 = Element[Outer, Inner](1)
    e2 = Element[Outer, Inner](2)
    g1 = Group[Outer, Inner](1, factory=str)
    g2 = Group[Outer, Inner](2)
    at = At[Outer, Inner](key="data")
    at.attach(g2, e1, g1, e2)

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
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append_2, n_args=3),
        group_factory=VectorPartial(
            lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []),
            n_args=3),
    )
    e1 = Element[Outer, Inner](1)
    e2 = Element[Outer, Inner](2)
    g1 = Group[Outer, Inner](1, factory=str)
    g2 = Group[Outer, Inner](2)
    from_list = FromList[Outer, Inner]()
    from_list.attach(g1, g2, e1, e2)

    from_list.process(data, context)
    assert result == answer


def test_from_list_with_key():
    Outer = dict[str, dict[int, list[tuple[int, int]]]]
    Inner = list[tuple[int, int]]

    data = [random.randint(-100, 100) for _ in range(10)]
    answer: Outer = {}
    for x in data:
        answer.setdefault(str(x), {}).setdefault(x, []).append((x, x))

    result: Outer = {}
    context = Context(
        top_container=result,
        inserter=VectorPartial(list_append_2, n_args=3),
        group_factory=VectorPartial(
            lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []),
            n_args=3),
    )
    e1 = Element[Outer, Inner](1)
    e2 = Element[Outer, Inner](2)
    g1 = Group[Outer, Inner](1, factory=str)
    g2 = Group[Outer, Inner](2)
    from_list = FromList[Outer, Inner](key="data")
    from_list.attach(g2, g1, e2, e1)

    from_list.process({"data": data}, context)
    assert result == answer
