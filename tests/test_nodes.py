import random
from typing import Any

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


def test_element():
    answer = [random.randint(-100, 100) for _ in range(10)]

    result: list[int] = []
    context = Context(
        top_container=result,
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
    )
    element = Element[list[int], list[int]](1)

    for obj in answer:
        assert element.process(obj, context) is State.SUCCESS
    assert result == answer


def test_element_with_exclude():
    banned = {random.randint(-100, 100) for _ in range(5)}
    data: list[int] = list(banned) + [random.randint(-100, 100) for _ in range(5)]
    random.shuffle(data)
    answer = list(filter(
        lambda x: x not in banned,
        data
    ))

    result: list[int] = []
    context = Context(
        top_container=result,
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
    )
    exclude = Exclude[list[int], list[int]](exclude=banned)
    element = Element[list[int], list[int]](1, next_node=exclude)

    for obj in data:
        state = element.process(obj, context)
        if obj not in banned:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
    assert result == answer


def test_element_with_invalidator():
    answer = list(filter(
        lambda x: x % 2 == 0,
        [random.randint(-100, 100) for _ in range(10)]
    ))

    result: list[int] = []
    context = Context(
        top_container=result,
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
    )
    exclude = Exclude[list[int], list[int]](invalidator=lambda x: x % 2 != 0)
    element = Element[list[int], list[int]](1, next_node=exclude)

    for obj in answer:
        state = element.process(obj, context)
        if obj % 2 == 0:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
    assert result == answer


def test_element_with_exclude_and_invalidator():
    banned = {random.randint(-100, 100) for _ in range(5)}
    data: list[int] = list(banned) + [random.randint(-100, 100) for _ in range(5)]
    random.shuffle(data)
    answer = list(filter(
        lambda x: x not in banned and x % 2 == 0,
        data
    ))

    result: list[int] = []
    context = Context(
        top_container=result,
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
    )
    exclude = Exclude[list[int], list[int]](exclude=banned, invalidator=lambda x: x % 2 != 0)
    element = Element[list[int], list[int]](1, next_node=exclude)

    for obj in data:
        state = element.process(obj, context)
        if obj not in banned and obj % 2 == 0:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
    assert result == answer


def test_element_with_include():
    allowed = {random.randint(-100, 100) for _ in range(5)}
    data: list[int] = list(allowed) + [random.randint(-100, 100) for _ in range(5)]
    random.shuffle(data)
    answer = list(filter(
        lambda x: x in allowed,
        data
    ))

    result: list[int] = []
    context = Context(
        top_container=result,
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
    )
    include = Include[list[int], list[int]](include=allowed)
    element = Element[list[int], list[int]](1, next_node=include)

    for obj in data:
        state = element.process(obj, context)
        if obj in allowed:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
    assert result == answer


def test_element_with_validator():
    answer = list(filter(
        lambda x: x % 2 == 0,
        [random.randint(-100, 100) for _ in range(10)]
    ))

    result: list[int] = []
    context = Context(
        top_container=result,
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
    )
    include = Include[list[int], list[int]](validator=lambda x: x % 2 == 0)
    element = Element[list[int], list[int]](1, next_node=include)

    for obj in answer:
        state = element.process(obj, context)
        if obj % 2 == 0:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
    assert result == answer


def test_element_with_include_and_validator():
    allowed = {random.randint(-100, 100) for _ in range(5)}
    data: list[int] = list(allowed) + [random.randint(-100, 100) for _ in range(5)]
    random.shuffle(data)
    answer = list(filter(
        lambda x: x in allowed and x % 2 == 0,
        data
    ))

    result: list[int] = []
    context = Context(
        top_container=result,
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
    )
    include = Include[list[int], list[int]](include=allowed, validator=lambda x: x % 2 == 0)
    element = Element[list[int], list[int]](1, next_node=include)

    for obj in data:
        state = element.process(obj, context)
        if obj in allowed and obj % 2 == 0:
            assert state is State.SUCCESS
        else:
            assert state is State.REJECT
    assert result == answer


def test_group():
    data = [random.randint(-100, 100) for _ in range(10)]
    answer: dict[str, dict[int, list[tuple[str, int]]]] = {}
    for x in data:
        answer.setdefault(str(x), {}).setdefault(x, []).append((str(x), x))

    result: dict[str, dict[int, list[tuple[str, int]]]] = {}
    context = Context(
        top_container=result,
        inserter=lambda c, e1, e2: c.append((e1, e2)), n_elements=2,
        group_factory=lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []), n_groups=2,
    )
    e1 = Element[dict[str, Any], list[tuple[str, int]]](1)
    e2 = Element[dict[str, Any], list[tuple[str, int]]](2)
    g1 = Group[dict[str, Any], list[tuple[str, int]]](1, factory=str)
    g2 = Group[dict[str, Any], list[tuple[str, int]]](2)

    for x in data:
        g1.process(x, context)
        e2.process(x, context)
        g2.process(x, context)
        e1.process(str(x), context)
    assert result == answer


def test_at():
    data = random.randint(-100, 100)
    answer: dict[str, dict[int, list[tuple[int, int]]]] = {
        str(data): {
            data: [
                (data, data)
            ]
        }
    }

    result: dict[str, dict[int, list[tuple[int, int]]]] = {}
    context = Context(
        top_container=result,
        inserter=lambda c, e1, e2: c.append((e1, e2)), n_elements=2,
        group_factory=lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []), n_groups=2,
    )
    e1 = Element[dict[str, Any], list[tuple[int, int]]](1)
    e2 = Element[dict[str, Any], list[tuple[int, int]]](2)
    g1 = Group[dict[str, Any], list[tuple[int, int]]](1, factory=str)
    g2 = Group[dict[str, Any], list[tuple[int, int]]](2)
    at = At[dict[str, Any], list[tuple[int, int]]](key="data", next_nodes=(g2, e1, g1, e2))

    at.process({"data": data}, context)
    assert result == answer


def test_from_list_without_key():
    data = [random.randint(-100, 100) for _ in range(10)]
    answer: dict[str, dict[int, list[tuple[int, int]]]] = {}
    for x in data:
        answer.setdefault(str(x), {}).setdefault(x, []).append((x, x))

    result: dict[str, dict[int, list[tuple[int, int]]]] = {}
    context = Context(
        top_container=result,
        inserter=lambda c, e1, e2: c.append((e1, e2)), n_elements=2,
        group_factory=lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []), n_groups=2,
    )
    e1 = Element[dict[str, Any], list[tuple[int, int]]](1)
    e2 = Element[dict[str, Any], list[tuple[int, int]]](2)
    g1 = Group[dict[str, Any], list[tuple[int, int]]](1, factory=str)
    g2 = Group[dict[str, Any], list[tuple[int, int]]](2)
    from_list = FromList[dict[str, Any], list[tuple[int, int]]](next_nodes=(g1, g2, e1, e2))

    from_list.process(data, context)
    assert result == answer


def test_from_list_with_key():
    data = [random.randint(-100, 100) for _ in range(10)]
    answer: dict[str, dict[int, list[tuple[int, int]]]] = {}
    for x in data:
        answer.setdefault(str(x), {}).setdefault(x, []).append((x, x))

    result: dict[str, dict[int, list[tuple[int, int]]]] = {}
    context = Context(
        top_container=result,
        inserter=lambda c, e1, e2: c.append((e1, e2)), n_elements=2,
        group_factory=lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []), n_groups=2,
    )
    e1 = Element[dict[str, Any], list[tuple[int, int]]](1)
    e2 = Element[dict[str, Any], list[tuple[int, int]]](2)
    g1 = Group[dict[str, Any], list[tuple[int, int]]](1, factory=str)
    g2 = Group[dict[str, Any], list[tuple[int, int]]](2)
    from_list = FromList[dict[str, Any], list[tuple[int, int]]](
        key="data", next_nodes=(g2, g1, e2, e1)
    )

    from_list.process({"data": data}, context)
    assert result == answer
