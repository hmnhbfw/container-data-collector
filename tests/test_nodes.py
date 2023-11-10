import random

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
    Outer = list[int]
    Inner = list[int]

    answer = [random.randint(-100, 100) for _ in range(10)]

    result: Outer = []
    context = Context(
        top_container=result,
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
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
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
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
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
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
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
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
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
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
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
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
        inserter=lambda c, e: c.append(e), n_elements=1,
        group_factory=lambda c: c, n_groups=0,
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


def test_group():
    Outer = dict[str, dict[int, list[tuple[str, int]]]]
    Inner = list[tuple[str, int]]

    data = [random.randint(-100, 100) for _ in range(10)]
    answer: Outer = {}
    for x in data:
        answer.setdefault(str(x), {}).setdefault(x, []).append((str(x), x))

    result: Outer = {}
    context = Context(
        top_container=result,
        inserter=lambda c, e1, e2: c.append((e1, e2)), n_elements=2,
        group_factory=lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []), n_groups=2,
    )
    e1 = Element[Outer, Inner](1)
    e2 = Element[Outer, Inner](2)
    g1 = Group[Outer, Inner](1, factory=str)
    g2 = Group[Outer, Inner](2)

    for x in data:
        g1.process(x, context)
        e2.process(x, context)
        g2.process(x, context)
        e1.process(str(x), context)
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
        inserter=lambda c, e1, e2: c.append((e1, e2)), n_elements=2,
        group_factory=lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []), n_groups=2,
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
        inserter=lambda c, e1, e2: c.append((e1, e2)), n_elements=2,
        group_factory=lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []), n_groups=2,
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
        inserter=lambda c, e1, e2: c.append((e1, e2)), n_elements=2,
        group_factory=lambda c, k1, k2: c.setdefault(k1, {}).setdefault(k2, []), n_groups=2,
    )
    e1 = Element[Outer, Inner](1)
    e2 = Element[Outer, Inner](2)
    g1 = Group[Outer, Inner](1, factory=str)
    g2 = Group[Outer, Inner](2)
    from_list = FromList[Outer, Inner](key="data")
    from_list.attach(g2, g1, e2, e1)

    from_list.process({"data": data}, context)
    assert result == answer
