from collections import Counter
from typing import Any

import pytest

from container_data_collector.context import Context


def inserter(c: Counter[str], e: str, n: int) -> None:
    c[e] += n


def group_factory(c: dict[str, Any], key_1: str, key_2: int) -> Counter[str]:
    return c.setdefault(key_1, {}).setdefault(key_2, Counter[str]())


def test_context():
    answer = {
        "a": {
            1: Counter({"x": 2, "y": 1}),
            2: Counter({"x": 1, "z": 3}),
        },
        "b": {
            3: Counter({"z": 4}),
        },
    }

    result: dict[str, Any] = {}

    context = Context(
        top_container=result,
        inserter=inserter, n_elements=2,
        group_factory=group_factory, n_groups=2,
    )

    with pytest.raises(ValueError):
        context.apply_element(None, pos=0)

    with pytest.raises(ValueError):
        context.create_group(None, pos=0)

    context.create_group("b", pos=1)

    context.create_group(3, pos=2)

    context.apply_element("z", pos=1)
    context.apply_element(2, pos=2)
    context.apply_element("z", pos=1)
    context.apply_element(2, pos=2)

    context.create_group("a", pos=1)

    context.create_group(1, pos=2)

    context.apply_element("x", pos=1)
    context.apply_element(1, pos=2)
    context.apply_element("y", pos=1)
    context.apply_element(1, pos=2)
    context.apply_element("x", pos=1)
    context.apply_element(1, pos=2)

    context.create_group(2, pos=2)

    context.apply_element("z", pos=1)
    context.apply_element(1, pos=2)
    context.apply_element("x", pos=1)
    context.apply_element(1, pos=2)
    context.apply_element("z", pos=1)
    context.apply_element(2, pos=2)

    assert result == answer
