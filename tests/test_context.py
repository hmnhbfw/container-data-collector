from collections import Counter
import random
from typing import Any

import pytest

from container_data_collector.context import Context
from container_data_collector.vector_partial import VectorPartial


def test_context_without_groups():
    def inserter(c: list[int], e: int) -> None:
        c.append(e)

    answer = [random.randint(-100, 100) for _ in range(10)]

    result: list[int] = []
    context = Context(
        top_container=result,
        inserter=VectorPartial(inserter, n_args=2),
        group_factory=VectorPartial(lambda c: c, n_args=1),
    )

    for e in answer:
        context.apply_element(e, pos=1)
    assert result == answer


def test_context_with_groups():
    def inserter(c: Counter[str], e: str, n: int) -> None:
        c[e] += n

    def group_factory(c: dict[str, Any], key_1: str, key_2: int) -> Counter[str]:
        return c.setdefault(key_1, {}).setdefault(key_2, Counter[str]())

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
        inserter=VectorPartial(inserter, n_args=3),
        group_factory=VectorPartial(group_factory, n_args=3),
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
