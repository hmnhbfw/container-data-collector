import random
import string
from collections import Counter
from typing import Any, TypeVar, cast

from container_data_collector.collector import GroupCollector, PlainCollector
from container_data_collector.query import Branch, Query


def random_str(length: int = 5) -> str:
    return "".join(random.choices(string.ascii_letters, k=length))


T = TypeVar("T")

def list_inserter(c: list[T], e: T) -> None:
    c.append(e)


def test_plain_list():
    answer = [random.randint(-100, 100) for _ in range(10)]

    collector = PlainCollector.compile(
        Query.element(),
        inner_factory=list[int],
        inserter=list_inserter,
    )

    assert collector.collect(answer) == answer


def test_counter_for_each():
    data = [str(random.randint(-100, 100)) for _ in range(10)]
    answer = Counter[str]()
    for x in data:
        answer[x] += 1

    def inserter(c: Counter[str], key: str) -> None:
        c[key] += 1

    collector = PlainCollector.compile(
        Query.element(),
        inner_factory=Counter[str],
        inserter=inserter,
    )

    assert collector.collect(data) == answer


def test_for_each():
    data = [[random.randint(-100, 100) for _ in range(5)] for _ in range(5)]
    answer = list[int]()
    for xs in data:
        for x in xs:
            answer.append(x)

    collector = PlainCollector.compile(
        Query.for_each().element(),
        inner_factory=list[int],
        inserter=list_inserter,
    )

    assert collector.collect(data) == answer


def test_one_group():
    key_group = random_str()
    value_group_1 = random_str()
    value_group_2 = random_str()
    key_elem = random_str()
    data = [
        {
            key_group: random.choice((value_group_1, value_group_2)),
            key_elem: random.randint(-100, 100)
        }
        for _ in range(10)
    ]
    answer = dict[str, list[int]]()
    for elem in data:
        inner = answer.setdefault(cast(str, elem[key_group]), list[int]())
        list_inserter(inner, cast(int, elem[key_elem]))

    collector = GroupCollector.compile(
        Query.branches(
            Branch.at(key_elem).element(),
            Branch.at(key_group).group(),
        ),
        inner_factory=list[int],
        inserter=list_inserter,
    )

    assert collector.collect(data) == answer


def test_many_groups():
    shipment_store_count = 10
    shipment_stores = {
        n: random_str(5)
        for n in range(shipment_store_count)
    }
    def store_producer(n: int) -> str:
        return shipment_stores[n]

    status_count = 10
    statuses = {
        n: random_str(5)
        for n in range(status_count)
    }
    def status_producer(n: int) -> str:
        return statuses[n]

    services = {
        random_str(5)
        for _ in range(5)
    }
    products = tuple(
        random_str(10)
        for _ in range(20)
    )
    offers = products + tuple(services)

    data = [
        {
            "success": random.choice((True, True, False)),
            "orders": [
                {
                    "shipmentStore": random.choice(range(shipment_store_count)),
                    "status": random.choice(range(status_count)),
                    "items": [
                        {
                            "quantity": random.randint(1, 10),
                            "offer": {
                                "name": random.choice(offers)
                            }
                        }
                        for _ in range(5)
                    ]
                }
                for _ in range(5)
            ]
        }
        for _ in range(5)
    ]
    answer = dict[str, dict[str, Counter[str]]]()
    for elem in data:
        if not elem["success"]:
            continue
        for order in cast(list[Any], elem["orders"]):
            for item in order["items"]:
                name = item["offer"]["name"]
                if name in services:
                    continue
                inner = answer.setdefault(
                    store_producer(order["shipmentStore"]), {}
                ).setdefault(status_producer(order["status"]), Counter[str]())
                inner[name] += item["quantity"]

    def inserter(c: Counter[str], name: str, quantity: int) -> None:
        c[name] += quantity

    collector = GroupCollector.compile(
        Query.branches(
            Branch.at("success").exclude(any_of=(False,)),
            Branch.at("orders").for_each().branches(
                Branch.at("shipmentStore").group(1, factory=store_producer),
                Branch.at("status").group(2, factory=status_producer),
                Branch.at("items").for_each().branches(
                    Branch.at("quantity").element(2),
                    Branch.at("offer").at("name").element(1).exclude(any_of=services)
                )
            )
        ),
        inner_factory=Counter[str],
        inserter=inserter,
    )

    assert collector.collect(data) == answer
