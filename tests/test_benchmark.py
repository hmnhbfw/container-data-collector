import random
import string
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from container_data_collector import Query, GroupCollector, Branch


def random_str(length: int = 5) -> str:
    return "".join(random.choices(string.ascii_letters, k=length))


@dataclass(slots=True, frozen=True)
class Product:
    internal_id: int = field(hash=True)
    name: str = field(compare=False)


@pytest.fixture(scope="session")
def services():
    return {
        random_str(20)
        for _ in range(5)
    }


@pytest.fixture(scope="session")
def shipment_stores():
    n_shipment_stores = 10
    return {
        n: random_str(5)
        for n in range(n_shipment_stores)
    }


@pytest.fixture(scope="session")
def statuses():
    n_statuses = 10
    return {
        n: random_str(5)
        for n in range(n_statuses)
    }


@pytest.fixture(scope="session")
def input_data(shipment_stores, statuses, services):
    n_pages = 100
    n_orders = 20

    products = tuple(
        random_str(50)
        for _ in range(100)
    )
    offers = products + tuple(services)
    ids = dict(enumerate(offers, start=1))

    def generate_offer():
        quantity = random.randint(1, 10)
        internal_id = random.randint(1, len(offers))
        return {
            "id": internal_id,
            "quantity": quantity,
            "offer": {
                "name": ids[internal_id],
            }
        }

    return [
        {
            "success": True,
            "pagination": {
                "limit": n_orders,
                "totalCount": n_orders * n_pages,
                "currentPage": page,
                "totalPageCount": n_pages,
            },
            "orders": [
                {
                    "shipmentStore": random.choice(range(len(shipment_stores))),
                    "status": random.choice(range(len(statuses))),
                    "items": [
                        generate_offer()
                        for _ in range(random.randint(1, 5))
                    ]
                }
                for _ in range(n_orders)
            ]
        }
        for page in range(1, n_pages + 1)
    ]


@pytest.fixture(scope="session")
def fs_for_comparison(input_data, shipment_stores, statuses, services):
    def store_producer(n: int) -> str:
        return shipment_stores[n]

    def status_producer(n: int) -> str:
        return statuses[n]

    def inserter(c: Counter[Product], internal_id: int, name: str, quantity: int) -> None:
        product = Product(internal_id, name)
        c[product] += quantity

    def for_loop():
        answer = dict[str, dict[str, Counter[Product]]]()
        for elem in input_data:
            for order in cast(list[Any], elem["orders"]):
                for item in order["items"]:
                    name = item["offer"]["name"]
                    if name in services:
                        continue
                    inner = answer.setdefault(
                        store_producer(order["shipmentStore"]), {}
                    ).setdefault(
                        status_producer(order["status"]), Counter[Product]()
                    )
                    product = Product(item["id"], name)
                    inner[product] += item["quantity"]
        return answer

    compiled_collector = GroupCollector.compile(
        Query.at("orders").for_each().branches(
            Branch.at("shipmentStore").group(1, factory=store_producer),
            Branch.at("status").group(2, factory=status_producer),
            Branch.at("items").for_each().branches(
                Branch.at("id").element(1),
                Branch.at("quantity").element(3),
                Branch.at("offer").at("name").element(2).exclude(any_of=services)
            )
        ),
        inner_factory=Counter[Product],
        inserter=inserter,
    )

    def collector():
        return compiled_collector.collect(input_data)

    return {
        for_loop.__name__: for_loop,
        collector.__name__: collector,
    }


def test_correctness(fs_for_comparison):
    assert fs_for_comparison["for_loop"]() == fs_for_comparison["collector"]()


def test_for_loop(fs_for_comparison, benchmark):
    benchmark(fs_for_comparison["for_loop"])


def test_collector(fs_for_comparison, benchmark):
    benchmark(fs_for_comparison["collector"])
