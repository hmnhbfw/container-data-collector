# Container Data Collector

[![PyPI - Version](https://img.shields.io/pypi/v/container-data-collector.svg)](https://pypi.org/project/container-data-collector)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/container-data-collector.svg)](https://pypi.org/project/container-data-collector)
[![License](https://img.shields.io/github/license/hmnhbfw/container-data-collector.svg)](https://github.com/hmnhbfw/container-data-collector/blob/main/LICENSE)

**Table of Contents**

- [Container Data Collector](#container-data-collector)
  - [Installation](#installation)
  - [Goals and usages](#goals-and-usages)
  - [Query Tree Syntax](#query-tree-syntax)
  - [Further plans](#further-plans)
  - [License](#license)

## Installation

```console
pip install container-data-collector
```

## Goals and usages

The main purpose of this library is collecting data from Python `list` and `dict`-like containers of arbitrary complexity.

Data is collected by setting which elements have to be collected, how to group and compound these elements, and in which container they have to be inserted to and how. E.g. we have a bunch of Python JSON-like objects like this:

```python
{
    "success": True,
    "pagination": {
        "limit": 20,
        "totalCount": 1583,
        "currentPage": 1,
        "totalPageCount": 80,
    },
    "orders": [
        {
            "shipmentStore": "Some store code",
            "status": "Some delivery code",
            "items": [
                {
                    "id": 1,
                    "quantity": 5,
                    "offer": {
                        "name": "Product name",
                        # other offer's information
                    }
                },
                # and other items inside the current order
            ],
        },
        # and so forth
    ],
}
```

Suppose, we need to get a product name, its quantity and group this information by shipment store and delivery status. Also we don't want to collect the same products. It'd be nice if we could get the number of each product in the second, rightmost group. So, what we need is to write:

```python
from collections import Counter
from dataclasses import dataclass, field

# Import nessecary objects
from container_data_collector import Branch, GroupCollector, Query

# (optional) Create a dataclass because we hardly want to compute
# the hash of a string every single time
@dataclass(slots=True, frozen=True)
class Product:
    internal_id: int = field(hash=True)
    name: str = field(compare=False)

# (optional) Create an insert function rather a lambda
def insert_product(c: Counter[Product],
                     internal_id: int, name: str, count: int) -> None:
    product = Product(internal_id, name)
    c[product] += count

# Compile our query (it'll be explained below)
collector = GroupCollector.compile(
    Query.at("orders").for_each().branches(
        Branch.at("shipmentStore").group(1),
        Branch.at("status").group(2),
        Branch.at("items").for_each().branches(
            Branch.at("quantity").element(3),
            Branch.at("id").element(1),
            Branch.at("offer").at("name").element(2),
        ),
    ),
    inner_factory=Counter[Product],
    inserter=insert_product,
)

data = ... # Iterable data with JSON-like objects

# Getting the result
result = collector.collect(data)
```

So far, there're two Collector's factories: `PlainCollector` and `GroupCollector`. The first one is used for a query without any groups, and the second one, on the contrary, is used for a query with at least one group.

Both factories have the class method `compile`, which receives three arguments: `query`, `inner_factory`, `inserter`. The first argument is a structure of the query. It answers the question: when we get each object from the iterable data, that we have, how we should process it? In the example above, we've got a `dict`, and now we'd like to look at the value of the `"orders"` key, so we're saying:

```python
# Takes any Hashable object
# in respect to key types of the dictionary
Query.at("orders")
```
We know this value is a `list`, and we're on the way to process each element of this array. So we're saying then:

```python
# NOTE: be careful, when placing `for_each` first in the chain.
# The `collect` method iterates over the given data,
# and each element of the data will be treated as a Iterable object.

# There's one rational case for that:
# if the data has, for example, a `list[list[Any]]` type.
Query.at("orders").for_each()
```

We get another `dict`, but now we want to take a look at a few key's contents in the same object, and it means we need multiple branches:

```python
Query.at("orders").for_each().branches(
    # At least two branches have to be here.

    # All branches queries start with the `Branch.at` method.

    # The order of the branches is not important.

    # There cannot be two `for_each` in different branches,
    # that is, if the query has any `for_each`,
    # there must be only one path with all `for_each`s.
)
```

The keys holding values of interest are `"shipmentStore"` and `"status"`. We want to group elements by these values, at first by `shipmentStore`'s values, at second by `status`'s values. The `group` method will handle it, marking the order of the group with a number:

```python
# If the value you'd like to group by is not `Hashable`,
# the `group` method has the `factory` keyword argument,
# expecting a `Callable` object that transforms the value to `Hashable`.
Branch.at("shipmentStore").group(1)
```

Let's take a step back and talk about two last arguments of the `compile` method. 

`inner_factory` specified _which container_ the elements should be inserted _into_. In our case, it's just `Counter[Product]`. In general, it's any argument-less producer-like `Callable` object.

`inserter` specified _how_ and _in what order_ the elements should be inserted into the container. It's also a `Callable` object, whose the first argument is always the container, the elements should be inserted into, and the other positional arguments expect values from `element` methods from the query in that order they've been marked:

```python
# NOTE: missing some position either in `element` or `group`
# causes a raise of `RuntimeError`
Branch.at("quantity").element(3)
Branch.at("id").element(1)
Branch.at("offer").at("name").element(2)
```

Also, if we'd like to filter some values from `element`, `group`, and `at`, we can use filter methods such as `include` and `exclude`:

```python
Branch.at("id").element(1).exclude(any_of=container_with_banned_ids)
Branch.at("quantity").element(3).include(validator=greater_than_five)
```

As the result, we'll get from the `collect` method something like this:

```python
assert collector.collect(data) == {
    "Some store code": {
        "Some delivery code": Counter(
            {
                Product(1, "Product name"): 5,
                # other products in this group
            }
        ),
        # other delivery statuses
    },
    # other store codes
}
```

The for-loop way collection some data is still nice for simple things, but the more complex it becomes, the easier it is to make a mistake. Summing up, this library is about reducing complexity, wrapping the basic logic of collection into a query tree structure.

## Query Tree Syntax

A simplified [BNF](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form) of the query tree is presented below. (It only doesn't specify what parameters the functions have and the correct number of these functions.)

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

## Further plans

- [ ] Some exceptions give an user the full information about what happened, e.g. during the Query Tree building, but not during collection. So, if there's no such key, mentioning in some `at` method, in the iterable data, all we'll get is a plain `KeyError`, but it'd be great if we'd get the full path to the root from the place where the error is occured.
- [ ] Obviously, the `collect` method is slower than a plain for-loops. Both ways to collect objects have order of the same time complexity. Now the constant is about `5`, and I'd like to make it lower. Possible directions to look at: the Query Tree building is fast and happens only once, so need to figure out the recursion impact, etc.

## License

`container-data-collector` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.