import pytest

from container_data_collector.query import Query, Branch


def test_without_elements():
    Query.element()()
    Query.at("key").element()()
    Query.for_each().element()()
    Query.branches(
        Branch.at("key").element(1),
        Branch.at("key").element(2),
        Branch.at("key").group(),
    )()

    with pytest.raises(RuntimeError):
        Query.at("key").include()()

    with pytest.raises(RuntimeError):
        Query.at("key").exclude()()

    with pytest.raises(RuntimeError):
        Query.at("key").group()()

    with pytest.raises(RuntimeError):
        Query.branches(
            Branch.at("key").group(),
            Branch.at("key").include(),
            Branch.at("key").exclude(),
        )()


def test_missing_positions():
    msg = r"In the groups: there is 1 missing position.\nMissing: 1."
    with pytest.raises(RuntimeError, match=msg):
        Query.branches(
            Branch.at("key").element(),
            Branch.at("key").group(2),
            Branch.at("key").group(3),
        )()

    msg = r"In the elements: there are 2 missing positions.\nMissing: 1, 4."
    with pytest.raises(RuntimeError, match=msg):
        Query.branches(
            Branch.at("key").element(2),
            Branch.at("key").element(3),
            Branch.at("key").element(5),
        )()

    msg = r"In the elements: there are 97 missing positions.\nMissing: 3, 5..99, 101."
    with pytest.raises(RuntimeError, match=msg):
        Query.at("key").branches(
            Branch.at("key").element(1),
            Branch.at("key").element(2),
            Branch.at("key").element(4),
            Branch.at("key").element(100),
            Branch.at("key").element(102),
        )()


def test_from_list_branches():
    Query.for_each().branches(
        Branch.at("key").for_each().branches(
            Branch.at("key").for_each().branches(
                Branch.at("key").element(2),
                Branch.at("key").element(3),
            ),
            Branch.at("key").group(1),
        ),
        Branch.at("key").element(1),
        Branch.at("key").group(2),
    )()

    with pytest.raises(RuntimeError):
        Query.for_each().branches(
            Branch.at("key").for_each().element(1),
            Branch.at("key").for_each().element(2),
        )()
