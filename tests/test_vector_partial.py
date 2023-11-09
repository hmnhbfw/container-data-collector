import random
from itertools import combinations, permutations
from collections.abc import Callable

import pytest

from container_data_collector.vector_partial import VectorPartial


def gen_func(*, args: int = 0, defaults: int = 0,
             varargs: bool = False,
             kwonly: int = 0, kwonlydefaults: int = 0,
             varkwargs: bool = False,
             body: str = "pass") -> Callable[..., None]:
    blocks: list[str] = []

    for pos in range(args):
        blocks.append(f"_{pos}")
    for pos in range(defaults):
        blocks.append(f"_d{pos}=None")

    if varargs:
        blocks.append("*args")
    elif kwonly or kwonlydefaults:
        blocks.append("*")

    for pos in range(kwonly):
        blocks.append(f"_k{pos}")
    for pos in range(kwonlydefaults):
        blocks.append(f"_kd{pos}=None")

    if varkwargs:
        blocks.append("**kwargs")

    func = "def func(" + ",".join(blocks) + "): " + body + "\n" + "f = func"

    scope: dict[str, Callable[..., None]] = {}
    exec(func, scope)
    return scope['f']


@pytest.fixture(scope="session")
def gen_func_args():
    return {
        "args": 1,
        "defaults": 1,
        "varargs": True,
        "kwonly": 1,
        "kwonlydefaults": 1,
        "varkwargs": True,
    }


def test_incorrect_n_args():
    f = gen_func(args=1)

    with pytest.raises(IndexError):
        VectorPartial(f, n_args=0)
    with pytest.raises(IndexError):
        VectorPartial(f, n_args=-1)
    for _ in range(10):
        with pytest.raises(IndexError):
            VectorPartial(f, n_args=random.randint(-100, -2))


def test_func_without_non_default_args(gen_func_args): # type: ignore
    values = gen_func_args # type: ignore
    fs: list[Callable[..., None]] = []
    fs.append(gen_func())
    for r in range(1, 6):
        combs = combinations(("defaults", "varargs", "kwonly", "kwonlydefaults", "varkwargs"), r)
        for comb in combs:
            params = {key: values[key] for key in comb} # type: ignore
            f = gen_func(**params) # type: ignore
            fs.append(f)

    for f in fs:
        with pytest.raises(TypeError):
            VectorPartial(f, n_args=1)


def test_func_with_default_pos_args(gen_func_args): # type: ignore
    values = gen_func_args # type: ignore
    fs: list[Callable[..., None]] = []
    for r in range(1, 5):
        combs = combinations(("varargs", "kwonly", "kwonlydefaults", "varkwargs"), r)
        for comb in combs:
            params = {key: values[key] for key in comb} # type: ignore
            f = gen_func(args=1, defaults=1, **params) # type: ignore
            fs.append(f)

    for f in fs:
        with pytest.raises(TypeError):
            VectorPartial(f, n_args=1)


def test_func_with_non_default_kwonly(gen_func_args): # type: ignore
    values = gen_func_args # type: ignore
    fs: list[Callable[..., None]] = []
    fs.append(gen_func(args=1, kwonly=1))
    for r in range(1, 4):
        combs = combinations(("varargs", "kwonlydefaults", "varkwargs"), r)
        for comb in combs:
            params = {key: values[key] for key in comb} # type: ignore
            f = gen_func(args=1, kwonly=1, **params) # type: ignore
            fs.append(f)

    for f in fs:
        with pytest.raises(TypeError):
            VectorPartial(f, n_args=1)


def test_func_n_args(gen_func_args): # type: ignore
    values = gen_func_args # type: ignore
    n_args = 5
    fs: list[Callable[..., None]] = []
    fs.append(gen_func(args=n_args))
    for r in range(1, 3):
        combs = combinations(("kwonlydefaults", "varkwargs"), r)
        for comb in combs:
            params = {key: values[key] for key in comb} # type: ignore
            f = gen_func(args=n_args, **params) # type: ignore
            fs.append(f)

    repeat = 5
    for f in fs:
        correct_n = n_args
        VectorPartial(f, n_args=correct_n)
        with pytest.raises(TypeError):
            VectorPartial(f, n_args=correct_n+1)
        with pytest.raises(TypeError):
            VectorPartial(f, n_args=correct_n-1)
        for _ in range(repeat):
            wrong_n = random.randint(correct_n + 2, 100)
            with pytest.raises(TypeError):
                VectorPartial(f, n_args=wrong_n)
        for _ in range(repeat):
            wrong_n = random.randint(1, correct_n - 2)
            with pytest.raises(TypeError):
                VectorPartial(f, n_args=wrong_n)


def test_func_n_args_with_varargs(gen_func_args): # type: ignore
    values = gen_func_args # type: ignore
    n_args = 5
    fs: list[Callable[..., None]] = []
    fs.append(gen_func(args=n_args, varargs=True))
    for r in range(1, 3):
        combs = combinations(("kwonlydefaults", "varkwargs"), r)
        for comb in combs:
            params = {key: values[key] for key in comb} # type: ignore
            f = gen_func(args=n_args, varargs=True, **params) # type: ignore
            fs.append(f)

    repeat = 5
    for f in fs:
        correct_n = n_args
        VectorPartial(f, n_args=correct_n)
        VectorPartial(f, n_args=correct_n+1)
        with pytest.raises(TypeError):
            VectorPartial(f, n_args=correct_n-1)
        for _ in range(repeat):
            wrong_n = random.randint(correct_n + 2, 100)
            VectorPartial(f, n_args=wrong_n)
        for _ in range(repeat):
            wrong_n = random.randint(1, correct_n - 2)
            with pytest.raises(TypeError):
                VectorPartial(f, n_args=wrong_n)


def test_wrong_pos(gen_func_args): # type: ignore
    values = gen_func_args # type: ignore
    n_args = 3
    fs: list[Callable[..., None]] = []
    fs.append(gen_func(args=n_args))
    for r in range(1, 3):
        combs = combinations(("varargs", "kwonlydefaults", "varkwargs"), r)
        for comb in combs:
            params = {key: values[key] for key in comb} # type: ignore
            f = gen_func(args=n_args, **params) # type: ignore
            fs.append(f)

    repeat = 5
    for f in fs:
        s = VectorPartial(f, n_args=n_args)
        assert not s.can_return
        assert s.missing_args == n_args

        with pytest.raises(IndexError):
            s.insert(None, pos=-1)
        assert not s.can_return
        assert s.missing_args == n_args

        with pytest.raises(IndexError):
            s.remove(-1)
        assert not s.can_return
        assert s.missing_args == n_args

        with pytest.raises(IndexError):
            s.insert(None, pos=0)
        assert not s.can_return
        assert s.missing_args == n_args

        with pytest.raises(IndexError):
            s.remove(0)
        assert not s.can_return
        assert s.missing_args == n_args

        with pytest.raises(IndexError):
            s.insert(None, pos=n_args+1)
        assert not s.can_return
        assert s.missing_args == n_args

        with pytest.raises(IndexError):
            s.remove(n_args+1)
        assert not s.can_return
        assert s.missing_args == n_args

        for _ in range(repeat):
            wrong_pos = random.randint(n_args + 2, 100)

            with pytest.raises(IndexError):
                s.insert(None, pos=wrong_pos)
            assert not s.can_return
            assert s.missing_args == n_args

            with pytest.raises(IndexError):
                s.remove(wrong_pos)
            assert not s.can_return
            assert s.missing_args == n_args

        for _ in range(repeat):
            wrong_pos = random.randint(-100, -2)

            with pytest.raises(IndexError):
                s.insert(None, pos=wrong_pos)
            assert not s.can_return
            assert s.missing_args == n_args

            with pytest.raises(IndexError):
                s.remove(wrong_pos)
            assert not s.can_return
            assert s.missing_args == n_args


def test_insert(gen_func_args): # type: ignore
    values = gen_func_args # type: ignore
    n_args = 3
    fs: list[Callable[..., None]] = []
    fs.append(gen_func(args=n_args))
    for r in range(1, 3):
        combs = combinations(("varargs", "kwonlydefaults", "varkwargs"), r)
        for comb in combs:
            params = {key: values[key] for key in comb} # type: ignore
            f = gen_func(args=n_args, **params) # type: ignore
            fs.append(f)

    for f in fs:
        for ps in permutations(range(1, n_args + 1)):
            s = VectorPartial(f, n_args=n_args)
            assert not s.can_return
            assert s.missing_args == n_args

            added_ps: list[int] = []
            for p in ps[:-1]:
                s.insert(None, pos=p)
                assert not s.can_return
                added_ps.append(p)
                assert s.missing_args == n_args - len(added_ps)

                for added_p in added_ps:
                    s.insert(None, pos=added_p)
                    assert not s.can_return
                    assert s.missing_args == n_args - len(added_ps)

            s.insert(None, pos=ps[-1])
            assert s.can_return
            assert s.missing_args == 0

            for p in ps:
                s.insert(None, pos=p)
                assert s.can_return
                assert s.missing_args == 0


def test_remove(gen_func_args): # type: ignore
    values = gen_func_args # type: ignore
    n_args = 3
    fs: list[Callable[..., None]] = []
    fs.append(gen_func(args=n_args))
    for r in range(1, 3):
        combs = combinations(("varargs", "kwonlydefaults", "varkwargs"), r)
        for comb in combs:
            params = {key: values[key] for key in comb} # type: ignore
            f = gen_func(args=n_args, **params) # type: ignore
            fs.append(f)

    for f in fs:
        s = VectorPartial(f, n_args=n_args)

        s.insert(None, pos=1)
        s.remove(1)
        assert not s.can_return
        assert s.missing_args == n_args

        for ps in permutations(range(1, n_args + 1)):
            for p in range(1, n_args + 1):
                s.insert(None, pos=p)
            assert s.can_return
            assert s.missing_args == 0

            count = 0
            for p in ps:
                s.remove(p)
                count += 1
                assert not s.can_return
                assert s.missing_args == count


def test_call():
    n_args = 3
    body = f"return ({','.join(f'_{n}' for n in range(n_args))})"
    f = gen_func(args=n_args, body=body)
    answer = (1, 2, 3)

    s = VectorPartial(f, n_args=n_args)
    assert not s.can_return
    with pytest.raises(RuntimeError):
        s()

    for ps in permutations(range(1, 4)):
        s = VectorPartial(f, n_args=n_args)
        for p in ps[:-1]:
            s.insert(p, pos=p)
            assert not s.can_return
            with pytest.raises(RuntimeError):
                s()

        s.insert(ps[-1], pos=ps[-1])
        assert s.can_return
        assert s() == answer
