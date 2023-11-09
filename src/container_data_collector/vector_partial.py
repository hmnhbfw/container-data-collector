"""VectorPartial class that describes a simplified partial object with only
non-default positional arguments and only default keyword arguments.
"""

import inspect
from enum import IntEnum, auto
from dataclasses import dataclass
from collections.abc import Callable
from typing import Any, Concatenate, Generic, TypeAlias, TypeVar, final


@final
class ArgState(IntEnum):
    """Stub saying that there is no argument."""
    NONE = auto()


FixedT = TypeVar("FixedT")
ReturnT = TypeVar("ReturnT")

FuncWithFixedArgType: TypeAlias = Callable[Concatenate[FixedT, ...], ReturnT]

@final
@dataclass(slots=True, eq=False, match_args=False)
class VectorPartial(Generic[FixedT, ReturnT]):
    """Partial wrapper for functions with the fixed type of the first argument
    and the return value type. The arguments of the functions can be added and
    removed at the specified position in constant time. After all the argument
    was added, the function can be called, but if someone of the arguments is
    missing, raise 'RuntimeError'.

    These function must have at least one positional argument, can't have any
    positional arguments with a default value, and can't have any keyword
    arguments without a default value. Raise 'TypeError' if someone of these
    conditions is not complited.

    Passed the 'n_args' argument describes how many positional arguments
    the function has to take. Raise 'IndexError' if it is not greater than zero.
    Raise 'TypeError' if it doesn't match with the number of the function's
    arguments.
    """

    _func: FuncWithFixedArgType[FixedT, ReturnT]
    _args: list[Any]
    _missing_args: int

    def __init__(self, func: FuncWithFixedArgType[FixedT, ReturnT], /, *, n_args: int) -> None:
        VectorPartial._init_check(func, n_args=n_args)
        self._func = func
        self._args = [ArgState.NONE for _ in range(n_args)]
        self._missing_args = n_args

    def __call__(self) -> ReturnT:
        if not self.can_return:
            msg = f"The function {self._func.__name__} can't be called:"
            hint = f"it is still waiting {self.missing_args} arguments."
            raise RuntimeError(msg + " " + hint)
        return self._func(*self._args)

    @property
    def can_return(self) -> bool:
        """Return True if the function is ready to be called."""
        return self._missing_args == 0

    @property
    def missing_args(self) -> int:
        """Return the number of the arguments the function is waiting."""
        return self._missing_args

    def insert(self, arg: Any, /, *, pos: int) -> None:
        """Insert a new argument of the function at the specified position.
        Raise 'IndexError' if 'pos' is not greater than zero and less or equal
        than a number of the arguments that the function expects.
        """
        self._check_pos(pos)
        index = pos - 1
        if self._args[index] is ArgState.NONE:
            self._missing_args -= 1
        self._args[index] = arg

    def remove(self, pos: int) -> None:
        """Remove an argument at the specified position.
        Raise 'IndexError' if 'pos' is not greater than zero and less or equal
        than a number of the arguments that the function expects.
        """
        self._check_pos(pos)
        index = pos - 1
        if self._args[index] is not ArgState.NONE:
            self._args[index] = ArgState.NONE
            self._missing_args += 1

    def _check_pos(self, pos: int) -> None:
        if not 0 < pos <= len(self._args):
            msg = f"Constraint 0 < pos <= {len(self._args)} is not satisfied.\n"
            hint = f"Instead, {pos=} is given."
            raise IndexError(msg + hint)

    @staticmethod
    def _init_check(func: FuncWithFixedArgType[FixedT, ReturnT], /, *, n_args: int) -> None:
        if n_args < 1:
            message = f"n_args must be greater than zero.\nInstead, {n_args=} is given."
            raise IndexError(message)

        spec = inspect.getfullargspec(func)
        msg_head = f"The function '{func.__name__}'"
        if not spec.args:
            msg = f"{msg_head} must have at least one positional argument."
            raise TypeError(msg)
        if spec.defaults:
            msg = f"{msg_head} can't have any default positional arguments."
            raise TypeError(msg)
        if spec.kwonlyargs and len(spec.kwonlyargs) != len(spec.kwonlydefaults or []):
            msg = f"{msg_head} can't have non-default keyword arguments."
            raise TypeError(msg)
        if spec.varargs and n_args < len(spec.args):
            msg = f"{msg_head} must have at least {n_args=} positional arguments."
            hint = f"Instead, {len(spec.args)} is given."
            raise TypeError(msg + "\n" + hint)
        if not spec.varargs and n_args != len(spec.args):
            msg = f"{msg_head} must have exactly {n_args=} arguments."
            hint = f"Instead, {len(spec.args)} is given."
            raise TypeError(msg + "\n" + hint)
