from __future__ import annotations

from typing import Any, Callable, Type
from collections.abc import Collection

from attrs import converters, define, field, fields
from joblib import Parallel, delayed

from downsat.etl import WeakIdKeyDictionary, types


RUN_CONTEXT_DUNDER = "_etl_run_context_"
_global_context_dict: WeakIdKeyDictionary = WeakIdKeyDictionary()


@define
class RunContext:
    """Class defining run context properties and related operations.

    :param num_workers: Number of workers to use for parallel jobs.
    :param max_workers: Maximum number of workers to use for parallel jobs.
        Takes precedence over num_workers.

    # TODO: test
    """

    num_workers: int | None = field(default=None, converter=converters.optional(int), kw_only=True)
    max_workers: int | None = field(default=None, converter=converters.optional(int), kw_only=True)

    @classmethod
    def from_obj(cls, obj: Any) -> RunContext:
        return cls(**getcontext(obj))

    def get_num_workers(self) -> int | None:
        """Get number of workers to be used for parallel computing."""
        if self.num_workers is None:
            return self.max_workers
        else:
            if self.max_workers is None:
                return self.num_workers
            else:
                return min(self.num_workers, self.max_workers)

    def map(
        self, fun: Callable[[types.InputType], types.OutputType], container: Collection[types.InputType]
    ) -> tuple[types.OutputType, ...]:
        """Apply funciton on all elements of an iterable.

        Uses joblib.Parallel if min(self.num_workers, self.max_workers) > 1.

        :param fun: Function to be applied.
        :param container: Iterable.
        :returns: Tuple of results.

        # TODO: allow selecting different parallel backend
        """
        num_workers = self.get_num_workers()
        if num_workers is None:
            # set default number of workers
            # TODO: if the parallel processing branch below would execute the paralelization within
            #       `with context(num_workers=1):` or `with context(num_workers=num_workers-len(container)):`
            #       and self.get_num_workers() in subtasks would take this number of num_workers as default, we could guarantee
            #       that the whole chain will use max num_workers unless some of the subcomponents overrides that by specifying
            #       its own number of workers
            # TODO: use some global switch to change to num_workers = -1 as default (i.e. use all CPUs as default)
            num_workers = 1

        if num_workers > 1 and len(container) > 1:
            # parallel processing
            results = Parallel(n_jobs=num_workers)(delayed(fun)(item) for item in container)
        else:
            # serial processing
            results = [fun(item) for item in container]

        return tuple(results)


def getcontext(obj: Type[Any]) -> dict[str, Any]:
    """Get context values of given object.

    Merges context values stored locally in the object and those in the global
    context dict. Local objects have priority in case of conflict.

    :param obj: Object whose context is being queried.
    :returns: Context dictionary of the object.
    """
    context = _global_context_dict.get(obj, {})
    class_context = getattr(obj, RUN_CONTEXT_DUNDER, {})
    context.update(class_context)

    return context


def setcontext(_strict: bool = True, **kwargs: Any) -> Callable[[Any], None]:
    """Decorator to set context property on a class.

    The properties are stored either in a dunder attribute with name given by
    `downsat.etl.context.RUN_CONTEXT_DUNDER` or in a global context dictionary.

    :param obj: Object whose context should be set.
    :param name: Name of the context property.
    :param value: Value of the context property.
    :param _strict: Raise if the name does not match any property of the RunContext class?
    :raises TypeError: Not possible to add context variable to this type of object.
    """
    if _strict:
        invalid_names = set(kwargs) - set([f.name for f in fields(RunContext)])
        if len(invalid_names) > 0:
            raise ValueError(f"Invalid run context property: {invalid_names}")

    def _setcontext(obj: Any) -> None:
        try:
            try:
                # try to store in an object attribute
                context = getattr(obj, RUN_CONTEXT_DUNDER)
            except AttributeError:
                # not possible, store in the global context dict
                _global_context_dict.setdefault(obj, {})
                context = _global_context_dict[obj]
        except Exception as e:
            # all failed
            raise TypeError(f"Cannot set context for object of type {type(obj)}. ") from e

        context.update(kwargs)

    return _setcontext
