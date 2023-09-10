from __future__ import annotations

from downsat.etl import types


def flatten(
    # TODO: recursive definition of the input type to express that each element can be a nested tuple of InputType
    items: types.InputType | tuple[types.InputType, ...],
    depth: int = 1,
) -> types.InputType | tuple[types.InputType, ...]:
    """Flatten items to a single tuple.

    E.g. (item1, (item2, item3)) -> (item1, item2, item3)
         item -> item

    :param items: Item or list or tuples or sets of (items or lists or tuples or sets of (items or lists or tuples or sets of (items or ...) ...))
    :param depth: Maximum level of flattenings Must be positive.
    """
    if depth < 1:
        raise ValueError(f"depth must be positive, got {depth}.")

    for _ in range(depth):
        if isinstance(items, tuple):
            # (item, ...) or ((item, ...), ...) or (item, (item, ...), ...)
            # TODO: simplify?
            items_list = []
            for item in items:
                if isinstance(item, (tuple, list, set)):
                    items_list.extend(list(item))
                else:
                    items_list.append(item)
            items = tuple(items_list)

    return items
