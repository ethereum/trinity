from typing import (
    Any,
    Iterable,
    Sequence,
)

from eth_utils import (
    ValidationError,
)


def update_tuple_item(tuple_data: Sequence[Any],
                      index: int,
                      new_value: Any) -> Iterable[Any]:
    """
    Update the ``index``th item of ``tuple_data`` to ``new_value``
    """
    list_data = list(tuple_data)

    try:
        list_data[index] = new_value
    except IndexError:
        raise ValidationError(
            "the length of the given tuple_data is {}, the given index {} is out of index".format(
                len(tuple_data),
                index,
            )
        )
    else:
        return tuple(list_data)
