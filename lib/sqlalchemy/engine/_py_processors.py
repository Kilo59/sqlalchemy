# sqlalchemy/processors.py
# Copyright (C) 2010-2023 the SQLAlchemy authors and contributors
# <see AUTHORS file>
# Copyright (C) 2010 Gaetan de Menten gdementen@gmail.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""defines generic type conversion functions, as used in bind and result
processors.

They all share one common characteristic: None is passed through unchanged.

"""

from __future__ import annotations

import datetime
from datetime import date as date_cls
from datetime import datetime as datetime_cls
from datetime import time as time_cls
from decimal import Decimal
import typing
from typing import Any
from typing import Callable
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union


_DT = TypeVar(
    "_DT", bound=Union[datetime.datetime, datetime.time, datetime.date]
)


def str_to_datetime_processor_factory(
    regexp: typing.Pattern[str], type_: Callable[..., _DT]
) -> Callable[[Optional[str]], Optional[_DT]]:
    rmatch = regexp.match
    # Even on python2.6 datetime.strptime is both slower than this code
    # and it does not support microseconds.
    has_named_groups = bool(regexp.groupindex)

    def process(value: Optional[str]) -> Optional[_DT]:
        if value is None:
            return None
        try:
            m = rmatch(value)
        except TypeError as err:
            raise ValueError(
                "Couldn't parse %s string '%r' "
                "- value is not a string." % (type_.__name__, value)
            ) from err

        if m is None:
            raise ValueError(f"Couldn't parse {type_.__name__} string: '{value}'")
        if has_named_groups:
            groups = m.groupdict(0)
            return type_(
                **dict(
                    list(
                        zip(
                            iter(groups.keys()),
                            list(map(int, iter(groups.values()))),
                        )
                    )
                )
            )
        else:
            return type_(*list(map(int, m.groups(0))))

    return process


def to_decimal_processor_factory(
    target_class: Type[Decimal], scale: int
) -> Callable[[Optional[float]], Optional[Decimal]]:
    fstring = "%%.%df" % scale

    def process(value: Optional[float]) -> Optional[Decimal]:
        return None if value is None else target_class(fstring % value)

    return process


def to_float(value: Optional[Union[int, float]]) -> Optional[float]:
    return None if value is None else float(value)


def to_str(value: Optional[Any]) -> Optional[str]:
    return None if value is None else str(value)


def int_to_boolean(value: Optional[int]) -> Optional[bool]:
    return None if value is None else bool(value)


def str_to_datetime(value: Optional[str]) -> Optional[datetime.datetime]:
    return datetime_cls.fromisoformat(value) if value is not None else None


def str_to_time(value: Optional[str]) -> Optional[datetime.time]:
    return time_cls.fromisoformat(value) if value is not None else None


def str_to_date(value: Optional[str]) -> Optional[datetime.date]:
    return date_cls.fromisoformat(value) if value is not None else None
