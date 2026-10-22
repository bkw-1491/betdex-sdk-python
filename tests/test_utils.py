from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pytest

from betdex.exceptions import BetDexDecodeError
from betdex.resources import BaseModel
from betdex.streaming.resources import PriceLevel
from betdex.utils import decode


@dataclass(kw_only=True, slots=True)
class Inner(BaseModel):
    seen_at: datetime
    price: float = 0.0


@dataclass(kw_only=True, slots=True)
class Outer(BaseModel):
    name: str
    inners: list[Inner] = field(default_factory=list)
    by_id: dict[str, Inner] = field(default_factory=dict)
    inner: Inner | None = None
    extra: Any = None


LEVEL = {"side": "For", "outcomeId": "o", "price": 2, "liquidity": 10.0, "change": 0.0,
         "validAt": "2026-10-22T13:46:14.555Z"}


def test_decodes_nested_models_from_camel_case_ignoring_unknown_keys():
    outer = decode(Outer, {"name": "a", "unknownKey": 1, "extra": {"x": [1]},
                           "inners": [{"seenAt": "2026-10-22T13:41:15Z", "price": 2}, None],
                           "byId": {"k": {"seenAt": "2026-10-22T13:41:15.5+0100"}},
                           "inner": None})
    assert outer.inners[0] == Inner(seen_at=datetime(2026, 10, 22, 13, 41, 15,
                                                     tzinfo=timezone.utc), price=2.0)
    assert type(outer.inners[0].price) is float and outer.inners[1] is None
    assert outer.by_id["k"].seen_at.utcoffset().total_seconds() == 3600
    assert (outer.inner, outer.extra) == (None, {"x": [1]})


def test_a_list_sent_as_null_keeps_its_default():
    assert decode(Outer, {"name": "a", "inners": None}).inners == []


def test_ints_become_floats_but_bools_stay():
    assert decode(float, 2) == 2.0 and type(decode(float, 2)) is float
    assert decode(float, True) is True
    assert decode(PriceLevel, LEVEL).price == 2.0


@pytest.mark.parametrize("tp, value", [
    (PriceLevel, {"side": "For"}),  # required fields missing
    (PriceLevel, "not an object"),
    (list[int], 5),
    (Inner, {"seenAt": "yesterday"}),
])
def test_a_wrong_shape_is_a_decode_error(tp, value):
    with pytest.raises(BetDexDecodeError):
        decode(tp, value)


def test_decoding_again_gives_the_same():
    assert decode(PriceLevel, LEVEL) == decode(PriceLevel, dict(LEVEL))
