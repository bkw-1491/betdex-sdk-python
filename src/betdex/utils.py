import base64
import dataclasses
import inspect
import os
import re
import types
import typing
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import datetime, timezone
from functools import cache
from typing import Any, Literal, TypeVar, Union

from betdex.exceptions import BetDexDecodeError, BetDexInvalidRequestError

T = TypeVar("T")
Item = TypeVar("Item")
MAX_PAGE_SIZE = 2000

# Most orders the API takes in one ``create_orders`` or ``create_order_requests`` call.
MAX_BATCH_SIZE = 50

# Endpoints that reject pages above 500 rows instead of clamping.
PAGE_SIZE_LIMITS = {"get_orders": 500, "get_orders_settled": 500, "get_market_orders": 500}

CAMEL_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")
ISO_DATETIME = re.compile(
    r"(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?)"
    r"(?:\.(?P<frac>\d+))?"
    r"(?P<tz>Z|[+-]\d{2}:?\d{2})?$"
)


@cache
def to_snake_case(key: str) -> str:
    """
    Convert an API key to its field name, e.g. ``_totalElements`` -> ``total_elements``.

    :param key: JSON key.
    :returns: Field name.
    """
    return CAMEL_BOUNDARY.sub("_", key.lstrip("_")).lower()


def to_camel_case(name: str) -> str:
    """
    Convert a field name to its API key, e.g. ``keep_when_in_play`` -> ``keepWhenInPlay``.

    :param name: Field name.
    :returns: JSON key.
    """
    head, *rest = name.split("_")
    return head + "".join(part.title() for part in rest)


def parse_datetime(value: str) -> datetime:
    """
    Parse an API timestamp into an aware datetime.

    Digits beyond microseconds are truncated; no offset means UTC.

    :param value: ISO 8601 string, e.g. ``"2026-06-23T14:05:09.123456789Z"``.
    :returns: The timestamp.
    :raises BetDexDecodeError: If ``value`` is not an ISO 8601 date-time.
    """
    match = ISO_DATETIME.match(value)
    if not match:
        raise BetDexDecodeError(f"not an ISO 8601 date-time: {value!r}")
    text = match["base"]
    if match["frac"]:
        text += "." + match["frac"][:6].ljust(6, "0")
    tz = match["tz"]
    if tz and tz != "Z":
        text += tz if ":" in tz else f"{tz[:3]}:{tz[3:]}"
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def format_datetime(value: datetime) -> str:
    """
    Format a datetime for a query filter.

    :param value: Aware datetime, or naive meaning UTC.
    :returns: UTC with milliseconds, e.g. ``"2026-06-23T14:05:09.123Z"``.
    """
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


@cache
def get_fields(cls: type) -> dict[str, Any]:
    """
    Resolve a dataclass's init field types, cached per class.

    :param cls: Model class.
    :returns: Field name to resolved type.
    """
    hints = typing.get_type_hints(cls)
    return {f.name: hints[f.name] for f in dataclasses.fields(cls) if f.init}


def decode(tp: Any, value: Any) -> Any:
    """
    Convert decoded JSON into ``tp``.

    Models ignore keys they don't declare; a union of models picks the one whose
    fields cover most of the object's keys.

    :param tp: A model, ``list[...]``, ``dict[str, ...]``, ``X | None``, ``datetime``, ``Literal`` alias or scalar.
    :param value: Decoded JSON value.
    :returns: The converted value; ``None`` stays ``None``.
    :raises BetDexDecodeError: If ``value`` doesn't have the shape of ``tp``.
    """
    return None if value is None else decoder(tp)(value)


@cache
def decoder(tp: Any) -> Callable[[Any], Any]:
    """
    A function converting a non-null JSON value into ``tp``, worked out once per type:
    ``decode`` runs on every streamed message.
    """
    origin = typing.get_origin(tp)

    if origin in (Union, types.UnionType):
        options = [a for a in typing.get_args(tp) if a is not type(None)]
        models = [o for o in options if dataclasses.is_dataclass(o)]
        if len(options) == 1 or not models:
            return decoder(options[0])
        first = decoder(options[0])

        def union(value: Any) -> Any:
            if not isinstance(value, dict):
                return first(value)
            # Polymorphic lists (``Order | OrderFailure``) carry no type tag, so the
            # model covering most of the object's keys wins; ties go to the first.
            keys = {to_snake_case(k) for k in value}
            return decoder(max(models, key=lambda m: len(keys & set(get_fields(m)))))(value)
        return checked(tp, union)

    if origin is list:
        (item,) = typing.get_args(tp)
        each = decoder(item)
        return checked(tp, lambda value: [None if v is None else each(v) for v in value])

    if origin is dict:
        _, item = typing.get_args(tp)
        each = decoder(item)
        return checked(tp, lambda value: {k: None if v is None else each(v)
                                          for k, v in value.items()})

    if origin is Literal or tp is Any:
        return identity

    if dataclasses.is_dataclass(tp):
        return checked(tp, model_decoder(tp))

    if tp is datetime:
        return checked(tp, parse_datetime)

    if tp is float:
        return checked(tp, lambda value: float(value) if type(value) is int else value)

    return identity


def model_decoder(tp: Any) -> Callable[[dict[str, Any]], Any]:
    """
    A model's decoder: each JSON key's field, and how to decode it, looked up once per key.
    """
    fields = get_fields(tp)
    keys: dict[str, tuple[str, Callable[[Any], Any], bool] | None] = {}
    has_raw = "raw" in fields

    def field(key: str) -> tuple[str, Callable[[Any], Any], bool] | None:
        name = to_snake_case(key)
        if name not in fields:
            return None
        return name, decoder(fields[name]), typing.get_origin(fields[name]) is list

    def model(value: dict[str, Any]) -> Any:
        kwargs: dict[str, Any] = {}
        for key, item in value.items():
            spec = keys[key] if key in keys else keys.setdefault(key, field(key))
            if spec is None:
                continue
            name, each, is_list = spec
            if item is None:
                # A list field the API sent as null keeps its empty-list default.
                if not is_list:
                    kwargs[name] = None
            else:
                kwargs[name] = item if each is identity else each(item)
        if has_raw and "raw" not in kwargs:
            kwargs["raw"] = value
        try:
            return tp(**kwargs)
        except TypeError as exc:  # a required field is missing
            raise BetDexDecodeError(f"{tp.__name__}: {exc}") from exc
    return model


def checked(tp: Any, convert: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """
    ``convert``, raising ``BetDexDecodeError`` for a value of the wrong shape.
    """
    def run(value: Any) -> Any:
        try:
            return convert(value)
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            # Wrong shapes (a string where a list belongs, say) surface here.
            raise BetDexDecodeError(f"cannot decode {value!r:.100} as {tp}") from exc
    return run


def identity(value: Any) -> Any:
    return value


def encode(value: Any) -> Any:
    """
    Convert a model, or a structure of models, to JSON-ready data.

    ``None`` fields are omitted, so optional request fields are only sent when set.

    :param value: Model, list, dict, datetime or scalar.
    :returns: The JSON-ready equivalent, with camelCase keys.
    """
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            to_camel_case(f.name): encode(getattr(value, f.name))
            for f in dataclasses.fields(value)
            if f.init and f.name != "raw" and getattr(value, f.name) is not None
        }
    if isinstance(value, list):
        return [encode(v) for v in value]
    if isinstance(value, dict):
        return {k: encode(v) for k, v in value.items()}
    if isinstance(value, datetime):
        return format_datetime(value)
    return value


def format_query_params(params: Mapping[str, Any]) -> dict[str, Any]:
    """
    Encode query parameters the way the API expects.

    Lists are comma-joined, except ``sort``, whose entries contain commas and are
    repeated instead.

    :param params: Raw parameters; ``None`` values are dropped.
    :returns: Parameters ready for ``requests``.
    """
    out: dict[str, Any] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, str):
            out[key] = value
        elif isinstance(value, bool):
            out[key] = "true" if value else "false"
        elif isinstance(value, datetime):
            out[key] = format_datetime(value)
        elif isinstance(value, Sequence):
            if key == "sort":
                out[key] = [str(v) for v in value]
            else:
                out[key] = ",".join(str(v) for v in value)
        else:
            out[key] = str(value)
    return out

    
def paginate(method: Callable[..., T], *args: Any, **kwargs: Any) -> Iterator[T]:
    """
    Call a paged endpoint repeatedly, yielding each page's response.

    Works with page-numbered endpoints (``page``/``size``, ``size`` defaulting to the
    endpoint's largest page) and keyset ones (``offset``/``secondary_offset``). Keyset
    cursors are inclusive, so each later page would repeat the previous page's
    last row; that row is dropped. Stop early with ``itertools.islice`` or ``break``.

    :param method: A client method, e.g. ``client.get_markets``.
    :param args: Positional arguments for ``method``.
    :param kwargs: Keyword arguments for ``method``; ``page`` or ``offset`` sets where to start.
    :returns: An iterator of page responses, in order.
    :raises BetDexInvalidRequestError: If ``method`` is not paged (on first iteration).
    :raises BetDexInvalidRequestError: If a keyset cursor stops advancing (e.g. ``limit=1``).
    """
    params = inspect.signature(method).parameters
    if "page" in params:
        default_size = PAGE_SIZE_LIMITS.get(getattr(method, "__name__", ""), MAX_PAGE_SIZE)
        kwargs = {"size": default_size, **kwargs}
        page = kwargs.pop("page", None) or 0
        while True:
            response: Any = method(*args, page=page, **kwargs)
            yield response
            position = response.meta.page if response.meta else None
            page += 1
            if position is None or not response.meta.count or page >= (position.total_pages or 0):
                return

    elif "offset" in params:
        last_id = None
        while True:
            response: Any = method(*args, **kwargs)
            documents = getattr(response, "documents", None)
            primary = response.meta.primary_document if response.meta else None
            rows = getattr(documents, to_snake_case(primary), []) if documents and primary else []
            if rows and last_id is not None and getattr(rows[0], "id", None) == last_id:
                del rows[0]
            if rows:
                last_id = getattr(rows[-1], "id", None)
            yield response
            cursor = response.meta.next_offset if response.meta else None
            if cursor is None:
                return
            if (cursor.offset, cursor.secondary_offset) == (
                    kwargs.get("offset"), kwargs.get("secondary_offset")):
                raise BetDexInvalidRequestError(
                    "keyset cursor did not advance; use a limit above 1")
            kwargs["offset"] = cursor.offset
            kwargs["secondary_offset"] = cursor.secondary_offset

    else:
        raise BetDexInvalidRequestError(f"{getattr(method, '__name__', method)} is not paged")


def batch(
    method: Callable[[Sequence[Item]], T],
    items: Sequence[Item],
    size: int = MAX_BATCH_SIZE,
) -> Iterator[T]:
    """
    Send ``items`` through a batch endpoint ``size`` at a time, yielding each response.

    Nothing is sent until the iterator is consumed, one call per step, so you
    can see what each batch did before the next goes, and ``break`` to stop.
    If a call raises, the batches before it have already been sent::

        for result in batch(client.create_orders, requests):
            placed += [o for o in result.orders if isinstance(o, Order)]

    :param method: A client method taking a list, e.g. ``client.create_orders``
        or ``client.create_order_requests``.
    :param items: Everything to send, in order.
    :param size: Items per call, 1 to ``MAX_BATCH_SIZE``.
    :returns: An iterator of responses, one per batch, in order.
    :raises BetDexInvalidRequestError: If ``size`` is out of range (on first iteration).
    """
    if not 1 <= size <= MAX_BATCH_SIZE:
        raise BetDexInvalidRequestError(f"size must be 1 to {MAX_BATCH_SIZE}, not {size}")
    for start in range(0, len(items), size):
        yield method(items[start:start + size])


def new_reference(prefix: str = "") -> str:
    """
    Generate a short, unique client reference for an order.

    11 URL-safe characters carrying 64 random bits, so collisions are negligible;
    drawn from ``os.urandom``, so it stays unique across forked processes.

    :param prefix: Text to put in front, e.g. a strategy name.
    :returns: ``prefix`` followed by the random part.
    """
    return prefix + base64.urlsafe_b64encode(os.urandom(8)).rstrip(b"=").decode()
