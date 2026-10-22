import json
import queue

from typing import Any

from betdex.utils import decode
from betdex.exceptions import BetDexDecodeError
from betdex.streaming.resources import (
    UPDATE_TYPES,
    ErrorMessage,
    MarketBook,
    MarketPriceUpdate,
    PriceLevel,
    StreamConnected,
    AuthenticationUpdate,
    SubscribeUpdate,
    UnsubscribeUpdate,
    Update, PriceKey,
)



def parse(data: Any) -> Update | ErrorMessage | dict[str, Any]:
    """
    Decode one stream message into its model.

    :param data: One decoded JSON message.
    :returns: The model for its ``type``; an ``ErrorMessage`` for a refusal; the
        dict itself for a type this SDK doesn't know yet.
    :raises BetDexDecodeError: If the message isn't an object, or doesn't fit its model.
    """
    if not isinstance(data, dict):
        raise BetDexDecodeError(f"stream message is not an object: {data!r:.100}")
    model = UPDATE_TYPES.get(data.get("type", ""))
    if model is not None:
        return decode(model, data)
    if "message" in data and "requestId" in data:
        return decode(ErrorMessage, data)
    return data


class BaseListener:
    """
    Receives every frame a ``BetDexStream`` reads, as text; does nothing with it.
    Subclass and override ``on_data`` to handle frames yourself.
    """

    def on_data(self, raw: str) -> bool | None:
        """
        Handle one frame. Runs on the thread that called ``BetDexStream.start``.

        :param raw: The frame's text: a JSON object, or a list of them.
        :returns: ``False`` to stop the stream.
        """
        return None

    def on_connect(self, connection_id: str) -> None:
        """
        Called by ``BetDexStream.connect`` once authenticated, before subscribing,
        on every connect and reconnect. Runs on the thread that connected.

        :param connection_id: The connection id the stream gave.
        """


class StreamListener(BaseListener):
    """
    Decodes frames into models and puts them on ``output_queue``.

    ``MarketPriceUpdate`` messages are applied to a per-market book, and the
    updated ``MarketBook`` is queued instead. Every other message is queued as
    decoded, refusals (``ErrorMessage``, ``SubscribeNotAuthenticatedUpdate``)
    included, except the confirmations (``AuthenticationUpdate``,
    ``SubscribeUpdate``, ``UnsubscribeUpdate``). A message of an unknown type,
    or one that doesn't fit its model, is queued as its dict, and a frame that
    isn't JSON as its text, so nothing is dropped silently.

    On every connect the books are dropped and ``StreamConnected`` is queued:
    the stream can't resume, so a book from the last connection may hold levels
    long gone. Everything queued before it came from the last connection.

    :param output_queue: Where updates go; a new ``queue.Queue`` if omitted.
    """

    def __init__(self, output_queue: queue.Queue[Any] | None = None) -> None:
        """
        Create the listener; see the class docstring for parameters.
        """
        self.output_queue: queue.Queue[Any] = (
            queue.Queue() if output_queue is None else output_queue)
        self.market_books: dict[str, MarketBook] = {}
        self._levels: dict[str, dict[PriceKey, PriceLevel]] = {}

    def on_data(self, raw: str) -> bool | None:
        """
        Decode a frame and queue what it carries.

        :param raw: The frame's text.
        :returns: ``None``.
        """
        try:
            data = json.loads(raw)
        except ValueError:
            self.output_queue.put(raw)
            return None
        for item in data if isinstance(data, list) else [data]:
            try:
                update = parse(item)
            except BetDexDecodeError:
                update = item
            self.on_update(update)
        return None

    def on_connect(self, connection_id: str) -> None:
        """
        Drop every book, in place, and queue ``StreamConnected``.

        :param connection_id: The connection id the stream gave.
        """
        self.market_books.clear()
        self._levels.clear()
        self.output_queue.put(StreamConnected(connection_id=connection_id))

    def on_update(self, update: Any) -> None:
        """
        Queue one decoded message; see the class docstring for what is queued.

        :param update: The decoded message, or the raw one if it couldn't be decoded.
        """
        if isinstance(update, MarketPriceUpdate):
            self.output_queue.put(self.create_snapshot(update))
        elif not isinstance(update, (AuthenticationUpdate, SubscribeUpdate, UnsubscribeUpdate)):
            self.output_queue.put(update)

    def create_snapshot(self, update: MarketPriceUpdate) -> MarketBook:
        """
        Apply a price update to its market's book.

        A snapshot replaces the book; an incremental replaces the levels it
        carries, and drops those left with no liquidity.

        :param update: The update.
        :returns: The market's book after the update.
        """
        levels = {} if update.update_type == "Snapshot" else self._levels.get(update.market_id, {})
        for level in update.prices:
            key = (level.outcome_id, level.side, level.price)
            if level.liquidity > 0:
                levels[key] = level
            else:
                levels.pop(key, None)
        self._levels[update.market_id] = levels
        book = MarketBook(
            market_id=update.market_id,
            event_id=update.event_id,
            event_group_id=update.event_group_id,
            category_id=update.category_id,
            sub_category_id=update.sub_category_id,
            prices=[levels[key] for key in sorted(levels)],
        )
        self.market_books[update.market_id] = book
        return book

    def remove_market(self, market_id: str) -> None:
        """
        Forget a market's book, e.g. once it has settled.

        :param market_id: The market.
        """
        self.market_books.pop(market_id, None)
        self._levels.pop(market_id, None)
