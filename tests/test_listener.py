"""
The listener's books, offline: no stream needed.
"""

import json

from betdex.streaming import MarketBook, StreamConnected, StreamListener


def price_update(update_type, *levels):
    """
    A price update; each level is ``(side, price, liquidity)``, or with a ``validAt``
    after it, e.g. ``"00:00:01.5"``.
    """
    return json.dumps({
        "type": "MarketPriceUpdate", "updateType": update_type, "marketId": "m",
        "eventId": "e", "eventGroupId": "g", "categoryId": "c", "subCategoryId": "s",
        "prices": [{"side": side, "outcomeId": "o", "price": price, "liquidity": liquidity,
                    "change": liquidity, "validAt": f"2026-01-01T{valid_at}Z"}
                   for side, price, liquidity, valid_at in
                   (level if len(level) == 4 else (*level, "00:00:00") for level in levels)]})


def prices(listener):
    """
    The book's ``(price, liquidity)`` levels after the last update.
    """
    *_, book = (listener.output_queue.get_nowait() for _ in range(listener.output_queue.qsize()))
    return [(p.price, p.liquidity) for p in book.prices]


def test_a_connect_drops_every_book_and_says_so():
    listener = StreamListener()
    listener.on_data(price_update("Snapshot", ("For", 2.0, 10.0)))
    listener.on_data(price_update("Incremental", ("For", 2.1, 5.0)))
    book = listener.output_queue.get_nowait()
    book = listener.output_queue.get_nowait()
    assert isinstance(book, MarketBook) and [p.price for p in book.prices] == [2.0, 2.1]
    listener.on_connect("c2")
    assert listener.market_books == {}
    assert listener.output_queue.get_nowait() == StreamConnected(connection_id="c2")
    # The book is built again from the new connection's updates only.
    listener.on_data(price_update("Incremental", ("For", 2.2, 5.0)))
    assert [p.price for p in listener.output_queue.get_nowait().prices] == [2.2]


def test_a_stale_add_arriving_after_its_removal_is_ignored():
    # As seen on prod: the removal arrives first, then the older add it replaced.
    listener = StreamListener()
    listener.on_data(price_update("Snapshot"))
    listener.on_data(price_update("Incremental", ("For", 1.33, 0.0, "18:26:35.644")))
    listener.on_data(price_update("Incremental", ("For", 1.33, 134.0, "18:26:35.528")))
    assert prices(listener) == []


def test_a_stale_liquidity_change_is_ignored_and_a_newer_one_applied():
    listener = StreamListener()
    listener.on_data(price_update("Snapshot", ("For", 60.0, 290.0, "18:29:06")))
    listener.on_data(price_update("Incremental", ("For", 60.0, 159.96, "18:29:53")))
    listener.on_data(price_update("Incremental", ("For", 60.0, 294.96, "18:29:10")))
    assert prices(listener) == [(60.0, 159.96)]
    listener.on_data(price_update("Incremental", ("For", 60.0, 4.96, "18:29:56")))
    assert prices(listener) == [(60.0, 4.96)]


def test_updates_with_the_same_valid_at_apply_in_arrival_order():
    listener = StreamListener()
    listener.on_data(price_update("Snapshot", ("For", 2.0, 10.0)))
    listener.on_data(price_update("Incremental", ("For", 2.0, 0.0)))
    listener.on_data(price_update("Incremental", ("For", 2.0, 7.0)))
    assert prices(listener) == [(2.0, 7.0)]


def test_a_snapshot_replaces_the_book_whatever_was_removed_before_it():
    listener = StreamListener()
    listener.on_data(price_update("Incremental", ("For", 2.0, 0.0, "00:00:05")))
    listener.on_data(price_update("Snapshot", ("For", 2.0, 10.0, "00:00:01")))
    assert prices(listener) == [(2.0, 10.0)]


def test_forgetting_a_market_forgets_when_its_levels_changed():
    listener = StreamListener()
    listener.on_data(price_update("Incremental", ("For", 2.0, 0.0, "00:00:05")))
    listener.remove_market("m")
    listener.on_data(price_update("Incremental", ("For", 2.0, 3.0, "00:00:01")))
    assert prices(listener) == [(2.0, 3.0)]
