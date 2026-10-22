"""
The listener's books, offline: no stream needed.
"""

import json

from betdex.streaming import MarketBook, StreamConnected, StreamListener


def price_update(update_type, *levels):
    return json.dumps({
        "type": "MarketPriceUpdate", "updateType": update_type, "marketId": "m",
        "eventId": "e", "eventGroupId": "g", "categoryId": "c", "subCategoryId": "s",
        "prices": [{"side": side, "outcomeId": "o", "price": price, "liquidity": liquidity,
                    "change": liquidity, "validAt": "2026-01-01T00:00:00Z"}
                   for side, price, liquidity in levels]})


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
