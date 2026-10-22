import json
import queue
import pytest
import time
import threading

from collections.abc import Callable, Iterator
from typing import Any

from conftest import schema_errors
from betdex.utils import new_reference
from betdex.exceptions import BetDexStreamAuthenticationError
from betdex import BetDexClient, BetDexConnection
from betdex.markets import Market
from betdex.streaming import (
    UPDATE_TYPES,
    BetDexStream,
    MarketSnapshot,
    MarketOrderSnapshot,
    StreamListener,
    SubscribeUpdate,
    UnsubscribeUpdate,
    parse,
)


PRICE, STAKE = 1000.0, 1.0
PREFIX = "sdk-test_"
WAIT = 30.0


class RecordingListener(StreamListener):
    """
    A ``StreamListener`` that also queues every decoded message, confirmations
    included, after checking it strictly against its model.
    """

    def __init__(self) -> None:
        super().__init__()
        self.messages: queue.Queue[Any] = queue.Queue()
        self.mismatches: list[str] = []

    def on_data(self, raw: str) -> bool | None:
        data = json.loads(raw)
        for item in data if isinstance(data, list) else [data]:
            model = UPDATE_TYPES.get(item.get("type"))
            if model is not None:
                self.mismatches += schema_errors(model, item)
            self.messages.put(parse(item))
        return super().on_data(raw)


def wait_for(source: queue.Queue[Any], match: Callable[[Any], bool], timeout: float = WAIT) -> Any:
    """
    Take items off ``source`` until one matches, failing after ``timeout`` seconds.
    """
    deadline = time.monotonic() + timeout
    while (left := deadline - time.monotonic()) > 0:
        try:
            item = source.get(timeout=left)
        except queue.Empty:
            break
        if match(item):
            return item
    pytest.fail(f"nothing matched within {timeout}s")


def eventually(condition: Callable[[], bool], timeout: float = WAIT) -> None:
    """
    Wait for ``condition`` to hold, failing after ``timeout`` seconds.
    """
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail(f"condition not met within {timeout}s")
        time.sleep(0.05)


@pytest.fixture
def listener() -> RecordingListener:
    """
    A listener recording every message.
    """
    return RecordingListener()


@pytest.fixture
def stream(client: BetDexClient, listener: RecordingListener) -> Iterator[BetDexStream]:
    """
    A stream reading on a background thread; stopped afterwards, and every
    message it received checked against its model.
    """
    stream = client.create_stream(listener)
    stream.connect()
    thread = threading.Thread(target=stream.start, daemon=True)
    thread.start()
    yield stream
    stream.stop()
    thread.join(5)
    assert not thread.is_alive()
    assert not listener.mismatches, "\n".join(listener.mismatches[:20])


def subscribed(subscription_type: str, subscription_id: str) -> Callable[[Any], bool]:
    """
    Matches the confirmation of one subscription id.
    """
    return lambda m: (isinstance(m, SubscribeUpdate)
                      and (m.subscription_type, m.subscription_id) == (subscription_type,
                                                                       subscription_id))


def test_bad_token_is_refused(client: BetDexClient):
    """
    The stream doesn't answer a bad access token, so connecting times out and raises.
    """
    conn = BetDexConnection(environment="sandbox")
    conn.access_token = "not-a-token"
    stream = BetDexStream(conn, StreamListener(), timeout=3)
    with pytest.raises(BetDexStreamAuthenticationError):
        stream.connect()
    assert not stream.is_connected


def test_subscribe_and_unsubscribe(stream: BetDexStream, listener: RecordingListener):
    """
    Each subscribed id is confirmed, and so is dropping it.
    """
    stream.subscribe_to_events(["1", "2"])
    wait_for(listener.messages, subscribed("EventUpdate", "1"))
    stream.unsubscribe("EventUpdate", ["1"])
    unsubscribed = wait_for(listener.messages, lambda m: isinstance(m, UnsubscribeUpdate))
    assert unsubscribed.subscription_id == "1"
    assert stream.subscriptions["EventUpdate"] == {"2"}


def test_catalogue_subscriptions(stream: BetDexStream, listener: RecordingListener):
    """
    The all-markets, market-status and all-events subscriptions are confirmed.

    Their updates only come when the catalogue changes, so none are waited for;
    any that arrive are still checked against their models.
    """
    stream.subscribe_to_markets()
    stream.subscribe_to_market_status()
    stream.subscribe_to_events()
    expected = {"MarketUpdate", "MarketStatusUpdate", "EventUpdate"}
    confirmed: set[str] = set()
    while confirmed != expected:
        ack = wait_for(listener.messages, lambda m: isinstance(m, SubscribeUpdate))
        confirmed.add(ack.subscription_type)


def test_order_lifecycle(
    client: BetDexClient,
    stream: BetDexStream,
    listener: RecordingListener,
    open_market: Market,
    wallet_id: str):
    """
    Placing and cancelling an order streams both order states, and the book
    gains then loses the order's level.
    """
    stream.subscribe_to_orders()
    stream.subscribe_to_wallets()
    stream.subscribe_to_market_prices([open_market.id])
    wait_for(listener.messages, subscribed("WalletUpdate", wallet_id))
    wait_for(listener.output_queue, lambda m: isinstance(m, MarketSnapshot))

    outcome_id = open_market.market_outcomes.ids[0]
    level = (outcome_id, "For", PRICE)

    def in_book() -> bool:
        book = listener.market_books.get(open_market.id)
        return book is not None and PRICE in [p.price for p in book.available_to_lay(outcome_id)]

    reference = new_reference(PREFIX)
    order = client.create_order(
        open_market.id, outcome_id, "For", PRICE, STAKE,
        keep_when_in_play=False, reference=reference).orders[0]
    try:
        placed = wait_for(listener.output_queue, lambda m: (
                isinstance(m, MarketOrderSnapshot) and order.id in m.orders)).orders[order.id]
        assert placed.reference == reference
        assert (placed.order_id, placed.wallet_id, placed.status, placed.stake_unmatched) == (
            order.id, wallet_id, "Unmatched", STAKE)
        assert (placed.stake_matched, placed.liability) == (0, STAKE)
        eventually(in_book)
    finally:
        client.cancel_orders(order_ids=[order.id])

    snapshot = wait_for(listener.output_queue, lambda m: (
            isinstance(m, MarketOrderSnapshot) and order.id in m.orders
            and m.orders[order.id].status == "Cancelled"))
    assert snapshot.orders[order.id].stake_voided == STAKE
    assert snapshot.market_id == open_market.id
    eventually(lambda: not in_book())


def test_order_subscription_forms(client: BetDexClient, open_market: Market, app_id: str):
    """
    Subscribing by ``"{app_id}:*"`` or by order id both stream that order's changes.
    """
    order = client.create_order(
        open_market.id, open_market.market_outcomes.ids[0], "For", PRICE, STAKE,
        keep_when_in_play=False, reference=new_reference(PREFIX)).orders[0]
    listeners = []
    try:
        for subscription_id in (f"{app_id}:*", order.id):
            listener = RecordingListener()
            stream = client.create_stream(listener)
            stream.subscribe_to_orders([subscription_id])
            stream.connect()
            threading.Thread(target=stream.start, daemon=True).start()
            listeners.append((stream, listener))
            wait_for(listener.messages, subscribed("OrderUpdate", subscription_id))
    finally:
        client.cancel_orders(order_ids=[order.id])
    try:
        for _, listener in listeners:
            wait_for(listener.output_queue, lambda m: (
                    isinstance(m, MarketOrderSnapshot) and order.id in m.orders
                    and m.orders[order.id].status == "Cancelled"))
    finally:
        for stream, _ in listeners:
            stream.stop()


def test_restart_resubscribes(client: BetDexClient, listener: RecordingListener):
    """
    After ``stop``, ``start`` reconnects and sends the remembered subscriptions again.
    """
    stream = client.create_stream(listener)
    stream.subscribe_to_events(["1"])
    for _ in range(2):
        thread = threading.Thread(target=stream.start, daemon=True)
        thread.start()
        wait_for(listener.messages, subscribed("EventUpdate", "1"))
        assert stream.is_running
        stream.stop()
        thread.join(5)
        assert not thread.is_alive() and not stream.is_connected


def test_stop_while_connecting(client: BetDexClient):
    """
    Stopping a stream that is still connecting ends ``start`` quietly.
    """
    stream = client.create_stream()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            stream.start()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    eventually(lambda: stream.is_running, timeout=5)
    stream.stop()
    thread.join(10)
    assert not thread.is_alive() and not stream.is_connected
    assert errors == []
