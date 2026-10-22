import websocket
import time
import json
import threading

from collections.abc import Sequence
from typing import Any

from betdex.connection import BetDexConnection
from betdex.exceptions import (
    BetDexError,
    BetDexInvalidRequestError,
    BetDexSocketError,
    BetDexStreamAuthenticationError,
)
from betdex.streaming.listener import BaseListener
from betdex.streaming.resources import SubscriptionType


ALL = "*"


class BetDexStream:
    """
    One connection to the BetDEX Stream API.

    Subscriptions are remembered, and sent on every connect; ``start`` connects
    if needed, then hands each frame to the listener until ``stop``.
    Nothing reconnects by itself: when the socket fails ``start`` raises
    ``BetDexSocketError``, and calling it again reconnects and resubscribes. The
    stream can't resume, so anything sent while disconnected is lost: each
    connect tells the listener (``on_connect``), so it can drop what it built
    from the last connection and wait for fresh snapshots.

    :param conn: Supplies the access token and the default app and wallet ids.
    :param listener: Receives every frame.
    :param url: Stream URL; defaults to ``conn.stream_url``.
    :param timeout: Seconds to wait to connect, or for the authentication reply.
    :param keepalive: Seconds without a frame before sending a ping; ``None``
        never pings. The gateway closes connections idle for 10 minutes.

    The API allows 10 connections and 500 active subscriptions per app, and 500
    subscriptions a minute. Each id counts as one subscription, so subscribe to
    ``"*"`` rather than to hundreds of markets one by one. The SDK doesn't
    enforce these limits.
    """

    def __init__(
        self,
        conn: BetDexConnection,
        listener: BaseListener,
        *,
        url: str | None = None,
        timeout: float = 30.0,
        keepalive: float | None = 60.0,
    ) -> None:
        """
        Create a new stream.
        """
        self.conn = conn
        self.listener = listener
        self.url = url or conn.stream_url
        self.timeout = timeout
        self.keepalive = keepalive
        self.subscriptions: dict[SubscriptionType, set[str]] = {}
        self._ws: websocket.WebSocket | None = None
        self._running = threading.Event()

    @property
    def is_connected(self) -> bool:
        """
        Whether the socket is open.
        """
        return self._ws is not None and self._ws.connected

    @property
    def is_running(self) -> bool:
        """
        Whether ``start`` is reading frames.
        """
        return self._running.is_set()

    def connect(self) -> None:
        """
        Open the socket, authenticate, tell the listener, and send the remembered
        subscriptions.

        :raises BetDexSocketError: If the socket can't be opened.
        :raises BetDexStreamAuthenticationError: If the stream doesn't accept the token.
        """
        if not self.conn.access_token:
            raise BetDexInvalidRequestError("no access token; call create_session first")
        self.close()
        try:
            self._ws = websocket.create_connection(self.url, timeout=self.timeout)
        except (websocket.WebSocketException, OSError) as exc:
            raise BetDexSocketError(f"could not connect to stream at {self.url}: {exc}") from exc
        connection_id = self.authenticate()
        self.listener.on_connect(connection_id)
        for subscription_type, ids in self.subscriptions.items():
            if ids:
                self._send("subscribe", subscription_type, sorted(ids))

    def authenticate(self) -> str:
        """
        Send the access token and wait for the stream to accept it.

        Called by ``connect``. The stream doesn't answer a bad token, so a
        missing reply within ``timeout`` counts as a refusal. Frames that
        arrive meanwhile go to the listener.

        :returns: The connection id the stream gave.
        :raises BetDexStreamAuthenticationError: If no ``AuthenticationUpdate`` arrives in time.
        """
        ws = self._socket()
        self._send_json({"action": "authenticate", "accessToken": self.conn.access_token})
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            try:
                ws.settimeout(max(deadline - time.monotonic(), 0.01))
                frame = ws.recv()
            except websocket.WebSocketTimeoutException:
                break
            except (websocket.WebSocketException, OSError) as exc:
                raise BetDexSocketError(f"stream failed while authenticating: {exc}") from exc
            raw = frame.decode() if isinstance(frame, bytes) else frame
            if not raw:
                raise BetDexSocketError("stream connection closed while authenticating")
            try:
                data = json.loads(raw)
            except ValueError:
                data = None
            if isinstance(data, dict) and data.get("type") == "AuthenticationUpdate":
                ws.settimeout(self.keepalive)
                return data.get("connectionId", "")
            self.listener.on_data(raw)
        self.close()
        raise BetDexStreamAuthenticationError(
            f"stream at {self.url} did not accept the access token within {self.timeout}s")

    def subscribe(self, subscription_type: SubscriptionType, ids: Sequence[str]) -> None:
        """
        Subscribe to updates of a type, sending now if connected.

        Each id is confirmed by a ``SubscribeUpdate``, and its updates start only
        after that; ids that match nothing are confirmed too. Each id counts
        towards the limit of 500 active subscriptions.

        :param subscription_type: What to receive.
        :param ids: Ids to receive it for, or ``["*"]`` for all.
        """
        self.subscriptions.setdefault(subscription_type, set()).update(ids)
        if self.is_connected:
            self._send("subscribe", subscription_type, list(ids))

    def unsubscribe(self, subscription_type: SubscriptionType, ids: Sequence[str]) -> None:
        """
        Stop updates of a type, sending now if connected.

        :param subscription_type: What to stop.
        :param ids: Ids subscribed to earlier; ``"*"`` drops only a ``"*"`` subscription.
        """
        self.subscriptions.get(subscription_type, set()).difference_update(ids)
        if self.is_connected:
            self._send("unsubscribe", subscription_type, list(ids))

    def subscribe_to_market_prices(self, market_ids: Sequence[str] = (ALL,)) -> None:
        """
        Subscribe to order books (``MarketPriceUpdate``): a snapshot per market, then changes.

        :param market_ids: Markets; ``"*"`` for all, which sends a snapshot of every
            open market first.
        """
        self.subscribe("MarketPriceUpdate", market_ids)

    def subscribe_to_market_status(self, market_ids: Sequence[str] = (ALL,)) -> None:
        """
        Subscribe to market status changes (``MarketStatusUpdate``).

        :param market_ids: Markets; ``"*"`` for all.
        """
        self.subscribe("MarketStatusUpdate", market_ids)

    def subscribe_to_markets(self, market_ids: Sequence[str] = (ALL,)) -> None:
        """
        Subscribe to market creations and changes (``MarketUpdate``).

        :param market_ids: Markets; ``"*"`` for all.
        """
        self.subscribe("MarketUpdate", market_ids)

    def subscribe_to_events(self, event_ids: Sequence[str] = (ALL,)) -> None:
        """
        Subscribe to event creations and changes (``EventUpdate``).

        :param event_ids: Events; ``"*"`` for all.
        """
        self.subscribe("EventUpdate", event_ids)

    def subscribe_to_orders(self, ids: Sequence[str] | None = None) -> None:
        """
        Subscribe to order changes (``OrderUpdate``).

        :param ids: Any of:

            - ``"{app_id}:{wallet_id}"``: the app's orders in one of its wallets
            - ``"{app_id}:*"``: all of the app's orders
            - an order id: that order
            - ``"*"``: every order on the exchange (other apps' without wallet ids)

            Only the connection's own app id is accepted. Defaults to the
            connection's app and wallet.
        """
        if ids is None:
            ids = [f"{self.conn.resolve_app_id(None)}:{self.conn.resolve_wallet_id(None)}"]
        self.subscribe("OrderUpdate", ids)

    def subscribe_to_wallets(self, wallet_ids: Sequence[str] | None = None) -> None:
        """
        Subscribe to balance changes (``WalletUpdate``).

        :param wallet_ids: Wallets owned by the app, or ``["*"]`` for all of them;
            defaults to the connection's wallet.
        """
        self.subscribe("WalletUpdate", wallet_ids or [self.conn.resolve_wallet_id(None)])

    def start(self) -> None:
        """
        Read frames and pass them to the listener until ``stop``, or until the
        listener returns ``False``. Blocks; run it in a thread to do other work.

        Connects first if not connected.

        :raises BetDexSocketError: If the socket fails or the server closes it.
        :raises BetDexStreamAuthenticationError: If (re)connecting is refused.
        """
        self._running.set()
        try:
            if not self.is_connected:
                self.connect()
            ws = self._socket()
            while self._running.is_set():
                try:
                    frame = ws.recv()
                except websocket.WebSocketTimeoutException:
                    try:
                        ws.ping()
                    except (websocket.WebSocketException, OSError) as exc:
                        raise BetDexSocketError(
                            f"stream connection to {self.url} lost: {exc}") from exc
                    continue
                except (websocket.WebSocketException, OSError) as exc:
                    if not self._running.is_set():
                        break
                    raise BetDexSocketError(
                        f"stream connection to {self.url} lost: {exc}") from exc
                raw = frame.decode() if isinstance(frame, bytes) else frame
                if not raw:
                    if not self._running.is_set():
                        break
                    raise BetDexSocketError(
                        f"stream connection to {self.url} lost: closed by the server")
                if self.listener.on_data(raw) is False:
                    break
        except BetDexError:
            # Raised because ``stop`` closed the socket mid-connect: not a failure.
            if self._running.is_set():
                raise
        finally:
            self._running.clear()
            self.close()

    def stop(self) -> None:
        """
        Stop ``start`` and close the socket; safe from any thread.
        """
        self._running.clear()
        self.close()

    def close(self) -> None:
        """
        Close the socket, if open.
        """
        ws, self._ws = self._ws, None
        if ws is not None:
            ws.close()

    def _send(self, action: str, subscription_type: str, ids: list[str]) -> None:
        """
        Send a subscribe or unsubscribe message.
        """
        self._send_json({
            "action": action,
            "subscriptionType": subscription_type,
            "subscriptionIds": ids,
        })

    def _send_json(self, message: dict[str, Any]) -> None:
        """
        Send one JSON message.

        :raises BetDexSocketError: If not connected or the send fails.
        """
        try:
            self._socket().send(json.dumps(message))
        except (websocket.WebSocketException, OSError) as exc:
            raise BetDexSocketError(f"stream send failed: {exc}") from exc

    def _socket(self) -> websocket.WebSocket:
        """
        The open socket.

        :raises BetDexSocketError: If not connected.
        """
        if self._ws is None:
            raise BetDexSocketError("stream is not connected")
        return self._ws
