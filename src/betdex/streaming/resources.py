from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from betdex.resources import (
    BaseModel,
    InPlayStatus,
    MarketAction,
    MarketStatus,
    OrderStatus,
    Side,
)

PriceKey = tuple[str, str, float]
UpdateType = Literal["Snapshot", "Incremental"]
SubscriptionType = Literal[
    "EventUpdate",
    "MarketUpdate",
    "MarketStatusUpdate",
    "MarketPriceUpdate",
    "OrderUpdate",
    "WalletUpdate",
]


@dataclass(kw_only=True, slots=True)
class AuthenticationUpdate(BaseModel):
    """
    The stream accepted the access token.
    """
    type: Literal["AuthenticationUpdate"]
    connection_id: str


@dataclass(kw_only=True, slots=True)
class SubscribeNotAuthenticatedUpdate(BaseModel):
    """
    A subscription was sent before authenticating, and ignored.
    """
    type: Literal["SubscribeNotAuthenticatedUpdate"]
    connection_id: str


@dataclass(kw_only=True, slots=True)
class SubscribeUpdate(BaseModel):
    """
    The stream confirmed one subscription id; sent once per id.
    """
    type: Literal["SubscribeUpdate"]
    subscription_type: SubscriptionType
    subscription_id: str


@dataclass(kw_only=True, slots=True)
class UnsubscribeUpdate(BaseModel):
    """
    The stream confirmed dropping one subscription id; sent once per id.
    """
    type: Literal["UnsubscribeUpdate"]
    subscription_type: SubscriptionType
    subscription_id: str


@dataclass(kw_only=True, slots=True)
class ErrorMessage(BaseModel):
    """
    The stream refused a message, e.g. an unknown action or one that isn't JSON.
    """
    message: str
    connection_id: str
    request_id: str


@dataclass(kw_only=True, slots=True)
class PriceLevel(BaseModel):
    """
    Unmatched liquidity at one price on one side of an outcome.
    """
    side: Side
    outcome_id: str
    price: float
    liquidity: float
    change: float
    valid_at: datetime


@dataclass(kw_only=True, slots=True)
class MarketPriceUpdate(BaseModel):
    """
    A market's order book, whole (``Snapshot``) or the levels that moved (``Incremental``).
    A snapshot arrives on subscribing; incrementals follow as liquidity or prices change.
    """
    type: Literal["MarketPriceUpdate"]
    update_type: UpdateType
    market_id: str
    event_id: str
    event_group_id: str
    category_id: str
    sub_category_id: str
    prices: list[PriceLevel] = field(default_factory=list)


@dataclass(kw_only=True, slots=True)
class MarketStatusUpdate(BaseModel):
    """
    A market's status changed.
    """
    type: Literal["MarketStatusUpdate"]
    market_id: str
    event_id: str
    event_group_id: str
    category_id: str
    sub_category_id: str
    status: MarketStatus
    published: bool
    suspended: bool
    in_play_status: InPlayStatus

    @property
    def is_in_play(self) -> bool:
        """
        Whether the market is open and trading in play. ``in_play_status`` alone
        stays ``InPlay`` after the market locks or settles.
        """
        return self.status == "Open" and self.in_play_status == "InPlay"


@dataclass(kw_only=True, slots=True)
class MarketUpdateOutcome(BaseModel):
    """
    One outcome of a ``MarketUpdate``.
    """
    outcome_id: str
    title: str
    ordering: int
    winner: bool | None = None
    participant_id: str | None = None
    created_at: datetime
    modified_at: datetime


@dataclass(kw_only=True, slots=True)
class MarketUpdate(BaseModel):
    """
    A market was created or changed; carries the whole market.
    """
    type: Literal["MarketUpdate"]
    market_id: str
    app_id: str
    event_id: str
    event_group_id: str
    category_id: str
    sub_category_id: str
    name: str
    market_type_id: str
    market_value: str | None = None
    market_discriminator: str | None = None
    escrow_wallet_id: str
    currency_id: str
    market_outcomes: list[MarketUpdateOutcome] = field(default_factory=list)
    cross_matching_enabled: bool
    lock_at: datetime
    settled_at: datetime | None = None
    market_lock_action: MarketAction
    event_start_action: MarketAction
    status: MarketStatus
    published: bool
    suspended: bool
    in_play_status: InPlayStatus
    in_play_delay: int | None = None
    created_at: datetime
    modified_at: datetime

    @property
    def is_in_play(self) -> bool:
        """
        Whether the market is open and trading in play. ``in_play_status`` alone
        stays ``InPlay`` after the market locks or settles.
        """
        return self.status == "Open" and self.in_play_status == "InPlay"


@dataclass(kw_only=True, slots=True)
class EventUpdate(BaseModel):
    """
    An event was created or changed; carries the whole event.
    """
    type: Literal["EventUpdate"]
    event_id: str
    event_group_id: str
    event_group_name: str
    subcategory_id: str
    subcategory_name: str
    category_id: str
    category_name: str
    app_id: str
    name: str
    code: str
    active: bool
    expected_start_time: datetime
    created_at: datetime
    modified_at: datetime


@dataclass(kw_only=True, slots=True)
class OrderUpdateTrade(BaseModel):
    """
    One match of an ``OrderUpdate``.
    """
    trade_id: str
    side: Side
    price: float
    stake: float
    created_at: datetime


@dataclass(kw_only=True, slots=True)
class OrderUpdate(BaseModel):
    """
    An order changed; carries the whole order, with every trade so far.
    """
    type: Literal["OrderUpdate"]
    order_id: str
    event_id: str
    event_group_id: str
    category_id: str
    sub_category_id: str
    market_id: str
    wallet_id: str | None = None
    status: OrderStatus
    side: Side
    outcome_index_id: str
    price: float
    price_avg: float | None = None
    total_exposure: float
    stake: float
    stake_unmatched: float
    stake_voided: float
    reference: str | None = None
    created_at: datetime
    modified_at: datetime
    trades: list[OrderUpdateTrade] = field(default_factory=list)
    cancellation_reason: str | None = None

    @property
    def liability(self) -> float:
        """
        The most this order can lose if all of it matches: the stake for ``For``,
        ``stake * (price - 1)`` for ``Against`` (``stake`` is always the backer's stake).
        """
        return self.stake if self.side == "For" else self.stake * (self.price - 1)

    @property
    def stake_matched(self) -> float:
        """
        Stake matched so far: what is neither unmatched nor voided.
        """
        return self.stake - self.stake_unmatched - self.stake_voided


@dataclass(kw_only=True, slots=True)
class WalletUpdate(BaseModel):
    """
    A wallet's balance in one currency changed.
    """
    type: Literal["WalletUpdate"]
    wallet_id: str
    currency_id: str
    balance: float
    change: float


@dataclass(kw_only=True, slots=True)
class StreamConnected(BaseModel):
    """
    A ``StreamListener`` connected, or reconnected, and dropped every book it had
    built: what came before was from the last connection. Fresh snapshots follow
    as the subscriptions are confirmed. Not a stream message; queued by the listener.
    """
    connection_id: str


@dataclass(kw_only=True, slots=True)
class MarketBook(BaseModel):
    """
    A market's whole order book, as a ``StreamListener`` has built it from
    ``MarketPriceUpdate`` messages.

    :param prices: Every level with liquidity, ordered by outcome, side and price.
    """
    market_id: str
    event_id: str
    event_group_id: str
    category_id: str
    sub_category_id: str
    prices: list[PriceLevel] = field(default_factory=list)

    def available_to_back(self, outcome_id: str) -> list[PriceLevel]:
        """
        Prices ``outcome_id`` can be backed at now, best (highest) first: the
        resting ``Against`` levels.
        """
        levels = [p for p in self.prices
                  if p.outcome_id == outcome_id and p.side == "Against" and p.liquidity > 0]
        return sorted(levels, key=lambda p: p.price, reverse=True)

    def available_to_lay(self, outcome_id: str) -> list[PriceLevel]:
        """
        Prices ``outcome_id`` can be laid at now, best (lowest) first: the resting
        ``For`` levels.
        """
        levels = [p for p in self.prices
                  if p.outcome_id == outcome_id and p.side == "For" and p.liquidity > 0]
        return sorted(levels, key=lambda p: p.price)


Update = (
    AuthenticationUpdate | SubscribeNotAuthenticatedUpdate | SubscribeUpdate | UnsubscribeUpdate
    | MarketPriceUpdate | MarketStatusUpdate | MarketUpdate | EventUpdate | OrderUpdate
    | WalletUpdate
)


UPDATE_TYPES: dict[str, type[Update]] = {
    "AuthenticationUpdate": AuthenticationUpdate,
    "SubscribeNotAuthenticatedUpdate": SubscribeNotAuthenticatedUpdate,
    "SubscribeUpdate": SubscribeUpdate,
    "UnsubscribeUpdate": UnsubscribeUpdate,
    "MarketPriceUpdate": MarketPriceUpdate,
    "MarketStatusUpdate": MarketStatusUpdate,
    "MarketUpdate": MarketUpdate,
    "EventUpdate": EventUpdate,
    "OrderUpdate": OrderUpdate,
    "WalletUpdate": WalletUpdate,
}
