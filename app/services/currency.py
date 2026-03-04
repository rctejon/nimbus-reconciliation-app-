from app.utils.exchange_rates import EXCHANGE_RATES


def convert(amount: float, from_currency: str, to_currency: str) -> float:
    """Convert amount between two currencies using static exchange rates."""
    from_rate = EXCHANGE_RATES.get(from_currency.upper())
    to_rate = EXCHANGE_RATES.get(to_currency.upper())
    if from_rate is None:
        raise ValueError(f"Unsupported currency: {from_currency}")
    if to_rate is None:
        raise ValueError(f"Unsupported currency: {to_currency}")
    usd_amount = amount / from_rate
    return round(usd_amount * to_rate, 2)


def to_usd(amount: float, currency: str) -> float:
    """Convert an amount to USD."""
    return convert(amount, currency, "USD")
