"""Reviewed monitor notional formula with finite Decimal input validation."""
from decimal import Decimal, InvalidOperation


def positive_decimal(value):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("Invalid numeric input") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError("Input must be finite and positive")
    return number


def max_usd_volume(max_vol, price, contract_size):
    # Original _calc_max_usd_volume: maxVol * futures price * contractSize.
    return positive_decimal(max_vol) * positive_decimal(price) * positive_decimal(contract_size)


def price_difference_percent(reference, observed):
    reference = positive_decimal(reference)
    return (positive_decimal(observed) - reference) / reference * Decimal("100")
