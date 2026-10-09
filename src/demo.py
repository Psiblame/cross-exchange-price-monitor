import json
from calculations import max_usd_volume, price_difference_percent

print(json.dumps({"fictional": True, "max_notional_usd": str(max_usd_volume("2000", "100", "0.01")), "price_difference_percent": str(price_difference_percent("100", "105"))}, indent=2))
