import sys
import unittest
from decimal import Decimal
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from calculations import max_usd_volume, price_difference_percent


class CalculationsTest(unittest.TestCase):
    def test_contract_units(self):
        self.assertEqual(max_usd_volume("2000", "100", "0.01"), Decimal("2000"))

    def test_decimal_precision(self):
        self.assertEqual(max_usd_volume("3", "0.1", "0.1"), Decimal("0.03"))

    def test_difference_direction(self):
        self.assertEqual(price_difference_percent("100", "105"), Decimal("5"))
        self.assertEqual(price_difference_percent("100", "95"), Decimal("-5"))

    def test_invalid_values(self):
        for value in ["NaN", "Infinity", "-1", "0", "bad", None]:
            with self.subTest(kind=type(value).__name__):
                with self.assertRaises(ValueError):
                    max_usd_volume(value, "100", "1")

    def test_zero_reference_rejected(self):
        with self.assertRaises(ValueError):
            price_difference_percent("0", "100")
