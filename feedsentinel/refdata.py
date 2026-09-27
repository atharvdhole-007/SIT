"""Company reference data: the Nasdaq Symbol Directory (nasdaqlisted.txt).

Header: Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
The last line is a "File Creation Time: ..." footer.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from .config import REFERENCE_FILE

log = logging.getLogger("feedsentinel.refdata")

FINANCIAL_STATUS = {
    "N": "normal",
    "D": "deficient",
    "E": "delinquent",
    "Q": "bankrupt",
    "G": "deficient and bankrupt",
    "H": "deficient and delinquent",
    "J": "delinquent and bankrupt",
    "K": "deficient, delinquent and bankrupt",
}


@dataclass(frozen=True)
class Security:
    symbol: str
    name: str
    market_category: str
    test_issue: bool
    financial_status: str
    round_lot: int
    etf: bool


class SymbolDirectory:
    def __init__(self, securities: dict[str, Security], created: str = "", source: str = ""):
        self.securities = securities
        self.created = created
        self.source = source

    def __len__(self) -> int:
        return len(self.securities)

    def get(self, symbol: str) -> Security | None:
        return self.securities.get(symbol)

    def test_symbols(self) -> list[str]:
        return sorted(s for s, sec in self.securities.items() if sec.test_issue)

    def add(self, sec: Security) -> None:
        self.securities[sec.symbol] = sec

    @classmethod
    def load(cls, path: Path = REFERENCE_FILE) -> "SymbolDirectory":
        securities: dict[str, Security] = {}
        created = ""
        with open(path, encoding="utf-8", errors="replace") as fh:
            header = fh.readline().strip().split("|")
            if header[:4] != ["Symbol", "Security Name", "Market Category", "Test Issue"]:
                raise ValueError(f"{path} does not look like nasdaqlisted.txt (header {header[:4]})")
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("File Creation Time"):
                    created = line.split(":", 1)[1].strip().strip("|")
                    continue
                f = line.split("|")
                if len(f) < 8:
                    continue
                try:
                    lot = int(f[5])
                except ValueError:
                    lot = 100
                securities[f[0]] = Security(
                    symbol=f[0], name=f[1], market_category=f[2], test_issue=f[3] == "Y",
                    financial_status=f[4], round_lot=lot, etf=f[6] == "Y")
        log.info("loaded %d securities from %s (created %s)", len(securities), path.name, created)
        return cls(securities, created, str(path))

    @classmethod
    def for_symbols(cls, symbols, name_prefix: str = "") -> "SymbolDirectory":
        """A minimal directory for live venues (crypto pairs are not in the Nasdaq directory)."""
        secs = {s: Security(s, f"{name_prefix}{s}", "", False, "N", 1, False) for s in symbols}
        return cls(secs, "", "live venue product list")
