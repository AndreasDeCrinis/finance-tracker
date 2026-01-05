from __future__ import annotations

import csv
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from app import app, db, Account, BalancePoint  # imports your Flask app + models


def parse_date_ddmmyyyy(value: str):
    value = (value or "").strip()
    if not value:
        raise ValueError("Empty date")
    return datetime.strptime(value, "%d.%m.%Y").date()


def parse_decimal(value: str) -> Decimal:
    """
    Accepts:
      4527.32
      4,527.32
      4527,32
      4.527,32
    """
    if value is None:
        raise ValueError("missing")
    s = value.strip()
    if not s:
        raise ValueError("missing")

    # If both separators exist, assume last one is decimal separator.
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            # 1.234,56 -> remove thousands dots, use comma as decimal
            s = s.replace(".", "").replace(",", ".")
        else:
            # 1,234.56 -> remove thousands commas
            s = s.replace(",", "")
    else:
        # Only comma present -> treat as decimal separator
        if "," in s and "." not in s:
            s = s.replace(",", ".")

    try:
        return Decimal(s).quantize(Decimal("0.01"))
    except InvalidOperation as e:
        raise ValueError(f"invalid number: {value}") from e


def detect_dialect(sample: str):
    """
    Auto-detect delimiter (tab vs comma). Works for your example.
    """
    sniffer = csv.Sniffer()
    try:
        return sniffer.sniff(sample, delimiters=[",", "\t", ";"])
    except csv.Error:
        # Default to tab since your example looks tab-separated
        return csv.excel_tab


def import_csv(filepath: Path):
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    text = filepath.read_text(encoding="utf-8-sig", errors="replace")
    sample = text[:4096]
    dialect = detect_dialect(sample)

    rows_total = 0
    created_accounts = 0
    inserted_points = 0
    updated_points = 0
    skipped_rows = 0

    with app.app_context():
        db.create_all()

        reader = csv.DictReader(text.splitlines(), dialect=dialect)
        # Normalize headers
        fieldnames = [f.strip().lower() for f in (reader.fieldnames or [])]
        if not {"date", "accountname", "balance"}.issubset(set(fieldnames)):
            raise ValueError(
                f"CSV must contain headers: date, accountname, balance. Found: {reader.fieldnames}"
            )

        # Remap keys to normalized names
        def norm_row(row):
            return {k.strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

        for raw in reader:
            rows_total += 1
            row = norm_row(raw)

            try:
                d = parse_date_ddmmyyyy(row["date"])
                name = row["accountname"].strip()
                bal = parse_decimal(row["balance"])
            except Exception as e:
                skipped_rows += 1
                print(f"[SKIP] Row {rows_total}: {e} | data={row}")
                continue

            if not name:
                skipped_rows += 1
                print(f"[SKIP] Row {rows_total}: empty accountname")
                continue

            # Ensure account exists
            acc = Account.query.filter_by(name=name).first()
            if not acc:
                acc = Account(name=name, account_type="other")
                db.session.add(acc)
                db.session.flush()  # assign ID
                created_accounts += 1

            # Upsert balance point (unique constraint account_id + as_of_date)
            point = BalancePoint.query.filter_by(account_id=acc.id, as_of_date=d).first()
            if point:
                # update
                point.balance = bal
                updated_points += 1
            else:
                point = BalancePoint(account_id=acc.id, as_of_date=d, balance=bal)
                db.session.add(point)
                inserted_points += 1

        db.session.commit()

    return {
        "rows_total": rows_total,
        "created_accounts": created_accounts,
        "inserted_points": inserted_points,
        "updated_points": updated_points,
        "skipped_rows": skipped_rows,
    }


def main():
    if len(sys.argv) != 2:
        print("Usage: python import_balances.py sunrise.csv")
        sys.exit(1)

    filepath = Path(sys.argv[1]).expanduser().resolve()

    stats = import_csv(filepath)
    print("\nImport finished:")
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
