from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from flask import Flask, flash, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///finance.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = "dev-change-me"  # replace for production

db = SQLAlchemy(app)


class Account(db.Model):
    __tablename__ = "accounts"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False, unique=True)
    account_type = db.Column(db.String(50), nullable=False, default="bank")  # bank, depot, broker, etc.

    monthly_payment_enabled = db.Column(db.Boolean, nullable=False, default=False)
    monthly_payment_amount = db.Column(db.Numeric(18, 2), nullable=False, default=0)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    balances = db.relationship(
        "BalancePoint",
        backref="account",
        cascade="all, delete-orphan",
        lazy=True,
        order_by="desc(BalancePoint.as_of_date), desc(BalancePoint.created_at)",
    )

    @property
    def current_balance(self) -> Decimal:
        if not self.balances:
            return Decimal("0.00")
        return Decimal(self.balances[0].balance)

    @property
    def monthly_payment(self) -> Decimal:
        if not self.monthly_payment_enabled:
            return Decimal("0.00")
        return Decimal(self.monthly_payment_amount)


class BalancePoint(db.Model):
    __tablename__ = "balance_points"

    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=False)

    as_of_date = db.Column(db.Date, nullable=False, default=date.today)
    balance = db.Column(db.Numeric(18, 2), nullable=False)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("account_id", "as_of_date", name="uq_account_date"),
    )


def parse_decimal(value: str) -> Decimal:
    """
    Accepts formats like:
      1234.56
      1,234.56
      1234,56  (common EU decimal)
      1.234,56
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
        raise ValueError("invalid number") from e


def parse_date(value: str) -> date:
    if not value:
        return date.today()
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError("invalid date") from e


def iso(d: date) -> str:
    return d.isoformat()


def build_account_series(account: Account):
    """
    Returns (labels, values) for a single account, ordered by date ascending.
    """
    points = (
        BalancePoint.query
        .filter_by(account_id=account.id)
        .order_by(BalancePoint.as_of_date.asc())
        .all()
    )
    labels = [iso(p.as_of_date) for p in points]
    values = [float(p.balance) for p in points]
    return labels, values


def build_stacked_series(accounts: list[Account]):
    """
    Returns:
      labels: sorted list of dates (ISO strings)
      datasets: list of {label, data} for each account with carry-forward values

    We build a unified date axis across all accounts and carry forward last known value.
    """
    all_points = (
        db.session.query(BalancePoint.account_id, BalancePoint.as_of_date, BalancePoint.balance)
        .order_by(BalancePoint.as_of_date.asc())
        .all()
    )

    # all_points rows are tuples: (account_id, as_of_date, balance)
    all_dates = sorted({row[1] for row in all_points})
    labels = [iso(d) for d in all_dates]

    per_account = defaultdict(dict)
    for account_id, d, bal in all_points:
        per_account[account_id][d] = float(bal)

    datasets = []
    for acc in accounts:
        series = []
        last_value = 0.0
        date_to_balance = per_account.get(acc.id, {})

        for d in all_dates:
            if d in date_to_balance:
                last_value = date_to_balance[d]
            series.append(last_value)

        datasets.append({
            "label": acc.name,
            "data": series
        })

    return labels, datasets


@app.get("/")
def dashboard():
    accounts = Account.query.order_by(Account.name.asc()).all()
    total_balance = sum((a.current_balance for a in accounts), Decimal("0.00"))
    total_monthly = sum((a.monthly_payment for a in accounts), Decimal("0.00"))

    last_update = db.session.query(func.max(BalancePoint.as_of_date)).scalar()

    chart_labels, chart_datasets = build_stacked_series(accounts)

    return render_template(
        "dashboard.html",
        accounts=accounts,
        total_balance=total_balance,
        total_monthly=total_monthly,
        last_update=last_update,
        chart_labels=chart_labels,
        chart_datasets=chart_datasets,
    )


@app.get("/accounts")
def accounts_list():
    accounts = Account.query.order_by(Account.name.asc()).all()
    return render_template("accounts.html", accounts=accounts)


@app.post("/accounts/create")
def accounts_create():
    name = (request.form.get("name") or "").strip()
    account_type = (request.form.get("account_type") or "bank").strip()

    monthly_enabled = request.form.get("monthly_enabled") == "on"
    monthly_amount_raw = request.form.get("monthly_amount") or "0"

    if not name:
        flash("Account name is required.", "danger")
        return redirect(url_for("accounts_list"))

    try:
        monthly_amount = parse_decimal(monthly_amount_raw)
    except ValueError:
        flash("Monthly payment amount is not a valid number.", "danger")
        return redirect(url_for("accounts_list"))

    if not monthly_enabled:
        monthly_amount = Decimal("0.00")

    existing = Account.query.filter_by(name=name).first()
    if existing:
        flash("An account with that name already exists.", "warning")
        return redirect(url_for("accounts_list"))

    acc = Account(
        name=name,
        account_type=account_type,
        monthly_payment_enabled=monthly_enabled,
        monthly_payment_amount=monthly_amount,
    )
    db.session.add(acc)
    db.session.commit()

    flash("Account created.", "success")
    return redirect(url_for("accounts_list"))


@app.get("/accounts/<int:account_id>")
def account_detail(account_id: int):
    acc = Account.query.get_or_404(account_id)
    labels, values = build_account_series(acc)

    return render_template(
        "account_detail.html",
        account=acc,
        chart_labels=labels,
        chart_values=values,
        today=date.today().isoformat(),  # default date value for new balance entry
    )


@app.post("/accounts/<int:account_id>/settings")
def account_settings(account_id: int):
    acc = Account.query.get_or_404(account_id)

    name = (request.form.get("name") or "").strip()
    account_type = (request.form.get("account_type") or "bank").strip()

    monthly_enabled = request.form.get("monthly_enabled") == "on"
    monthly_amount_raw = request.form.get("monthly_amount") or "0"

    if not name:
        flash("Account name is required.", "danger")
        return redirect(url_for("account_detail", account_id=account_id))

    existing = Account.query.filter(Account.name == name, Account.id != acc.id).first()
    if existing:
        flash("Another account already uses that name.", "warning")
        return redirect(url_for("account_detail", account_id=account_id))

    try:
        monthly_amount = parse_decimal(monthly_amount_raw)
    except ValueError:
        flash("Monthly payment amount is not a valid number.", "danger")
        return redirect(url_for("account_detail", account_id=account_id))

    if not monthly_enabled:
        monthly_amount = Decimal("0.00")

    acc.name = name
    acc.account_type = account_type
    acc.monthly_payment_enabled = monthly_enabled
    acc.monthly_payment_amount = monthly_amount

    db.session.commit()
    flash("Settings updated.", "success")
    return redirect(url_for("account_detail", account_id=account_id))


@app.post("/accounts/<int:account_id>/delete")
def account_delete(account_id: int):
    acc = Account.query.get_or_404(account_id)
    db.session.delete(acc)
    db.session.commit()
    flash("Account deleted.", "info")
    return redirect(url_for("accounts_list"))


@app.post("/accounts/<int:account_id>/balances/add")
def balance_add(account_id: int):
    acc = Account.query.get_or_404(account_id)

    as_of_date_raw = request.form.get("as_of_date") or ""
    balance_raw = request.form.get("balance") or ""

    try:
        d = parse_date(as_of_date_raw)
    except ValueError:
        flash("Date must be in YYYY-MM-DD format.", "danger")
        return redirect(url_for("account_detail", account_id=account_id))

    try:
        bal = parse_decimal(balance_raw)
    except ValueError:
        flash("Balance is not a valid number.", "danger")
        return redirect(url_for("account_detail", account_id=account_id))

    point = BalancePoint.query.filter_by(account_id=acc.id, as_of_date=d).first()
    if point:
        point.balance = bal
        flash("Balance updated for that date.", "success")
    else:
        point = BalancePoint(account_id=acc.id, as_of_date=d, balance=bal)
        db.session.add(point)
        flash("Balance point added.", "success")

    db.session.commit()
    return redirect(url_for("account_detail", account_id=account_id))


@app.post("/balances/<int:point_id>/delete")
def balance_delete(point_id: int):
    point = BalancePoint.query.get_or_404(point_id)
    account_id = point.account_id
    db.session.delete(point)
    db.session.commit()
    flash("Balance point deleted.", "info")
    return redirect(url_for("account_detail", account_id=account_id))


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(host="0.0.0.0", port=5000, debug=True)