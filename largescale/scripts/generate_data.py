#!/usr/bin/env python3
"""
Dummy data generator for the large-scale database.

Dependencies: pip install pymysql
Connection : reads DB_URL, DB_USER_ID, DB_PASSWORD env vars.

Phase 1 — User:        500,000 rows
Phase 2 — Account:   1,500,000 rows
Phase 3 — Transaction: 20,000,000 rows
"""

import os
import sys
import random
import time
from datetime import datetime, timedelta

import pymysql

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BATCH_SIZE = 10_000          # rows per INSERT statement
COMMIT_EVERY = 50            # commit every N batches (= 500K rows)

USER_COUNT = 500_000
ACCOUNT_COUNT = 1_500_000
TRANSACTION_COUNT = 20_000_000

OUR_SWIFT_CODE = "VSFEKRSEXXX"
EXTERNAL_SWIFT_CODES = [
    "BNPAKRSEXXX", "CITIKRSEXXX", "HABORKSEXXX",
    "KOABORKSXXX", "SHBKKRSEXXX", "NACFKRSEXXX",
    "HVBKKRSEXXX", "SCBLKRSEXXX", "BOKRKRSEXXX",
    "JEONKRSEXXX",
]

FIRST_NAMES = [
    "Kim", "Lee", "Park", "Choi", "Jung",
    "Kang", "Cho", "Yoon", "Jang", "Lim",
    "Han", "Oh", "Seo", "Shin", "Kwon",
    "Hwang", "Ahn", "Song", "Yoo", "Hong",
]
LAST_NAMES = [
    "Minho", "Jihye", "Sungho", "Yuna", "Donghyun",
    "Jieun", "Taewoo", "Soojin", "Hyunwoo", "Minji",
    "Junseok", "Hayoung", "Woojin", "Nayeon", "Seungmin",
    "Eunji", "Jaehyuk", "Bora", "Kyungho", "Dahee",
]

MEMO_POOL = [
    "payment", "refund", "salary", "transfer",
    "deposit", "withdrawal", "fee", "interest",
    "bonus", "rent", "utilities", "subscription",
    "groceries", "dining", "travel", "insurance",
    "loan", "investment", "gift", "charity",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_now = datetime.now()
_two_years_ago = _now - timedelta(days=730)
_range_seconds = int((_now - _two_years_ago).total_seconds())


def random_datetime() -> datetime:
    """Random datetime within the past 2 years."""
    return _two_years_ago + timedelta(seconds=random.randint(0, _range_seconds))


def format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def get_validate_sum(s: str) -> int:
    """Matches Account.getValidateSum in Java: Σ (i+1)*digit[i]."""
    return sum((i + 1) * int(ch) for i, ch in enumerate(s))


def generate_account_number(*, valid: bool = True) -> str:
    """
    Format: 3333-NNNN-NNNNC
    C = checksum digit = (getValidateSum(part2) + getValidateSum(part3_prefix)) % 10
    """
    part2 = f"{random.randint(0, 9999):04d}"
    part3_prefix = f"{random.randint(0, 9999):04d}"

    checksum = (get_validate_sum(part2) + get_validate_sum(part3_prefix)) % 10

    if not valid:
        # Shift checksum to make it intentionally wrong
        checksum = (checksum + random.randint(1, 9)) % 10

    return f"3333-{part2}-{part3_prefix}{checksum}"


def random_external_account() -> str:
    """Random external account number (non-3333 prefix)."""
    prefix = random.choice(["1111", "2222", "4444", "5555", "6666", "7777", "8888", "9999"])
    return f"{prefix}-{random.randint(0,9999):04d}-{random.randint(0,99999):05d}"


def random_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def get_connection():
    db_url = os.environ.get("DB_URL")
    db_user = os.environ.get("DB_USER_ID")
    db_password = os.environ.get("DB_PASSWORD")

    if not all([db_url, db_user, db_password]):
        print("ERROR: Set DB_URL, DB_USER_ID, DB_PASSWORD environment variables.")
        sys.exit(1)

    # db_url may be host:port or just host
    host_port = db_url.split(":")
    host = host_port[0]
    port = int(host_port[1]) if len(host_port) > 1 else 3306

    return pymysql.connect(
        host=host,
        port=port,
        user=db_user,
        password=db_password,
        database="large-scale",
        charset="utf8mb4",
        autocommit=False,
    )


def execute_batch(cursor, sql_prefix, rows, conn, batch_counter):
    """Execute a single batch INSERT and optionally commit."""
    if not rows:
        return batch_counter
    values = ",".join(rows)
    cursor.execute(sql_prefix + values)
    batch_counter += 1
    if batch_counter % COMMIT_EVERY == 0:
        conn.commit()
    return batch_counter


# ---------------------------------------------------------------------------
# Phase 1: Users
# ---------------------------------------------------------------------------
def generate_users(conn):
    print(f"\n{'='*60}")
    print(f"Phase 1: Generating {USER_COUNT:,} users ...")
    print(f"{'='*60}")
    start = time.time()

    cursor = conn.cursor()
    sql_prefix = (
        "INSERT INTO `user` "
        "(user_id, username, email, nickname, group_id, user_status, create_date, update_date) VALUES "
    )

    rows = []
    batch_counter = 0
    for uid in range(1, USER_COUNT + 1):
        group_id = uid % 2
        status = "D" if random.random() < 0.05 else "A"
        create_date = random_datetime()
        update_date = create_date + timedelta(days=random.randint(0, 60))
        if update_date > _now:
            update_date = _now

        rows.append(
            f"({uid},'user_{uid}','user_{uid}@test.com','u{uid}',"
            f"{group_id},'{status}','{format_dt(create_date)}','{format_dt(update_date)}')"
        )

        if len(rows) >= BATCH_SIZE:
            batch_counter = execute_batch(cursor, sql_prefix, rows, conn, batch_counter)
            print(f"  Users: {uid:>10,} / {USER_COUNT:,}  ({uid*100//USER_COUNT}%)")
            rows = []

    batch_counter = execute_batch(cursor, sql_prefix, rows, conn, batch_counter)
    conn.commit()

    elapsed = time.time() - start
    print(f"  Phase 1 done — {USER_COUNT:,} users in {elapsed:.1f}s")
    cursor.close()


# ---------------------------------------------------------------------------
# Phase 2: Accounts
# ---------------------------------------------------------------------------
def generate_accounts(conn) -> list[str]:
    """Returns list of all generated account_numbers for use in Phase 3."""
    print(f"\n{'='*60}")
    print(f"Phase 2: Generating {ACCOUNT_COUNT:,} accounts ...")
    print(f"{'='*60}")
    start = time.time()

    cursor = conn.cursor()
    sql_prefix = (
        "INSERT INTO `account` "
        "(account_number, user_id, account_type, memo, balance, create_date, recent_transaction_date) VALUES "
    )

    invalid_count = int(ACCOUNT_COUNT * 0.005)  # 0.5%
    invalid_set = set(random.sample(range(1, ACCOUNT_COUNT + 1), invalid_count))

    account_numbers: list[str] = []
    rows = []
    batch_counter = 0

    for i in range(1, ACCOUNT_COUNT + 1):
        valid = i not in invalid_set
        acct_num = generate_account_number(valid=valid)
        account_numbers.append(acct_num)

        user_id = (i % USER_COUNT) + 1  # distribute across users 1..500K
        acct_type = random.choice(["1", "2", "3"])
        balance = random.randint(0, 100_000_000)
        create_date = random_datetime()

        if random.random() < 0.20:
            recent_txn = "NULL"
        else:
            recent_txn = f"'{format_dt(random_datetime())}'"

        memo = "NULL" if random.random() < 0.3 else f"'{random.choice(MEMO_POOL)}'"

        rows.append(
            f"('{acct_num}',{user_id},'{acct_type}',{memo},"
            f"{balance},'{format_dt(create_date)}',{recent_txn})"
        )

        if len(rows) >= BATCH_SIZE:
            batch_counter = execute_batch(cursor, sql_prefix, rows, conn, batch_counter)
            print(f"  Accounts: {i:>10,} / {ACCOUNT_COUNT:,}  ({i*100//ACCOUNT_COUNT}%)")
            rows = []

    batch_counter = execute_batch(cursor, sql_prefix, rows, conn, batch_counter)
    conn.commit()

    elapsed = time.time() - start
    print(f"  Phase 2 done — {ACCOUNT_COUNT:,} accounts ({invalid_count:,} invalid checksums) in {elapsed:.1f}s")
    cursor.close()

    return account_numbers


# ---------------------------------------------------------------------------
# Phase 3: Transactions
# ---------------------------------------------------------------------------
def generate_transactions(conn, account_numbers: list[str]):
    print(f"\n{'='*60}")
    print(f"Phase 3: Generating {TRANSACTION_COUNT:,} transactions ...")
    print(f"{'='*60}")
    start = time.time()

    cursor = conn.cursor()

    # Disable indexes for faster bulk insert
    print("  Disabling keys on `transaction` table ...")
    cursor.execute("ALTER TABLE `transaction` DISABLE KEYS")
    conn.commit()

    sql_prefix = (
        "INSERT INTO `transaction` "
        "(sender_account, receiver_account, sender_swift_code, receiver_swift_code, "
        "sender_name, receiver_name, amount, memo, transaction_date) VALUES "
    )

    acct_len = len(account_numbers)
    rows = []
    batch_counter = 0

    for i in range(1, TRANSACTION_COUNT + 1):
        roll = random.random()

        if roll < 0.4:
            # 40%: sender internal, receiver external
            sender_acct = account_numbers[random.randint(0, acct_len - 1)]
            receiver_acct = random_external_account()
            sender_swift = OUR_SWIFT_CODE
            receiver_swift = random.choice(EXTERNAL_SWIFT_CODES)
        elif roll < 0.8:
            # 40%: receiver internal, sender external
            sender_acct = random_external_account()
            receiver_acct = account_numbers[random.randint(0, acct_len - 1)]
            sender_swift = random.choice(EXTERNAL_SWIFT_CODES)
            receiver_swift = OUR_SWIFT_CODE
        else:
            # 20%: both internal
            sender_acct = account_numbers[random.randint(0, acct_len - 1)]
            receiver_acct = account_numbers[random.randint(0, acct_len - 1)]
            sender_swift = OUR_SWIFT_CODE
            receiver_swift = OUR_SWIFT_CODE

        sender_name = random_name()
        receiver_name = random_name()
        amount = random.randint(1_000, 50_000_000)
        txn_date = random_datetime()

        if random.random() < 0.30:
            memo = "NULL"
        else:
            memo = f"'{random.choice(MEMO_POOL)}'"

        rows.append(
            f"('{sender_acct}','{receiver_acct}',"
            f"'{sender_swift}','{receiver_swift}',"
            f"'{sender_name}','{receiver_name}',"
            f"{amount},{memo},'{format_dt(txn_date)}')"
        )

        if len(rows) >= BATCH_SIZE:
            batch_counter = execute_batch(cursor, sql_prefix, rows, conn, batch_counter)
            print(f"  Transactions: {i:>14,} / {TRANSACTION_COUNT:,}  ({i*100//TRANSACTION_COUNT}%)")
            rows = []

    batch_counter = execute_batch(cursor, sql_prefix, rows, conn, batch_counter)
    conn.commit()

    # Re-enable indexes
    print("  Re-enabling keys on `transaction` table (this may take a while) ...")
    cursor.execute("ALTER TABLE `transaction` ENABLE KEYS")
    conn.commit()

    elapsed = time.time() - start
    print(f"  Phase 3 done — {TRANSACTION_COUNT:,} transactions in {elapsed:.1f}s")
    cursor.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    total_start = time.time()
    print("Connecting to database ...")
    conn = get_connection()
    print("Connected.\n")

    try:
        generate_users(conn)
        account_numbers = generate_accounts(conn)
        generate_transactions(conn, account_numbers)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    total_elapsed = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"All done in {total_elapsed:.1f}s")
    print(f"{'='*60}")
    print(f"  Users:        {USER_COUNT:>14,}")
    print(f"  Accounts:     {ACCOUNT_COUNT:>14,}")
    print(f"  Transactions: {TRANSACTION_COUNT:>14,}")


if __name__ == "__main__":
    main()
