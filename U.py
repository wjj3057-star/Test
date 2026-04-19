# ============================================================
# Discord 종합봇 + 마켓 UI 통합본 (서버별 분리 버전)
#
# 핵심:
# - 모든 잔액 / 구매 / 충전 / 상품 / 설정이 guild_id(서버) 기준으로 분리됨
# - 기본 마켓 제목은 "Market"
# - 기존 "MungChi Market" 저장값은 실행 시 자동으로 "Market"으로 변경
# - 서버 주인(owner)도 관리자 기능 사용 가능
#
# 기능:
# - 관리: /kick /ban /clear /timeout /untimeout
# - AI: /ask (Gemini 선택)
# - 티켓: 버튼 1회 생성형
# - 마켓 UI: 구매 / 제품 / 충전 / 정보
# - 충전 모달 제출 후 계좌 정보 표시
# - 관리자 충전 승인/거절
# - 상품 구매 후 관리자 지급 로그
#
# [PowerShell 필수 환경변수]
# $env:DISCORD_TOKEN="디스코드_봇_토큰"
#
# [선택]
# $env:GEMINI_API_KEY="제미니_API_키"
# $env:GEMINI_MODEL="gemini-2.5-flash"
# $env:DEFAULT_MARKET_TITLE="Market"
# $env:DEFAULT_TICKET_CATEGORY_NAME="Tickets"
# $env:DEFAULT_REVIEW_URL="https://..."
#
# Python 3.10 이상 권장
# ============================================================

import os
import asyncio
import json
import math
import re
import hashlib
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from urllib import request as urllib_request
from urllib.parse import urlencode, urlparse

import discord
from discord import app_commands
from google import genai
from google.genai import types


def read_default_review_url() -> str:
    raw = os.getenv("DEFAULT_REVIEW_URL", "").strip()
    if not raw:
        return ""

    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        print("경고: DEFAULT_REVIEW_URL 값이 올바른 http/https URL이 아니라서 무시합니다.")
        return ""

    if len(raw) > 500:
        print("경고: DEFAULT_REVIEW_URL 값이 너무 길어서 무시합니다.")
        return ""

    return raw


# ============================================================
# 환경변수
# ============================================================

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

DEFAULT_MARKET_TITLE = os.getenv("DEFAULT_MARKET_TITLE", "Market")
DEFAULT_TICKET_CATEGORY_NAME = os.getenv("DEFAULT_TICKET_CATEGORY_NAME", "Tickets")
DEFAULT_REVIEW_URL = read_default_review_url()

gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

TICKET_ISSUE_TYPES = [
    ("문의", "inquiry", discord.ButtonStyle.primary, 0),
    ("중개", "brokerage", discord.ButtonStyle.primary, 0),
    ("경매", "auction", discord.ButtonStyle.primary, 0),
    ("후원", "support", discord.ButtonStyle.primary, 0),
    ("유저 신고", "user_report", discord.ButtonStyle.danger, 1),
    ("구매하기", "purchase", discord.ButtonStyle.success, 1),
]
MAX_OPEN_TICKETS_PER_USER = 3

TICKET_ISSUE_ROLE_COLUMNS = {
    "inquiry": "ticket_inquiry_role_id",
    "brokerage": "ticket_brokerage_role_id",
    "auction": "ticket_auction_role_id",
    "support": "ticket_support_role_id",
    "user_report": "ticket_user_report_role_id",
    "purchase": "ticket_purchase_role_id",
}

TICKET_ISSUE_MESSAGE_COLUMNS = {
    "inquiry": "ticket_inquiry_message",
    "brokerage": "ticket_brokerage_message",
    "auction": "ticket_auction_message",
    "support": "ticket_support_message",
    "user_report": "ticket_user_report_message",
    "purchase": "ticket_purchase_message",
}

TICKET_ISSUE_CHOICES = [
    app_commands.Choice(name="문의", value="inquiry"),
    app_commands.Choice(name="중개", value="brokerage"),
    app_commands.Choice(name="경매", value="auction"),
    app_commands.Choice(name="후원", value="support"),
    app_commands.Choice(name="유저 신고", value="user_report"),
    app_commands.Choice(name="구매하기", value="purchase"),
]

COIN_WALLET_COLUMNS = {
    "LTC": "coin_ltc_wallet",
    "USDT": "coin_usdt_wallet",
    "Tron": "coin_tron_wallet",
    "Bitcoin": "coin_bitcoin_wallet",
}

COIN_CHOICES = [
    app_commands.Choice(name="LTC", value="LTC"),
    app_commands.Choice(name="USDT", value="USDT"),
    app_commands.Choice(name="Tron", value="Tron"),
    app_commands.Choice(name="Bitcoin", value="Bitcoin"),
]

COIN_MARKET_CODES = {
    "USDT": "KRW-USDT",
    "Tron": "KRW-TRX",
    "Bitcoin": "KRW-BTC",
}

BINANCE_COIN_SYMBOLS = {
    "LTC": "LTCUSDT",
}

BINANCE_REFERENCE_COINS = {
    "Bitcoin": "BTCUSDT",
}

ALL_COIN_SYMBOLS = list(COIN_MARKET_CODES.keys()) + list(BINANCE_COIN_SYMBOLS.keys())

COIN_DISPLAY_SYMBOLS = {
    "LTC": "LTC",
    "USDT": "USDT",
    "Tron": "TRX",
    "Bitcoin": "BTC",
}

WITHDRAW_FEE_PERCENT = 15
USD_KRW_RATE_CACHE_TTL = 300
COIN_PRICE_CACHE_TTL = 60
USD_KRW_RATE_API_URL = "https://api.frankfurter.dev/v2/rates?base=USD&quotes=KRW"
UPBIT_TICKER_API_URL = f"https://api.upbit.com/v1/ticker?{urlencode({'markets': ','.join(COIN_MARKET_CODES.values())})}"


# ============================================================
# DB
# ============================================================

DB_PATH = "bot_data_multi_guild.db"
SAFE_TEXT_RE = re.compile(r"[\x00-\x1f\x7f]")
COMPONENT_FIELD_RE = re.compile(r"^\*\*(?P<name>.+?)\*\*\n(?P<value>.+)$", re.S)
USER_ID_PATTERN = re.compile(r"^(?:<@!?(?P<mention>\d+)>|(?P<raw>\d+))$")

MARKET_ACCENT = discord.Color.blurple()
SUCCESS_ACCENT = discord.Color.green()
WARNING_ACCENT = discord.Color.orange()
DANGER_ACCENT = discord.Color.red()
INFO_ACCENT = discord.Color.teal()

_market_cache = {
    "usd_krw_rate": {"value": None, "fetched_at": 0.0},
    "coin_prices_krw": {"value": None, "fetched_at": 0.0},
    "kimchi_premium": {"value": None, "fetched_at": 0.0},
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column_exists(cur: sqlite3.Cursor, table_name: str, column_name: str, definition: str):
    cur.execute(f"PRAGMA table_info({table_name})")
    columns = {row[1] for row in cur.fetchall()}
    if column_name not in columns:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS guild_settings (
        guild_id INTEGER PRIMARY KEY,
        charge_log_channel_id INTEGER NOT NULL DEFAULT 0,
        purchase_log_channel_id INTEGER NOT NULL DEFAULT 0,
        bank_name TEXT NOT NULL DEFAULT '',
        bank_account_number TEXT NOT NULL DEFAULT '',
        bank_account_holder TEXT NOT NULL DEFAULT '',
        coin_ltc_wallet TEXT NOT NULL DEFAULT '',
        coin_usdt_wallet TEXT NOT NULL DEFAULT '',
        coin_tron_wallet TEXT NOT NULL DEFAULT '',
        coin_bitcoin_wallet TEXT NOT NULL DEFAULT '',
        ticket_staff_role_id INTEGER NOT NULL DEFAULT 0,
        ticket_inquiry_role_id INTEGER NOT NULL DEFAULT 0,
        ticket_brokerage_role_id INTEGER NOT NULL DEFAULT 0,
        ticket_auction_role_id INTEGER NOT NULL DEFAULT 0,
        ticket_support_role_id INTEGER NOT NULL DEFAULT 0,
        ticket_quick_buy_role_id INTEGER NOT NULL DEFAULT 0,
        ticket_user_report_role_id INTEGER NOT NULL DEFAULT 0,
        ticket_purchase_role_id INTEGER NOT NULL DEFAULT 0,
        ticket_category_name TEXT NOT NULL DEFAULT 'Tickets',
        market_title TEXT NOT NULL DEFAULT 'Market',
        review_url TEXT NOT NULL DEFAULT ''
    )
    """)

    for column_name in TICKET_ISSUE_ROLE_COLUMNS.values():
        ensure_column_exists(cur, "guild_settings", column_name, "INTEGER NOT NULL DEFAULT 0")
    for column_name in TICKET_ISSUE_MESSAGE_COLUMNS.values():
        ensure_column_exists(cur, "guild_settings", column_name, "TEXT NOT NULL DEFAULT ''")
    for column_name in COIN_WALLET_COLUMNS.values():
        ensure_column_exists(cur, "guild_settings", column_name, "TEXT NOT NULL DEFAULT ''")
    ensure_column_exists(cur, "guild_settings", "auction_channel_id", "INTEGER NOT NULL DEFAULT 0")

    cur.execute("""
        UPDATE guild_settings
        SET market_title = 'Market'
        WHERE market_title = 'MungChi Market'
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        guild_id INTEGER NOT NULL,
        discord_user_id INTEGER NOT NULL,
        balance INTEGER NOT NULL DEFAULT 0,
        total_spent INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (guild_id, discord_user_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS charge_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        discord_user_id INTEGER NOT NULL,
        amount INTEGER NOT NULL,
        credited_amount INTEGER NOT NULL DEFAULT 0,
        depositor_name TEXT NOT NULL,
        charge_type TEXT NOT NULL DEFAULT 'bank',
        coin_symbol TEXT NOT NULL DEFAULT '',
        usd_krw_rate REAL NOT NULL DEFAULT 0,
        coin_price_krw REAL NOT NULL DEFAULT 0,
        coin_quantity TEXT NOT NULL DEFAULT '',
        transaction_id TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL,
        processed_at TEXT,
        processed_by INTEGER
    )
    """)

    ensure_column_exists(cur, "charge_requests", "credited_amount", "INTEGER NOT NULL DEFAULT 0")
    ensure_column_exists(cur, "charge_requests", "charge_type", "TEXT NOT NULL DEFAULT 'bank'")
    ensure_column_exists(cur, "charge_requests", "coin_symbol", "TEXT NOT NULL DEFAULT ''")
    ensure_column_exists(cur, "charge_requests", "usd_krw_rate", "REAL NOT NULL DEFAULT 0")
    ensure_column_exists(cur, "charge_requests", "coin_price_krw", "REAL NOT NULL DEFAULT 0")
    ensure_column_exists(cur, "charge_requests", "coin_quantity", "TEXT NOT NULL DEFAULT ''")
    ensure_column_exists(cur, "charge_requests", "transaction_id", "TEXT NOT NULL DEFAULT ''")
    cur.execute("""
        UPDATE charge_requests
        SET credited_amount = amount
        WHERE credited_amount = 0
          AND charge_type != 'coin'
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS balance_transfers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        sender_user_id INTEGER NOT NULL,
        recipient_user_id INTEGER NOT NULL,
        amount INTEGER NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS balance_withdrawals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        discord_user_id INTEGER NOT NULL,
        bank_name TEXT NOT NULL,
        account_number TEXT NOT NULL,
        account_holder TEXT NOT NULL,
        request_amount INTEGER NOT NULL,
        fee_amount INTEGER NOT NULL,
        payout_amount INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'submitted',
        created_at TEXT NOT NULL
    )
    """)
    ensure_column_exists(cur, "balance_withdrawals", "processed_at", "TEXT NOT NULL DEFAULT ''")
    ensure_column_exists(cur, "balance_withdrawals", "processed_by", "INTEGER NOT NULL DEFAULT 0")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS items (
        guild_id INTEGER NOT NULL,
        item_key TEXT NOT NULL,
        category TEXT NOT NULL,
        item_name TEXT NOT NULL,
        price INTEGER NOT NULL,
        stock INTEGER NOT NULL DEFAULT 0,
        description TEXT,
        sales_count INTEGER NOT NULL DEFAULT 0,
        display_order INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (guild_id, item_key)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        discord_user_id INTEGER NOT NULL,
        item_key TEXT NOT NULL,
        item_name TEXT NOT NULL,
        price_paid INTEGER NOT NULL,
        original_price INTEGER NOT NULL,
        discount_percent INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending_delivery',
        created_at TEXT NOT NULL,
        processed_at TEXT,
        processed_by INTEGER
    )
    """)

    conn.commit()
    conn.close()


# ============================================================
# 공용 유틸
# ============================================================

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_guild(interaction: discord.Interaction):
    if interaction.guild is None:
        raise app_commands.AppCommandError("이 명령어는 서버 안에서만 사용할 수 있어요.")


def normalize_item_key(item_key: str) -> str:
    return item_key.strip().lower()


def is_manager(user: discord.abc.User | discord.Member, guild: discord.Guild | None = None) -> bool:
    if guild is None and isinstance(user, discord.Member):
        guild = user.guild

    if guild is None:
        return False

    if user.id == guild.owner_id:
        return True

    if isinstance(user, discord.Member):
        return (
            user.guild_permissions.administrator
            or user.guild_permissions.manage_guild
        )

    member = guild.get_member(user.id)
    if member is None:
        return False

    return (
        member.guild_permissions.administrator
        or member.guild_permissions.manage_guild
    )


async def safe_notify_user(client: discord.Client, user_id: int, text: str):
    user = client.get_user(user_id)
    if user is None:
        try:
            user = await client.fetch_user(user_id)
        except Exception:
            return

    try:
        await user.send(text)
    except Exception:
        pass


async def send_purchase_delivery_dm(user: discord.abc.User, data: dict):
    description = sanitize_plain_text(data.get("description"), max_length=3500, multiline=True)
    if not description:
        description = "전달할 상품 내용이 비어 있어요."

    view = build_component_view(
        build_component_container(
            "구매한 상품을 전달드려요",
            description=description,
            fields=[
                ("구매 ID", str(data["purchase_id"])),
                ("상품명", sanitize_plain_text(data["item_name"], max_length=80)),
                ("상품 키", sanitize_plain_text(data["item_key"], max_length=50)),
                ("결제 금액", f"{data['price_paid']}원"),
            ],
            footer="DM 수신을 꺼두면 전달이 실패할 수 있어요.",
            accent_color=SUCCESS_ACCENT,
        ),
        timeout=300,
    )

    await user.send(view=view, allowed_mentions=discord.AllowedMentions.none())


def truncate_component_text(value: str | None, *, max_length: int = 4000) -> str:
    text = (value or "").strip()
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip() + "..."


def build_component_container(
    title: str | None = None,
    description: str | None = None,
    *,
    fields: list[tuple[str, str]] | None = None,
    footer: str | None = None,
    accent_color: discord.Color | int | None = None,
) -> discord.ui.Container:
    children: list[discord.ui.Item] = []

    if title:
        children.append(discord.ui.TextDisplay(f"## {truncate_component_text(title, max_length=200)}"))

    if description:
        children.append(discord.ui.TextDisplay(truncate_component_text(description)))

    for name, value in fields or []:
        field_name = truncate_component_text(name, max_length=200)
        field_value = truncate_component_text(value or "-", max_length=3900)
        children.append(discord.ui.TextDisplay(f"**{field_name}**\n{field_value}"))

    if footer:
        if children:
            children.append(discord.ui.Separator())
        children.append(discord.ui.TextDisplay(truncate_component_text(footer)))

    return discord.ui.Container(*children, accent_color=accent_color)


def build_component_view(
    *items: discord.ui.Item,
    timeout: float | None = 300,
) -> discord.ui.LayoutView:
    view = discord.ui.LayoutView(timeout=timeout)
    for item in items:
        view.add_item(item)
    return view


async def send_component_view(
    channel: discord.abc.Messageable,
    view: discord.ui.LayoutView,
    *,
    mention_text: str | None = None,
    allowed_mentions: discord.AllowedMentions | None = None,
):
    if mention_text:
        await channel.send(mention_text, allowed_mentions=allowed_mentions)

    await channel.send(view=view)


def build_action_row(*items: discord.ui.Item) -> discord.ui.ActionRow:
    return discord.ui.ActionRow(*items)


def iter_component_text_displays(message: discord.Message | None) -> list[str]:
    if message is None:
        return []

    try:
        parsed_view = discord.ui.LayoutView.from_message(message, timeout=None)
    except Exception:
        return []

    if hasattr(parsed_view, "walk_children"):
        children = parsed_view.walk_children()
    else:
        children = parsed_view.children

    return [
        item.content
        for item in children
        if isinstance(item, discord.ui.TextDisplay)
    ]


def extract_component_field_value(message: discord.Message | None, field_name: str) -> str | None:
    for content in iter_component_text_displays(message):
        match = COMPONENT_FIELD_RE.match(content.strip())
        if match and match.group("name").strip() == field_name:
            return match.group("value").strip()
    return None


def extract_component_field_int(message: discord.Message | None, field_name: str) -> int | None:
    value = extract_component_field_value(message, field_name)
    if value is None:
        return None

    normalized = re.sub(r"[^\d-]", "", value)
    if not normalized:
        return None

    try:
        return int(normalized)
    except ValueError:
        return None


def sanitize_plain_text(
    value: str | None,
    *,
    max_length: int = 100,
    multiline: bool = False,
) -> str:
    text = (value or "").strip()
    if not multiline:
        text = text.replace("\r", " ").replace("\n", " ")
    else:
        text = text.replace("\r\n", "\n").replace("\r", "\n")

    text = SAFE_TEXT_RE.sub("", text)
    text = discord.utils.escape_mentions(text)
    return text[:max_length].strip()


def sanitize_item_key(item_key: str) -> str:
    normalized = normalize_item_key(item_key)
    return re.sub(r"[^a-z0-9_-]", "", normalized)[:50]


def generate_item_key(item_name: str) -> str:
    clean_name = sanitize_plain_text(item_name, max_length=80)
    slug = re.sub(r"[^a-z0-9_-]", "", clean_name.strip().lower())
    if slug:
        return slug[:50]

    digest = hashlib.sha1(clean_name.encode("utf-8")).hexdigest()[:12]
    return f"item-{digest}"


def ensure_unique_item_key(guild_id: int, item_name: str) -> str:
    clean_item_name = sanitize_plain_text(item_name, max_length=80)
    base_key = generate_item_key(clean_item_name)
    existing = get_item(guild_id, base_key)

    if existing is None:
        return base_key

    existing_name = sanitize_plain_text(existing["item_name"], max_length=80)
    if existing_name.casefold() == clean_item_name.casefold():
        return existing["item_key"]

    digest = hashlib.sha1(clean_item_name.encode("utf-8")).hexdigest()[:12]
    if len(base_key) > 37:
        base_key = base_key[:37]
    return f"{base_key}-{digest}"[:50]


def normalize_money_account(value: str, *, max_length: int = 40) -> str:
    raw = sanitize_plain_text(value, max_length=max_length)
    return re.sub(r"[^0-9\- ]", "", raw).strip()


def normalize_wallet_address(value: str, *, max_length: int = 120) -> str:
    return sanitize_plain_text(value, max_length=max_length)


def normalize_transaction_id(value: str, *, max_length: int = 150) -> str:
    return sanitize_plain_text(value, max_length=max_length)


def normalize_review_url(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""

    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise app_commands.AppCommandError("후기 링크는 http:// 또는 https:// 주소만 사용할 수 있어요.")

    if len(raw) > 500:
        raise app_commands.AppCommandError("후기 링크가 너무 길어요.")

    return raw


def format_won(amount: int) -> str:
    return f"{amount:,}원"


def format_usd(amount: int | float) -> str:
    numeric = float(amount)
    if numeric.is_integer():
        return f"${int(numeric):,}"
    return f"${numeric:,.2f}"


def format_decimal(value: float, *, max_decimals: int = 8) -> str:
    text = f"{value:,.{max_decimals}f}".rstrip("0").rstrip(".")
    return text if text else "0"


def calculate_withdrawal_fee(amount: int) -> int:
    return math.floor(amount * WITHDRAW_FEE_PERCENT / 100)


def parse_user_id_input(value: str) -> int | None:
    match = USER_ID_PATTERN.match((value or "").strip())
    if not match:
        return None

    raw_user_id = match.group("mention") or match.group("raw")
    if raw_user_id is None:
        return None

    try:
        return int(raw_user_id)
    except ValueError:
        return None


def get_coin_display_symbol(coin_symbol: str) -> str:
    return COIN_DISPLAY_SYMBOLS.get(coin_symbol, coin_symbol)


# ============================================================
# 서버 설정
# ============================================================

def ensure_guild_settings_row(guild_id: int):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT guild_id FROM guild_settings WHERE guild_id = ?", (guild_id,))
    row = cur.fetchone()

    if row is None:
        cur.execute("""
            INSERT INTO guild_settings (
                guild_id,
                charge_log_channel_id,
                purchase_log_channel_id,
                bank_name,
                bank_account_number,
                bank_account_holder,
                coin_ltc_wallet,
                coin_usdt_wallet,
                coin_tron_wallet,
                coin_bitcoin_wallet,
                ticket_staff_role_id,
                ticket_inquiry_role_id,
                ticket_brokerage_role_id,
                ticket_auction_role_id,
                ticket_support_role_id,
                ticket_quick_buy_role_id,
                ticket_user_report_role_id,
                ticket_purchase_role_id,
                ticket_category_name,
                market_title,
                review_url
            ) VALUES (?, 0, 0, '', '', '', '', '', '', '', 0, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?)
        """, (
            guild_id,
            DEFAULT_TICKET_CATEGORY_NAME,
            DEFAULT_MARKET_TITLE,
            DEFAULT_REVIEW_URL,
        ))
        conn.commit()

    conn.close()


def get_guild_settings(guild_id: int) -> dict:
    ensure_guild_settings_row(guild_id)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM guild_settings WHERE guild_id = ?", (guild_id,))
    row = cur.fetchone()

    conn.close()
    return dict(row)


def update_guild_settings(guild_id: int, **fields):
    ensure_guild_settings_row(guild_id)

    if not fields:
        return

    allowed = {
        "charge_log_channel_id",
        "purchase_log_channel_id",
        "bank_name",
        "bank_account_number",
        "bank_account_holder",
        "coin_ltc_wallet",
        "coin_usdt_wallet",
        "coin_tron_wallet",
        "coin_bitcoin_wallet",
        "ticket_staff_role_id",
        "ticket_inquiry_role_id",
        "ticket_brokerage_role_id",
        "ticket_auction_role_id",
        "ticket_support_role_id",
        "ticket_quick_buy_role_id",
        "ticket_user_report_role_id",
        "ticket_purchase_role_id",
        "ticket_category_name",
        "market_title",
        "review_url",
        "auction_channel_id",
        "ticket_inquiry_message",
        "ticket_brokerage_message",
        "ticket_auction_message",
        "ticket_support_message",
        "ticket_user_report_message",
        "ticket_purchase_message",
    }

    clean_fields = {k: v for k, v in fields.items() if k in allowed}
    if not clean_fields:
        return

    set_clause = ", ".join(f"{key} = ?" for key in clean_fields.keys())
    values = list(clean_fields.values()) + [guild_id]

    conn = get_db()
    cur = conn.cursor()

    cur.execute(f"""
        UPDATE guild_settings
        SET {set_clause}
        WHERE guild_id = ?
    """, values)

    conn.commit()
    conn.close()


def is_bank_configured(guild_id: int) -> bool:
    settings = get_guild_settings(guild_id)
    return bool(
        settings["bank_name"].strip()
        and settings["bank_account_number"].strip()
        and settings["bank_account_holder"].strip()
    )


def is_coin_configured(guild_id: int, coin_symbol: str) -> bool:
    settings = get_guild_settings(guild_id)
    column_name = COIN_WALLET_COLUMNS.get(coin_symbol)
    if not column_name:
        return False
    return bool((settings.get(column_name) or "").strip())


def get_coin_wallet_address(guild_id: int, coin_symbol: str) -> str:
    settings = get_guild_settings(guild_id)
    column_name = COIN_WALLET_COLUMNS.get(coin_symbol)
    if not column_name:
        return ""
    return (settings.get(column_name) or "").strip()


def list_configured_coins(guild_id: int) -> list[str]:
    settings = get_guild_settings(guild_id)
    coins: list[str] = []
    for coin_symbol, column_name in COIN_WALLET_COLUMNS.items():
        if (settings.get(column_name) or "").strip():
            coins.append(coin_symbol)
    return coins


def _fetch_json(url: str) -> dict | list:
    req = urllib_request.Request(
        url,
        headers={
            "User-Agent": "discord-bot/1.0",
            "Accept": "application/json",
        },
    )
    with urllib_request.urlopen(req, timeout=10) as response:
        return json.load(response)


def _update_market_cache(cache_key: str, value):
    _market_cache[cache_key]["value"] = value
    _market_cache[cache_key]["fetched_at"] = time.monotonic()


def _fetch_binance_price_usdt(binance_symbol: str) -> float | None:
    payload = _fetch_json(f"https://api.binance.com/api/v3/ticker/price?{urlencode({'symbol': binance_symbol})}")
    if not isinstance(payload, dict):
        return None
    price = payload.get("price")
    if price is None:
        return None
    return float(price)


def _get_kimchi_premium() -> float:
    cache = _market_cache["kimchi_premium"]
    if cache["value"] is not None and time.monotonic() - cache["fetched_at"] < COIN_PRICE_CACHE_TTL:
        return float(cache["value"])

    premiums: list[float] = []
    for coin_symbol, binance_symbol in BINANCE_REFERENCE_COINS.items():
        upbit_market = COIN_MARKET_CODES.get(coin_symbol)
        if not upbit_market:
            continue
        try:
            upbit_payload = _fetch_json(f"https://api.upbit.com/v1/ticker?{urlencode({'markets': upbit_market})}")
            if not isinstance(upbit_payload, list) or not upbit_payload:
                continue
            upbit_krw = float(upbit_payload[0].get("trade_price", 0))
            if upbit_krw <= 0:
                continue

            binance_usdt = _fetch_binance_price_usdt(binance_symbol)
            if binance_usdt is None or binance_usdt <= 0:
                continue

            usdt_payload = _fetch_json(f"https://api.upbit.com/v1/ticker?{urlencode({'markets': 'KRW-USDT'})}")
            if not isinstance(usdt_payload, list) or not usdt_payload:
                continue
            usdt_krw = float(usdt_payload[0].get("trade_price", 0))
            if usdt_krw <= 0:
                continue

            fair_krw = binance_usdt * usdt_krw
            premium = (upbit_krw - fair_krw) / fair_krw
            premiums.append(premium)
        except Exception as e:
            print(f"[kimchi-premium] {coin_symbol} calc failed: {e!r}")
            continue

    result = sum(premiums) / len(premiums) if premiums else 0.0
    _update_market_cache("kimchi_premium", result)
    return result


def _fetch_single_coin_price_krw(coin_symbol: str) -> float | None:
    # Binance 코인 (LTC 등): USDT 가격 조회 후 KRW-USDT 시세로 환산 + 김치프리미엄 반영
    binance_symbol = BINANCE_COIN_SYMBOLS.get(coin_symbol)
    if binance_symbol:
        usdt_price = _fetch_binance_price_usdt(binance_symbol)
        if usdt_price is None:
            return None
        usdt_krw = _fetch_single_coin_price_krw("USDT")
        if usdt_krw is None:
            return None
        try:
            premium = _get_kimchi_premium()
        except Exception:
            premium = 0.0
        return usdt_price * usdt_krw * (1 + premium)

    # Upbit 코인
    market_code = COIN_MARKET_CODES.get(coin_symbol)
    if not market_code:
        return None

    payload = _fetch_json(f"https://api.upbit.com/v1/ticker?{urlencode({'markets': market_code})}")
    if not isinstance(payload, list) or not payload:
        return None

    trade_price = payload[0].get("trade_price")
    if trade_price is None:
        return None

    return float(trade_price)


def _get_cached_usd_krw_rate() -> float:
    cache = _market_cache["usd_krw_rate"]
    if cache["value"] is not None and time.monotonic() - cache["fetched_at"] < USD_KRW_RATE_CACHE_TTL:
        return float(cache["value"])

    try:
        payload = _fetch_json(USD_KRW_RATE_API_URL)
        rate: float | None = None

        if isinstance(payload, list):
            for row in payload:
                if row.get("quote") == "KRW":
                    rate = float(row["rate"])
                    break
        elif isinstance(payload, dict):
            rates = payload.get("rates") or {}
            if "KRW" in rates:
                rate = float(rates["KRW"])

        if rate is None:
            raise RuntimeError("USD/KRW 환율 응답 형식이 올바르지 않아요.")

        _update_market_cache("usd_krw_rate", rate)
        return rate
    except Exception:
        if cache["value"] is not None:
            return float(cache["value"])
        raise


def _get_cached_coin_prices_krw() -> dict[str, float]:
    cache = _market_cache["coin_prices_krw"]
    if cache["value"] is not None and time.monotonic() - cache["fetched_at"] < COIN_PRICE_CACHE_TTL:
        return dict(cache["value"])

    try:
        prices: dict[str, float] = {}

        try:
            payload = _fetch_json(UPBIT_TICKER_API_URL)
            if isinstance(payload, list):
                market_to_symbol = {market_code: coin_symbol for coin_symbol, market_code in COIN_MARKET_CODES.items()}
                for row in payload:
                    market_code = row.get("market")
                    coin_symbol = market_to_symbol.get(market_code)
                    trade_price = row.get("trade_price")
                    if coin_symbol and trade_price is not None:
                        prices[coin_symbol] = float(trade_price)
        except Exception as batch_err:
            print(f"[coin-price] batch fetch failed, falling back to individual: {batch_err!r}")

        missing = [coin_symbol for coin_symbol in ALL_COIN_SYMBOLS if coin_symbol not in prices]
        for coin_symbol in list(missing):
            single_price = _fetch_single_coin_price_krw(coin_symbol)
            if single_price is not None:
                prices[coin_symbol] = single_price

        if not prices:
            raise RuntimeError("코인 시세를 불러오지 못했어요.")

        _update_market_cache("coin_prices_krw", prices)
        return dict(prices)
    except Exception:
        if cache["value"] is not None:
            return dict(cache["value"])
        raise


async def get_coin_charge_quote(coin_symbol: str, usd_amount: int) -> dict:
    if coin_symbol not in COIN_MARKET_CODES and coin_symbol not in BINANCE_COIN_SYMBOLS:
        raise app_commands.AppCommandError("지원하지 않는 코인 종류예요.")

    usd_krw_rate_result, coin_prices_result = await asyncio.gather(
        asyncio.to_thread(_get_cached_usd_krw_rate),
        asyncio.to_thread(_get_cached_coin_prices_krw),
        return_exceptions=True,
    )

    if isinstance(coin_prices_result, Exception):
        print(f"[coin-charge] coin price fetch failed: {coin_prices_result!r}")
        raise app_commands.AppCommandError("코인 시세를 불러오지 못했어요. 잠시 후 다시 시도해 주세요.")

    coin_prices = coin_prices_result
    if isinstance(usd_krw_rate_result, Exception):
        fallback_rate = float(coin_prices.get("USDT") or 0)
        if fallback_rate <= 0:
            print(f"[coin-charge] exchange rate fetch failed without fallback: {usd_krw_rate_result!r}")
            raise app_commands.AppCommandError("실시간 환율을 불러오지 못했어요. 잠시 후 다시 시도해 주세요.")

        usd_krw_rate = fallback_rate
        _update_market_cache("usd_krw_rate", usd_krw_rate)
        print(
            "[coin-charge] exchange rate API failed; "
            f"using KRW-USDT fallback rate: {usd_krw_rate:.2f} KRW"
        )
    else:
        usd_krw_rate = float(usd_krw_rate_result)

    coin_price_krw = coin_prices.get(coin_symbol)
    if coin_price_krw is None or coin_price_krw <= 0:
        raise app_commands.AppCommandError("선택한 코인의 현재 시세를 불러오지 못했어요.")

    try:
        kimchi_premium = _get_kimchi_premium()
    except Exception:
        kimchi_premium = 0.0

    credited_amount = max(1, int(round(usd_amount * usd_krw_rate)))
    coin_quantity = credited_amount / coin_price_krw
    coin_price_usd = coin_price_krw / usd_krw_rate

    return {
        "coin_symbol": coin_symbol,
        "usd_amount": usd_amount,
        "usd_krw_rate": usd_krw_rate,
        "credited_amount": credited_amount,
        "coin_price_krw": coin_price_krw,
        "coin_price_usd": coin_price_usd,
        "coin_quantity": coin_quantity,
        "coin_quantity_text": format_decimal(coin_quantity),
        "kimchi_premium": kimchi_premium,
    }


async def get_coin_price_preview(configured_coins: list[str]) -> tuple[dict[str, float], float, str | None]:
    if not configured_coins:
        return {}, 0.0, None

    try:
        coin_prices = await asyncio.to_thread(_get_cached_coin_prices_krw)
        try:
            kimchi_premium = await asyncio.to_thread(_get_kimchi_premium)
        except Exception:
            kimchi_premium = 0.0
        preview = {coin_symbol: coin_prices[coin_symbol] for coin_symbol in configured_coins if coin_symbol in coin_prices}
        missing = [coin_symbol for coin_symbol in configured_coins if coin_symbol not in preview]
        if missing:
            return preview, kimchi_premium, f"일부 코인 시세를 불러오지 못했어요: {', '.join(missing)}"
        return preview, kimchi_premium, None
    except Exception as e:
        print(f"[coin-charge] preview fetch failed: {e!r}")
        return {}, 0.0, "코인 시세를 불러오지 못했어요."


def build_coin_quote_fields(quote: dict) -> list[tuple[str, str]]:
    coin_symbol = get_coin_display_symbol(quote["coin_symbol"])
    fields = [
        ("달러 금액", format_usd(quote["usd_amount"])),
        ("적용 환율", f"1 USD = {quote['usd_krw_rate']:,.2f}원"),
        ("현재 시세", f"1 {coin_symbol} = {quote['coin_price_krw']:,.2f}원 / ${quote['coin_price_usd']:,.4f}"),
    ]
    if quote["coin_symbol"] in BINANCE_COIN_SYMBOLS:
        premium = quote.get("kimchi_premium", 0.0)
        sign = "+" if premium >= 0 else ""
        fields.append(("김치프리미엄", f"{sign}{premium * 100:.2f}% 반영됨"))
    fields.extend([
        ("예상 전송 수량", f"{quote['coin_quantity_text']} {coin_symbol}"),
        ("충전 반영 금액", format_won(quote["credited_amount"])),
    ])
    return fields


def format_charge_amount(amount: int, charge_type: str, credited_amount: int | None = None) -> str:
    if charge_type == "coin":
        if credited_amount is not None and credited_amount > 0:
            return f"{format_usd(amount)} -> {format_won(credited_amount)}"
        return format_usd(amount)
    return format_won(amount)


# ============================================================
# 유저 / 잔액 / 할인
# ============================================================

def ensure_user_row(guild_id: int, user_id: int):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT guild_id, discord_user_id
        FROM users
        WHERE guild_id = ? AND discord_user_id = ?
    """, (guild_id, user_id))
    row = cur.fetchone()

    if row is None:
        cur.execute("""
            INSERT INTO users (guild_id, discord_user_id, balance, total_spent)
            VALUES (?, ?, 0, 0)
        """, (guild_id, user_id))
        conn.commit()

    conn.close()


def get_discount_percent(total_spent: int) -> int:
    if total_spent >= 1_000_000:
        return 10
    if total_spent >= 500_000:
        return 7
    if total_spent >= 200_000:
        return 5
    if total_spent >= 100_000:
        return 3
    return 0


def get_discount_text(total_spent: int) -> str:
    percent = get_discount_percent(total_spent)
    return "없음" if percent <= 0 else f"{percent}%"


def apply_discount(price: int, discount_percent: int) -> int:
    return int(price * (100 - discount_percent) / 100)


def get_user_stats(guild_id: int, user_id: int) -> dict:
    ensure_user_row(guild_id, user_id)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT balance, total_spent
        FROM users
        WHERE guild_id = ? AND discord_user_id = ?
    """, (guild_id, user_id))
    row = cur.fetchone()
    conn.close()

    balance = row["balance"]
    total_spent = row["total_spent"]

    return {
        "balance": balance,
        "total_spent": total_spent,
        "discount_percent": get_discount_percent(total_spent),
        "discount_text": get_discount_text(total_spent),
    }


def add_balance_db(guild_id: int, user_id: int, amount: int):
    ensure_user_row(guild_id, user_id)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        UPDATE users
        SET balance = balance + ?
        WHERE guild_id = ? AND discord_user_id = ?
    """, (amount, guild_id, user_id))

    conn.commit()
    conn.close()


def subtract_balance_db(guild_id: int, user_id: int, amount: int) -> tuple[bool, int]:
    ensure_user_row(guild_id, user_id)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT balance
        FROM users
        WHERE guild_id = ? AND discord_user_id = ?
    """, (guild_id, user_id))
    row = cur.fetchone()
    current_balance = row["balance"]

    if current_balance < amount:
        conn.rollback()
        conn.close()
        return False, current_balance

    new_balance = current_balance - amount

    cur.execute("""
        UPDATE users
        SET balance = ?
        WHERE guild_id = ? AND discord_user_id = ?
    """, (new_balance, guild_id, user_id))

    conn.commit()
    conn.close()
    return True, new_balance


def transfer_balance_db(
    guild_id: int,
    sender_user_id: int,
    recipient_user_id: int,
    amount: int,
) -> tuple[bool, str, dict | None]:
    if amount <= 0:
        return False, "송금 금액은 1원 이상이어야 해요.", None

    if sender_user_id == recipient_user_id:
        return False, "본인에게는 송금할 수 없어요.", None

    conn = get_db()
    cur = conn.cursor()

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("""
        INSERT INTO users (guild_id, discord_user_id, balance, total_spent)
        VALUES (?, ?, 0, 0)
        ON CONFLICT(guild_id, discord_user_id) DO NOTHING
    """, (guild_id, sender_user_id))
    cur.execute("""
        INSERT INTO users (guild_id, discord_user_id, balance, total_spent)
        VALUES (?, ?, 0, 0)
        ON CONFLICT(guild_id, discord_user_id) DO NOTHING
    """, (guild_id, recipient_user_id))

    cur.execute("""
        SELECT balance
        FROM users
        WHERE guild_id = ? AND discord_user_id = ?
    """, (guild_id, sender_user_id))
    sender_row = cur.fetchone()
    sender_balance = sender_row["balance"]

    if sender_balance < amount:
        conn.rollback()
        conn.close()
        return False, f"잔액이 부족해요. 현재 잔액: {format_won(sender_balance)}", None

    new_sender_balance = sender_balance - amount
    cur.execute("""
        UPDATE users
        SET balance = balance - ?
        WHERE guild_id = ? AND discord_user_id = ?
    """, (amount, guild_id, sender_user_id))
    cur.execute("""
        UPDATE users
        SET balance = balance + ?
        WHERE guild_id = ? AND discord_user_id = ?
    """, (amount, guild_id, recipient_user_id))
    cur.execute("""
        INSERT INTO balance_transfers (
            guild_id, sender_user_id, recipient_user_id, amount, created_at
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        guild_id,
        sender_user_id,
        recipient_user_id,
        amount,
        now_iso(),
    ))
    transfer_id = cur.lastrowid

    cur.execute("""
        SELECT balance
        FROM users
        WHERE guild_id = ? AND discord_user_id = ?
    """, (guild_id, recipient_user_id))
    recipient_row = cur.fetchone()
    new_recipient_balance = recipient_row["balance"]

    conn.commit()
    conn.close()

    return True, "송금 완료", {
        "transfer_id": transfer_id,
        "amount": amount,
        "new_sender_balance": new_sender_balance,
        "new_recipient_balance": new_recipient_balance,
        "recipient_user_id": recipient_user_id,
    }


def create_balance_withdrawal(
    guild_id: int,
    user_id: int,
    bank_name: str,
    account_number: str,
    account_holder: str,
    request_amount: int,
) -> tuple[bool, str, dict | None]:
    clean_bank_name = sanitize_plain_text(bank_name, max_length=50)
    clean_account_number = normalize_money_account(account_number, max_length=40)
    clean_account_holder = sanitize_plain_text(account_holder, max_length=50)

    if not clean_bank_name:
        return False, "은행명을 입력해 주세요.", None
    if not clean_account_number:
        return False, "계좌번호를 올바르게 입력해 주세요.", None
    if not clean_account_holder:
        return False, "예금주명을 입력해 주세요.", None
    if request_amount <= 0:
        return False, "출금 금액은 1원 이상이어야 해요.", None

    fee_amount = calculate_withdrawal_fee(request_amount)
    payout_amount = request_amount - fee_amount
    if payout_amount <= 0:
        return False, "출금 후 실수령액이 0원 이하예요. 금액을 다시 확인해 주세요.", None

    ensure_user_row(guild_id, user_id)

    conn = get_db()
    cur = conn.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute("""
        SELECT balance
        FROM users
        WHERE guild_id = ? AND discord_user_id = ?
    """, (guild_id, user_id))
    row = cur.fetchone()
    current_balance = row["balance"]

    if current_balance < request_amount:
        conn.rollback()
        conn.close()
        return False, f"잔액이 부족해요. 현재 잔액: {format_won(current_balance)}", None

    new_balance = current_balance - request_amount
    cur.execute("""
        UPDATE users
        SET balance = ?
        WHERE guild_id = ? AND discord_user_id = ?
    """, (new_balance, guild_id, user_id))
    cur.execute("""
        INSERT INTO balance_withdrawals (
            guild_id, discord_user_id, bank_name, account_number, account_holder,
            request_amount, fee_amount, payout_amount, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        guild_id,
        user_id,
        clean_bank_name,
        clean_account_number,
        clean_account_holder,
        request_amount,
        fee_amount,
        payout_amount,
        now_iso(),
    ))
    withdrawal_id = cur.lastrowid

    conn.commit()
    conn.close()

    return True, "출금 요청 완료", {
        "withdrawal_id": withdrawal_id,
        "bank_name": clean_bank_name,
        "account_number": clean_account_number,
        "account_holder": clean_account_holder,
        "request_amount": request_amount,
        "fee_amount": fee_amount,
        "payout_amount": payout_amount,
        "new_balance": new_balance,
    }


# ============================================================
# 충전 DB
# ============================================================

def get_charge_request(request_id: int) -> sqlite3.Row | None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM charge_requests WHERE id = ?", (request_id,))
    row = cur.fetchone()
    conn.close()
    return row


def create_charge_request(
    guild_id: int,
    user_id: int,
    amount: int,
    depositor_name: str = "",
    *,
    charge_type: str = "bank",
    coin_symbol: str = "",
    credited_amount: int | None = None,
    usd_krw_rate: float = 0.0,
    coin_price_krw: float = 0.0,
    coin_quantity: str = "",
    transaction_id: str = "",
    status: str = "pending",
) -> int:
    clean_name = sanitize_plain_text(depositor_name, max_length=50)
    clean_coin_symbol = sanitize_plain_text(coin_symbol, max_length=20)
    clean_coin_quantity = sanitize_plain_text(coin_quantity, max_length=50)
    clean_transaction_id = normalize_transaction_id(transaction_id)
    final_credited_amount = amount if charge_type != "coin" else int(credited_amount or 0)

    if charge_type == "bank" and not clean_name:
        raise app_commands.AppCommandError("입금자명을 비워둘 수 없어요.")
    if charge_type == "coin" and clean_coin_symbol not in COIN_WALLET_COLUMNS:
        raise app_commands.AppCommandError("지원하지 않는 코인 종류예요.")
    if charge_type == "coin" and final_credited_amount <= 0:
        raise app_commands.AppCommandError("실시간 코인 환산 금액을 계산하지 못했어요. 다시 시도해 주세요.")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO charge_requests (
            guild_id, discord_user_id, amount, credited_amount, depositor_name,
            charge_type, coin_symbol, usd_krw_rate, coin_price_krw, coin_quantity,
            transaction_id, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        guild_id,
        user_id,
        amount,
        final_credited_amount,
        clean_name,
        charge_type,
        clean_coin_symbol,
        float(usd_krw_rate or 0.0),
        float(coin_price_krw or 0.0),
        clean_coin_quantity,
        clean_transaction_id,
        status,
        now_iso(),
    ))

    request_id = cur.lastrowid
    conn.commit()
    conn.close()
    return request_id


def submit_coin_transaction_id(
    request_id: int,
    user_id: int,
    transaction_id: str,
) -> tuple[bool, str, sqlite3.Row | None]:
    clean_transaction_id = normalize_transaction_id(transaction_id)
    if not clean_transaction_id:
        return False, "트랜잭션 ID를 비워둘 수 없어요.", None

    conn = get_db()
    cur = conn.cursor()

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("SELECT * FROM charge_requests WHERE id = ?", (request_id,))
    row = cur.fetchone()

    if row is None:
        conn.rollback()
        conn.close()
        return False, "충전 요청 정보를 찾을 수 없어요.", None

    if row["discord_user_id"] != user_id:
        conn.rollback()
        conn.close()
        return False, "본인 요청만 제출할 수 있어요.", None

    if row["charge_type"] != "coin":
        conn.rollback()
        conn.close()
        return False, "코인 충전 요청이 아니에요.", None

    if row["status"] == "pending":
        conn.rollback()
        conn.close()
        return False, "이미 트랜잭션 ID를 제출했어요.", row

    if row["status"] != "awaiting_txid":
        conn.rollback()
        conn.close()
        return False, "이미 처리된 충전 요청이에요.", row

    cur.execute("""
        UPDATE charge_requests
        SET transaction_id = ?,
            status = 'pending'
        WHERE id = ? AND status = 'awaiting_txid'
    """, (
        clean_transaction_id,
        request_id,
    ))

    if cur.rowcount != 1:
        conn.rollback()
        conn.close()
        return False, "트랜잭션 ID를 저장하지 못했어요. 다시 시도해 주세요.", None

    cur.execute("SELECT * FROM charge_requests WHERE id = ?", (request_id,))
    updated_row = cur.fetchone()
    conn.commit()
    conn.close()
    return True, "트랜잭션 ID 제출 완료", updated_row


def approve_charge_request(request_id: int, admin_user_id: int) -> tuple[bool, str, sqlite3.Row | None]:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("SELECT * FROM charge_requests WHERE id = ?", (request_id,))
    row = cur.fetchone()

    if row is None:
        conn.rollback()
        conn.close()
        return False, "존재하지 않는 충전 요청이에요.", None

    if row["status"] != "pending":
        conn.rollback()
        conn.close()
        return False, "이미 처리된 충전 요청이에요.", None

    guild_id = row["guild_id"]
    user_id = row["discord_user_id"]
    amount = row["amount"]
    credited_amount = int(row["credited_amount"] or 0) or amount

    cur.execute("""
        INSERT INTO users (guild_id, discord_user_id, balance, total_spent)
        VALUES (?, ?, 0, 0)
        ON CONFLICT(guild_id, discord_user_id) DO NOTHING
    """, (guild_id, user_id))

    cur.execute("""
        UPDATE users
        SET balance = balance + ?
        WHERE guild_id = ? AND discord_user_id = ?
    """, (credited_amount, guild_id, user_id))

    cur.execute("""
        UPDATE charge_requests
        SET status = 'approved',
            processed_at = ?,
            processed_by = ?
        WHERE id = ? AND status = 'pending'
    """, (
        now_iso(),
        admin_user_id,
        request_id,
    ))

    if cur.rowcount != 1:
        conn.rollback()
        conn.close()
        return False, "이미 처리된 충전 요청이에요.", None

    conn.commit()
    conn.close()

    return True, "승인 완료", row


def reject_charge_request(request_id: int, admin_user_id: int) -> tuple[bool, str, int | None]:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("SELECT * FROM charge_requests WHERE id = ?", (request_id,))
    row = cur.fetchone()

    if row is None:
        conn.rollback()
        conn.close()
        return False, "존재하지 않는 충전 요청이에요.", None

    if row["status"] != "pending":
        conn.rollback()
        conn.close()
        return False, "이미 처리된 충전 요청이에요.", None

    user_id = row["discord_user_id"]

    cur.execute("""
        UPDATE charge_requests
        SET status = 'rejected',
            processed_at = ?,
            processed_by = ?
        WHERE id = ? AND status = 'pending'
    """, (
        now_iso(),
        admin_user_id,
        request_id,
    ))

    if cur.rowcount != 1:
        conn.rollback()
        conn.close()
        return False, "이미 처리된 충전 요청이에요.", None

    conn.commit()
    conn.close()

    return True, "거절 완료", user_id


def get_withdrawal(withdrawal_id: int) -> sqlite3.Row | None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM balance_withdrawals WHERE id = ?", (withdrawal_id,))
    row = cur.fetchone()
    conn.close()
    return row


def approve_withdrawal(withdrawal_id: int, admin_user_id: int) -> tuple[bool, str, sqlite3.Row | None]:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("SELECT * FROM balance_withdrawals WHERE id = ?", (withdrawal_id,))
    row = cur.fetchone()

    if row is None:
        conn.rollback()
        conn.close()
        return False, "존재하지 않는 출금 요청이에요.", None

    if row["status"] != "submitted":
        conn.rollback()
        conn.close()
        return False, "이미 처리된 출금 요청이에요.", None

    cur.execute("""
        UPDATE balance_withdrawals
        SET status = 'approved',
            processed_at = ?,
            processed_by = ?
        WHERE id = ? AND status = 'submitted'
    """, (now_iso(), admin_user_id, withdrawal_id))

    if cur.rowcount != 1:
        conn.rollback()
        conn.close()
        return False, "이미 처리된 출금 요청이에요.", None

    conn.commit()
    conn.close()

    return True, "승인 완료", row


def reject_withdrawal(withdrawal_id: int, admin_user_id: int) -> tuple[bool, str, sqlite3.Row | None]:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("SELECT * FROM balance_withdrawals WHERE id = ?", (withdrawal_id,))
    row = cur.fetchone()

    if row is None:
        conn.rollback()
        conn.close()
        return False, "존재하지 않는 출금 요청이에요.", None

    if row["status"] != "submitted":
        conn.rollback()
        conn.close()
        return False, "이미 처리된 출금 요청이에요.", None

    user_id = row["discord_user_id"]
    guild_id = row["guild_id"]
    request_amount = row["request_amount"]

    ensure_user_row(guild_id, user_id)
    cur.execute("""
        UPDATE users
        SET balance = balance + ?
        WHERE guild_id = ? AND discord_user_id = ?
    """, (request_amount, guild_id, user_id))

    cur.execute("""
        UPDATE balance_withdrawals
        SET status = 'rejected',
            processed_at = ?,
            processed_by = ?
        WHERE id = ? AND status = 'submitted'
    """, (now_iso(), admin_user_id, withdrawal_id))

    if cur.rowcount != 1:
        conn.rollback()
        conn.close()
        return False, "이미 처리된 출금 요청이에요.", None

    conn.commit()
    conn.close()

    return True, "거절 완료", row


# ============================================================
# 상품 / 구매 DB
# ============================================================

def upsert_item(
    guild_id: int,
    item_key: str,
    category: str,
    item_name: str,
    price: int,
    stock: int,
    description: str | None,
    display_order: int,
):
    clean_item_key = sanitize_item_key(item_key)
    clean_category = sanitize_plain_text(category, max_length=50)
    clean_item_name = sanitize_plain_text(item_name, max_length=80)
    clean_description = sanitize_plain_text(description, max_length=1000, multiline=True) if description else None

    if not clean_item_key:
        raise app_commands.AppCommandError("상품 키는 영문 소문자, 숫자, `_`, `-`만 사용할 수 있어요.")

    if not clean_category or not clean_item_name:
        raise app_commands.AppCommandError("카테고리와 상품 이름은 비워둘 수 없어요.")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO items (
            guild_id, item_key, category, item_name, price, stock, description, sales_count, display_order
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
        ON CONFLICT(guild_id, item_key) DO UPDATE SET
            category = excluded.category,
            item_name = excluded.item_name,
            price = excluded.price,
            stock = excluded.stock,
            description = excluded.description,
            display_order = excluded.display_order
    """, (
        guild_id,
        clean_item_key,
        clean_category,
        clean_item_name,
        price,
        stock,
        clean_description,
        display_order,
    ))

    conn.commit()
    conn.close()


def set_item_stock(guild_id: int, item_key: str, stock: int) -> bool:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        UPDATE items
        SET stock = ?
        WHERE guild_id = ? AND item_key = ?
    """, (stock, guild_id, normalize_item_key(item_key)))

    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def set_item_stock_by_name(guild_id: int, item_name: str, stock: int) -> tuple[bool, str]:
    item, error = resolve_item_by_name(guild_id, item_name)
    if item is None:
        return False, error or "상품을 찾을 수 없어요."

    changed = set_item_stock(guild_id, item["item_key"], stock)
    if not changed:
        return False, "재고를 변경하지 못했어요."

    return True, item["item_name"]


def delete_item(guild_id: int, item_key: str) -> bool:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM items
        WHERE guild_id = ? AND item_key = ?
    """, (guild_id, normalize_item_key(item_key)))

    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def delete_item_by_name(guild_id: int, item_name: str) -> tuple[bool, str]:
    item, error = resolve_item_by_name(guild_id, item_name)
    if item is None:
        return False, error or "상품을 찾을 수 없어요."

    changed = delete_item(guild_id, item["item_key"])
    if not changed:
        return False, "상품을 삭제하지 못했어요."

    return True, item["item_name"]


def get_item(guild_id: int, item_key: str):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM items
        WHERE guild_id = ? AND item_key = ?
    """, (guild_id, normalize_item_key(item_key)))

    row = cur.fetchone()
    conn.close()
    return row


def find_items_by_name(guild_id: int, item_name: str):
    clean_item_name = sanitize_plain_text(item_name, max_length=80)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM items
        WHERE guild_id = ? AND item_name = ? COLLATE NOCASE
        ORDER BY display_order ASC, item_key ASC
    """, (guild_id, clean_item_name))

    rows = cur.fetchall()
    conn.close()
    return rows


def resolve_item_by_name(guild_id: int, item_name: str) -> tuple[sqlite3.Row | None, str | None]:
    clean_item_name = sanitize_plain_text(item_name, max_length=80)
    if not clean_item_name:
        return None, "상품 이름을 비워둘 수 없어요."

    rows = find_items_by_name(guild_id, clean_item_name)
    if not rows:
        return None, "존재하지 않는 상품 이름이에요."

    if len(rows) > 1:
        return None, "같은 이름의 상품이 여러 개 있어요. 먼저 상품명을 정리해 주세요."

    return rows[0], None


def list_categories(guild_id: int) -> list[str]:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT category
        FROM items
        WHERE guild_id = ?
        ORDER BY category COLLATE NOCASE ASC
    """, (guild_id,))

    rows = cur.fetchall()
    conn.close()
    return [row["category"] for row in rows]


def list_items_by_category(guild_id: int, category: str):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM items
        WHERE guild_id = ? AND category = ?
        ORDER BY display_order ASC, item_name COLLATE NOCASE ASC
    """, (guild_id, category))

    rows = cur.fetchall()
    conn.close()
    return rows


def list_all_items(guild_id: int):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM items
        WHERE guild_id = ?
        ORDER BY category COLLATE NOCASE ASC, display_order ASC, item_name COLLATE NOCASE ASC
    """, (guild_id,))

    rows = cur.fetchall()
    conn.close()
    return rows


def create_purchase(guild_id: int, user_id: int, item_key: str) -> tuple[bool, str, dict | None]:
    conn = get_db()
    cur = conn.cursor()
    safe_item_key = sanitize_item_key(item_key)

    if not safe_item_key:
        conn.close()
        return False, "상품 키 형식이 올바르지 않아요.", None

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("""
        INSERT INTO users (guild_id, discord_user_id, balance, total_spent)
        VALUES (?, ?, 0, 0)
        ON CONFLICT(guild_id, discord_user_id) DO NOTHING
    """, (guild_id, user_id))

    cur.execute("""
        SELECT *
        FROM items
        WHERE guild_id = ? AND item_key = ?
    """, (guild_id, safe_item_key))
    item = cur.fetchone()

    if item is None:
        conn.rollback()
        conn.close()
        return False, "존재하지 않는 상품이에요.", None

    if item["stock"] <= 0:
        conn.rollback()
        conn.close()
        return False, "재고가 없어요.", None

    cur.execute("""
        SELECT balance, total_spent
        FROM users
        WHERE guild_id = ? AND discord_user_id = ?
    """, (guild_id, user_id))
    user = cur.fetchone()

    if user is None:
        conn.rollback()
        conn.close()
        return False, "유저 정보를 찾지 못했어요. 다시 시도해 주세요.", None

    current_balance = user["balance"]
    total_spent = user["total_spent"]
    discount_percent = get_discount_percent(total_spent)
    final_price = apply_discount(item["price"], discount_percent)

    if current_balance < final_price:
        conn.rollback()
        conn.close()
        return False, f"잔액이 부족해요. 현재 잔액: {current_balance}원", None

    new_balance = current_balance - final_price
    new_stock = item["stock"] - 1
    new_total_spent = total_spent + final_price
    new_sales_count = item["sales_count"] + 1

    cur.execute("""
        UPDATE users
        SET balance = ?, total_spent = ?
        WHERE guild_id = ? AND discord_user_id = ?
    """, (new_balance, new_total_spent, guild_id, user_id))

    if cur.rowcount != 1:
        conn.rollback()
        conn.close()
        return False, "잔액을 갱신하지 못했어요. 다시 시도해 주세요.", None

    cur.execute("""
        UPDATE items
        SET stock = ?, sales_count = ?
        WHERE guild_id = ? AND item_key = ?
    """, (new_stock, new_sales_count, guild_id, safe_item_key))

    if cur.rowcount != 1:
        conn.rollback()
        conn.close()
        return False, "재고가 동시에 변경되어 구매를 완료하지 못했어요. 다시 시도해 주세요.", None

    cur.execute("""
        INSERT INTO purchases (
            guild_id, discord_user_id, item_key, item_name, price_paid,
            original_price, discount_percent, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_delivery', ?)
    """, (
        guild_id,
        user_id,
        safe_item_key,
        item["item_name"],
        final_price,
        item["price"],
        discount_percent,
        now_iso(),
    ))

    purchase_id = cur.lastrowid

    conn.commit()
    conn.close()

    data = {
        "purchase_id": purchase_id,
        "guild_id": guild_id,
        "user_id": user_id,
        "item_key": item["item_key"],
        "item_name": item["item_name"],
        "price_paid": final_price,
        "original_price": item["price"],
        "discount_percent": discount_percent,
        "new_balance": new_balance,
        "new_stock": new_stock,
        "sales_count": new_sales_count,
        "description": item["description"] or "설명 없음",
    }

    return True, "구매 완료", data


def create_purchase_by_name(guild_id: int, user_id: int, item_name: str) -> tuple[bool, str, dict | None]:
    item, error = resolve_item_by_name(guild_id, item_name)
    if item is None:
        return False, error or "상품을 찾을 수 없어요.", None

    return create_purchase(guild_id, user_id, item["item_key"])

def mark_purchase_delivered_system(purchase_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        UPDATE purchases
        SET status = 'delivered',
            processed_at = ?,
            processed_by = 0
        WHERE id = ? AND status = 'pending_delivery'
    """, (
        now_iso(),
        purchase_id,
    ))

    changed = cur.rowcount == 1
    conn.commit()
    conn.close()
    return changed


def rollback_purchase_delivery(purchase_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("""
        SELECT *
        FROM purchases
        WHERE id = ? AND status = 'pending_delivery'
    """, (purchase_id,))
    purchase = cur.fetchone()

    if purchase is None:
        conn.rollback()
        conn.close()
        return False

    cur.execute("""
        UPDATE users
        SET balance = balance + ?,
            total_spent = CASE
                WHEN total_spent >= ? THEN total_spent - ?
                ELSE 0
            END
        WHERE guild_id = ? AND discord_user_id = ?
    """, (
        purchase["price_paid"],
        purchase["price_paid"],
        purchase["price_paid"],
        purchase["guild_id"],
        purchase["discord_user_id"],
    ))

    cur.execute("""
        UPDATE items
        SET stock = stock + 1,
            sales_count = CASE
                WHEN sales_count > 0 THEN sales_count - 1
                ELSE 0
            END
        WHERE guild_id = ? AND item_key = ?
    """, (
        purchase["guild_id"],
        purchase["item_key"],
    ))

    cur.execute("""
        UPDATE purchases
        SET status = 'delivery_failed',
            processed_at = ?,
            processed_by = 0
        WHERE id = ? AND status = 'pending_delivery'
    """, (
        now_iso(),
        purchase_id,
    ))

    ok = cur.rowcount == 1
    if ok:
        conn.commit()
    else:
        conn.rollback()
    conn.close()
    return ok


def mark_purchase_delivered(purchase_id: int, admin_user_id: int) -> tuple[bool, str, int | None, str | None]:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM purchases WHERE id = ?", (purchase_id,))
    purchase = cur.fetchone()

    if purchase is None:
        conn.close()
        return False, "존재하지 않는 구매 기록이에요.", None, None

    if purchase["status"] != "pending_delivery":
        conn.close()
        return False, "이미 처리된 구매 기록이에요.", None, None

    cur.execute("""
        UPDATE purchases
        SET status = 'delivered',
            processed_at = ?,
            processed_by = ?
        WHERE id = ?
    """, (
        now_iso(),
        admin_user_id,
        purchase_id,
    ))

    conn.commit()
    conn.close()

    return True, "지급 완료", purchase["discord_user_id"], purchase["item_name"]


async def send_purchase_log(guild: discord.Guild, buyer_mention: str, data: dict):
    settings = get_guild_settings(guild.id)
    channel_id = settings["purchase_log_channel_id"]

    if not channel_id:
        return

    log_channel = guild.get_channel(channel_id)
    if not isinstance(log_channel, discord.TextChannel):
        return

    await log_channel.send(
        view=build_component_view(
            build_component_container(
                "자동 전달된 구매 내역",
                description="구매 직후 상품 내용을 DM으로 자동 전달했어요.",
                fields=[
                    ("구매 ID", str(data["purchase_id"])),
                    ("구매자", buyer_mention),
                    ("상품명", sanitize_plain_text(data["item_name"], max_length=80)),
                    ("상품 키", sanitize_plain_text(data["item_key"], max_length=50)),
                    ("결제 금액", f"{data['price_paid']}원"),
                    ("상태", "delivered"),
                ],
                accent_color=SUCCESS_ACCENT,
            ),
            timeout=300,
        ),
        allowed_mentions=discord.AllowedMentions.none(),
    )


async def send_charge_log(
    guild: discord.Guild,
    user_mention: str,
    request_id: int,
    amount: int,
    depositor_name: str,
    *,
    charge_type: str = "bank",
    coin_symbol: str = "",
    transaction_id: str = "",
):
    settings = get_guild_settings(guild.id)
    channel_id = settings["charge_log_channel_id"]

    if not channel_id:
        return

    log_channel = guild.get_channel(channel_id)
    if not isinstance(log_channel, discord.TextChannel):
        return

    await log_channel.send(
        view=ChargeApprovalView(
            request_id=request_id,
            user_mention=user_mention,
            depositor_name=depositor_name,
            amount=amount,
            charge_type=charge_type,
            coin_symbol=coin_symbol,
            transaction_id=transaction_id,
        ),
        allowed_mentions=discord.AllowedMentions.none(),
    )


async def send_balance_withdrawal_log(guild: discord.Guild, user_mention: str, data: dict):
    settings = get_guild_settings(guild.id)
    channel_id = settings["charge_log_channel_id"]

    if not channel_id:
        return

    log_channel = guild.get_channel(channel_id)
    if not isinstance(log_channel, discord.TextChannel):
        return

    await log_channel.send(
        view=WithdrawalApprovalView(
            withdrawal_id=data["withdrawal_id"],
            user_mention=user_mention,
            request_amount=data["request_amount"],
            fee_amount=data["fee_amount"],
            payout_amount=data["payout_amount"],
            bank_name=data["bank_name"],
            account_number=data["account_number"],
            account_holder=data["account_holder"],
        ),
        allowed_mentions=discord.AllowedMentions.none(),
    )


async def resolve_guild_member(guild: discord.Guild, raw_value: str) -> discord.Member | None:
    user_id = parse_user_id_input(raw_value)
    if user_id is None:
        return None

    member = guild.get_member(user_id)
    if member is not None:
        return member

    try:
        return await guild.fetch_member(user_id)
    except Exception:
        return None


async def finalize_purchase_delivery(interaction: discord.Interaction, data: dict) -> tuple[bool, str]:
    try:
        await send_purchase_delivery_dm(interaction.user, data)
    except Exception:
        rollback_purchase_delivery(data["purchase_id"])
        return False, "DM 전송에 실패해서 구매를 취소하고 잔액과 재고를 복구했어요. 서버에서 개인메시지 허용 후 다시 시도해 주세요."

    mark_purchase_delivered_system(data["purchase_id"])

    if interaction.guild is not None:
        await send_purchase_log(interaction.guild, interaction.user.mention, data)

    return True, "ok"


# ============================================================
# 티켓 유틸
# ============================================================

# channel_id -> {"user_id": int, "guild_id": int, "issue_key": str}
_pending_evidence_channels: dict[int, dict] = {}


def get_ticket_owner_id(channel) -> int | None:
    if not isinstance(channel, discord.TextChannel) or not channel.topic:
        return None

    for prefix in ("ticket_owner:", "ticket_closed:"):
        if channel.topic.startswith(prefix):
            raw = channel.topic[len(prefix):].strip()
            if raw.isdigit():
                return int(raw)

    return None


def is_ticket_channel(channel) -> bool:
    return get_ticket_owner_id(channel) is not None


def is_ticket_closed(channel) -> bool:
    return (
        isinstance(channel, discord.TextChannel)
        and bool(channel.topic)
        and channel.topic.startswith("ticket_closed:")
    )


def get_ticket_staff_role(guild: discord.Guild) -> discord.Role | None:
    settings = get_guild_settings(guild.id)
    role_id = settings["ticket_staff_role_id"]
    if not role_id:
        return None
    return guild.get_role(role_id)


def get_ticket_issue_label(issue_key: str) -> str:
    for label, key, _, _ in TICKET_ISSUE_TYPES:
        if key == issue_key:
            return label
    return issue_key


def get_ticket_issue_role(guild: discord.Guild, issue_key: str) -> discord.Role | None:
    settings = get_guild_settings(guild.id)
    column_name = TICKET_ISSUE_ROLE_COLUMNS.get(issue_key)
    if not column_name:
        return None

    role_id = settings.get(column_name, 0)
    if not role_id:
        return None

    return guild.get_role(role_id)


def get_ticket_access_roles(guild: discord.Guild, issue_key: str) -> list[discord.Role]:
    roles: list[discord.Role] = []

    for role in (get_ticket_staff_role(guild), get_ticket_issue_role(guild, issue_key)):
        if role is not None and role not in roles:
            roles.append(role)

    return roles


def get_ticket_issue_message(guild_id: int, issue_key: str) -> str:
    settings = get_guild_settings(guild_id)
    column_name = TICKET_ISSUE_MESSAGE_COLUMNS.get(issue_key)
    if not column_name:
        return ""
    return (settings.get(column_name) or "").strip()


def get_auction_channel(guild: discord.Guild) -> discord.TextChannel | None:
    settings = get_guild_settings(guild.id)
    channel_id = settings.get("auction_channel_id", 0)
    if not channel_id:
        return None
    channel = guild.get_channel(channel_id)
    if isinstance(channel, discord.TextChannel):
        return channel
    return None



def has_staff_access(member: discord.Member) -> bool:
    if member.guild_permissions.administrator or member.guild_permissions.manage_channels:
        return True

    settings = get_guild_settings(member.guild.id)
    role_ids = {settings["ticket_staff_role_id"]}
    for column_name in TICKET_ISSUE_ROLE_COLUMNS.values():
        role_ids.add(settings.get(column_name, 0))

    for role in member.roles:
        if role.id in role_ids:
            return True

    return False


def can_manage_ticket(member: discord.Member, channel) -> bool:
    owner_id = get_ticket_owner_id(channel)
    if owner_id is None:
        return False
    return member.id == owner_id or has_staff_access(member)


def find_open_ticket_channels(guild: discord.Guild, user_id: int) -> list[discord.TextChannel]:
    return [
        channel
        for channel in guild.text_channels
        if channel.topic == f"ticket_owner:{user_id}"
    ]


def find_open_ticket_channel(guild: discord.Guild, user_id: int) -> discord.TextChannel | None:
    channels = find_open_ticket_channels(guild, user_id)
    return channels[0] if channels else None


def get_ticket_limit_message(guild: discord.Guild, user_id: int) -> str | None:
    open_channels = find_open_ticket_channels(guild, user_id)
    if len(open_channels) < MAX_OPEN_TICKETS_PER_USER:
        return None

    channel_mentions = ", ".join(channel.mention for channel in open_channels[:MAX_OPEN_TICKETS_PER_USER])
    return (
        f"열린 티켓은 최대 {MAX_OPEN_TICKETS_PER_USER}개까지 만들 수 있어요. "
        f"현재 열린 티켓: {channel_mentions}"
    )


async def get_or_create_ticket_category(guild: discord.Guild) -> discord.CategoryChannel:
    settings = get_guild_settings(guild.id)
    category_name = settings["ticket_category_name"] or DEFAULT_TICKET_CATEGORY_NAME

    category = discord.utils.get(guild.categories, name=category_name)
    if category is not None:
        return category

    return await guild.create_category(
        category_name,
        reason="Ticket system setup"
    )


async def create_ticket_channel(
    interaction: discord.Interaction,
    issue_key: str
) -> discord.TextChannel:
    guild = interaction.guild
    if guild is None:
        raise RuntimeError("서버에서만 사용할 수 있어요.")

    bot_member = guild.me
    if bot_member is None:
        raise RuntimeError("봇 멤버 정보를 가져오지 못했어요.")

    category = await get_or_create_ticket_category(guild)
    issue_text = get_ticket_issue_label(issue_key)
    access_roles = get_ticket_access_roles(guild, issue_key)
    mention_role = get_ticket_issue_role(guild, issue_key) or get_ticket_staff_role(guild)

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        interaction.user: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        ),
        bot_member: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
            manage_messages=True,
            attach_files=True,
            embed_links=True,
        ),
    }

    for role in access_roles:
        overwrites[role] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
            manage_messages=True,
            attach_files=True,
            embed_links=True,
        )

    channel = await guild.create_text_channel(
        name=f"ticket-{interaction.user.id}",
        category=category,
        overwrites=overwrites,
        topic=f"ticket_owner:{interaction.user.id}",
        reason=f"Ticket created by {interaction.user}",
    )

    content = mention_role.mention if mention_role is not None else None

    await send_component_view(
        channel,
        TicketControlsView(
            owner_mention=interaction.user.mention,
            issue_text=issue_text,
        ),
        mention_text=content,
        allowed_mentions=discord.AllowedMentions(roles=True),
    )

    custom_msg = get_ticket_issue_message(guild.id, issue_key)
    if custom_msg:
        await channel.send(
            view=build_component_view(
                build_component_container(
                    description=custom_msg,
                    accent_color=INFO_ACCENT,
                ),
                timeout=300,
            ),
        )

    return channel


async def close_ticket_channel(channel: discord.TextChannel, actor: discord.abc.User) -> None:
    owner_id = get_ticket_owner_id(channel)
    if owner_id is None:
        raise RuntimeError("티켓 채널이 아니에요.")

    owner_member = channel.guild.get_member(owner_id)
    if owner_member is not None:
        overwrite = channel.overwrites_for(owner_member)
        overwrite.send_messages = False
        overwrite.add_reactions = False
        overwrite.attach_files = False
        await channel.set_permissions(
            owner_member,
            overwrite=overwrite,
            reason=f"Ticket closed by {actor}",
        )

    new_name = channel.name
    if not new_name.startswith("closed-"):
        new_name = f"closed-{new_name}"
        new_name = new_name[:100]

    await channel.edit(
        name=new_name,
        topic=f"ticket_closed:{owner_id}",
        reason=f"Ticket closed by {actor}",
    )


async def delete_ticket_channel(channel: discord.TextChannel, actor: discord.abc.User) -> None:
    await channel.delete(reason=f"Ticket deleted by {actor}")


# ============================================================
# 제재 유틸
# ============================================================

def can_act_on_member(
    actor: discord.Member,
    target: discord.Member,
    guild: discord.Guild,
) -> tuple[bool, str | None]:
    if target.id == actor.id:
        return False, "자기 자신에게는 사용할 수 없어요."

    if target.id == guild.owner_id:
        return False, "서버 소유자에게는 사용할 수 없어요."

    if target.guild_permissions.administrator:
        return False, "관리자 권한이 있는 유저에게는 사용할 수 없어요."

    if actor.id != guild.owner_id and target.top_role >= actor.top_role:
        return False, "자기보다 높거나 같은 역할의 유저에게는 사용할 수 없어요."

    bot_member = guild.me
    if bot_member is None:
        return False, "봇 멤버 정보를 가져오지 못했어요."

    if target.top_role >= bot_member.top_role:
        return False, "봇보다 높거나 같은 역할의 유저에게는 사용할 수 없어요."

    return True, None


# ============================================================
# 컴포넌트 V2 빌더 - 마켓 UI
# ============================================================

def build_main_container(guild_id: int) -> discord.ui.Container:
    settings = get_guild_settings(guild_id)
    title = (settings["market_title"] or "").strip() or DEFAULT_MARKET_TITLE or "Market"
    return build_component_container(
        title,
        description=(
            "이용할 서비스를 아래 버튼에서 선택해 주세요.\n"
            "구매와 충전 전에 안내사항을 먼저 확인해 주세요.\n"
            "버튼을 누르면 본인만 볼 수 있는 화면이 열려요."
        ),
        accent_color=MARKET_ACCENT,
    )


def build_purchase_container(selected_category: str | None = None) -> discord.ui.Container:
    fields = []
    if selected_category:
        fields.append(("선택된 카테고리", selected_category))

    return build_component_container(
        "구매하기",
        description="카테고리를 고르면 제품 목록과 구매 단계가 순서대로 열려요.",
        fields=fields,
        accent_color=MARKET_ACCENT,
    )


def build_catalog_container(guild_id: int, selected_category: str | None = None) -> discord.ui.Container:
    fields: list[tuple[str, str]] = []

    if selected_category:
        items = list_items_by_category(guild_id, selected_category)
        if not items:
            fields.append((f"{selected_category}", "등록된 상품이 없어요."))
        else:
            lines = []
            for item in items[:10]:
                lines.append(
                    f"**{item['item_name']}**\n"
                    f"재고 {item['stock']}개 · 총 {item['sales_count']}개 판매"
                )

            value = "\n\n".join(lines)
            if len(items) > 10:
                value += f"\n\n외 {len(items) - 10}개..."

            fields.append((f"{selected_category} ({len(items)}개)", value))

    return build_component_container(
        "제품 목록",
        description="카테고리를 선택하면 제품 목록이 표시됩니다.",
        fields=fields,
        accent_color=INFO_ACCENT,
    )


def build_item_detail_container(guild_id: int, user_id: int, item) -> discord.ui.Container:
    stats = get_user_stats(guild_id, user_id)
    discount_percent = stats["discount_percent"]
    final_price = apply_discount(item["price"], discount_percent)

    if discount_percent > 0:
        price_fields = [
            ("정가", f"{item['price']}원"),
            ("적용 할인", f"{discount_percent}%"),
            ("결제 금액", f"{final_price}원"),
        ]
    else:
        price_fields = [
            ("가격", f"{item['price']}원"),
            ("적용 할인", "없음"),
            ("결제 금액", f"{final_price}원"),
        ]

    return build_component_container(
        item["item_name"],
        description="상품 상세 정보는 구매 완료 후 개인메시지(DM)로 전달돼요.",
        fields=[
            ("카테고리", item["category"]),
            ("재고", f"{item['stock']}개"),
            ("판매량", f"{item['sales_count']}개"),
            *price_fields,
        ],
        accent_color=INFO_ACCENT,
    )


def build_charge_container() -> discord.ui.Container:
    return build_component_container(
        "충전하기",
        description="원하는 충전 방식을 선택하세요. 계좌이체와 코인 충전을 모두 지원해요.",
        accent_color=WARNING_ACCENT,
    )


def build_charge_result_container(guild_id: int, request_id: int, depositor: str, amount: int) -> discord.ui.Container:
    settings = get_guild_settings(guild_id)
    return build_component_container(
        "충전 요청 접수 완료",
        description=(
            "아래 계좌로 정확한 금액을 입금해 주세요.\n"
            "관리자가 확인 후 승인하면 잔액이 충전돼요."
        ),
        fields=[
            ("요청 ID", str(request_id)),
            ("입금자명", depositor),
            ("금액", f"{amount}원"),
            ("은행", settings["bank_name"] or "-"),
            ("계좌번호", settings["bank_account_number"] or "-"),
            ("예금주", settings["bank_account_holder"] or "-"),
        ],
        footer="입금자명과 금액이 다르면 승인 지연이 생길 수 있어요.",
        accent_color=WARNING_ACCENT,
    )


def build_coin_charge_select_container(
    configured_coins: list[str],
    selected_coin: str | None = None,
    price_preview: dict[str, float] | None = None,
    preview_error: str | None = None,
    kimchi_premium: float = 0.0,
) -> discord.ui.Container:
    fields = []
    if selected_coin:
        fields.append(("선택한 코인", selected_coin))
    if configured_coins:
        lines = []
        for coin_symbol in configured_coins:
            display_symbol = get_coin_display_symbol(coin_symbol)
            if price_preview and coin_symbol in price_preview:
                price_text = f"{display_symbol}: {price_preview[coin_symbol]:,.2f}원"
                if coin_symbol in BINANCE_COIN_SYMBOLS:
                    price_text += " (김프 반영)"
                lines.append(price_text)
            else:
                lines.append(f"{display_symbol}: 시세 확인 중")
        fields.append(("현재 코인 시세", "\n".join(lines)))
    if abs(kimchi_premium) > 0.0001:
        sign = "+" if kimchi_premium >= 0 else ""
        fields.append(("김치프리미엄", f"{sign}{kimchi_premium * 100:.2f}%"))
    if preview_error:
        fields.append(("안내", preview_error))

    return build_component_container(
        "코인 충전",
        description=(
            "충전할 코인 종류를 먼저 선택한 뒤 달러 금액을 입력해 주세요.\n"
            "입금 안내를 받은 뒤 트랜잭션 ID를 제출하면 관리자 확인이 시작돼요."
        ),
        fields=fields,
        accent_color=WARNING_ACCENT,
    )


def build_coin_charge_result_container(
    guild_id: int,
    request_id: int,
    quote: dict,
) -> discord.ui.Container:
    wallet_address = get_coin_wallet_address(guild_id, quote["coin_symbol"]) or "-"
    display_symbol = get_coin_display_symbol(quote["coin_symbol"])
    return build_component_container(
        "코인 충전 요청 접수 완료",
        description=(
            "아래 주소로 정확한 금액을 입금해 주세요.\n"
            "입금이 끝나면 트랜잭션 ID를 제출해야 관리자 확인이 시작돼요."
        ),
        fields=[
            ("요청 ID", str(request_id)),
            ("충전 방식", "coin"),
            ("코인 종류", display_symbol),
            *build_coin_quote_fields(quote),
            ("입금 주소", wallet_address),
            ("상태", "awaiting_txid"),
        ],
        footer="네트워크와 코인 종류를 꼭 확인한 뒤 전송해 주세요.",
        accent_color=WARNING_ACCENT,
    )


def build_coin_txid_submitted_container(request_id: int, quote: dict, transaction_id: str) -> discord.ui.Container:
    display_symbol = get_coin_display_symbol(quote["coin_symbol"])
    return build_component_container(
        "트랜잭션 ID 제출 완료",
        description="관리자가 입금을 확인한 뒤 승인 또는 거절해 줄 거예요.",
        fields=[
            ("요청 ID", str(request_id)),
            ("충전 방식", "coin"),
            ("코인 종류", display_symbol),
            *build_coin_quote_fields(quote),
            ("트랜잭션 ID", transaction_id),
            ("상태", "pending"),
        ],
        accent_color=SUCCESS_ACCENT,
    )


def build_info_container(guild_id: int, user: discord.abc.User) -> discord.ui.Container:
    stats = get_user_stats(guild_id, user.id)
    return build_component_container(
        "정보",
        fields=[
            ("유저", user.mention),
            ("잔액", format_won(stats["balance"])),
            ("누적 구매금액", format_won(stats["total_spent"])),
            ("적용 할인", stats["discount_text"]),
        ],
        accent_color=INFO_ACCENT,
    )


def build_balance_transfer_result_container(recipient: discord.Member, data: dict) -> discord.ui.Container:
    return build_component_container(
        "잔액 송금 완료",
        fields=[
            ("송금 ID", str(data["transfer_id"])),
            ("받는 유저", recipient.mention),
            ("송금 금액", format_won(data["amount"])),
            ("내 잔액", format_won(data["new_sender_balance"])),
        ],
        accent_color=SUCCESS_ACCENT,
    )


def build_balance_withdrawal_result_container(data: dict) -> discord.ui.Container:
    return build_component_container(
        "잔액 출금 요청 완료",
        description="출금 요청이 접수되었어요. 관리자 확인 후 입력한 계좌로 수동 이체가 진행돼요.",
        fields=[
            ("출금 ID", str(data["withdrawal_id"])),
            ("출금 신청액", format_won(data["request_amount"])),
            ("수수료", f"{WITHDRAW_FEE_PERCENT}% ({format_won(data['fee_amount'])})"),
            ("실수령액", format_won(data["payout_amount"])),
            ("은행", data["bank_name"]),
            ("계좌번호", data["account_number"]),
            ("예금주", data["account_holder"]),
            ("남은 잔액", format_won(data["new_balance"])),
        ],
        accent_color=WARNING_ACCENT,
    )


def build_purchase_success_container(data: dict) -> discord.ui.Container:
    return build_component_container(
        "구매 완료",
        description="구매가 완료되었어요. 상품 내용을 개인메시지(DM)로 바로 전달했어요.",
        fields=[
            ("구매 ID", str(data["purchase_id"])),
            ("상품명", sanitize_plain_text(data["item_name"], max_length=80)),
            ("결제 금액", f"{data['price_paid']}원"),
            ("남은 잔액", f"{data['new_balance']}원"),
            ("남은 재고", f"{data['new_stock']}개"),
        ],
        accent_color=SUCCESS_ACCENT,
    )


def build_shop_view(items: list[sqlite3.Row]) -> discord.ui.LayoutView:
    view = discord.ui.LayoutView(timeout=300)
    view.add_item(
        build_component_container(
            "상점 목록",
            description="상품 이름을 확인한 뒤 `/buy`로 구매하거나, 마켓 패널의 `구매` 버튼을 사용하세요.",
            accent_color=MARKET_ACCENT,
        )
    )

    current_category = None
    lines: list[str] = []
    sections: list[tuple[str, str]] = []

    for item in items:
        line = (
            f"**{item['item_name']}** / "
            f"{item['price']}원 / 재고 {item['stock']}개 / 판매 {item['sales_count']}개"
        )
        if len(line) > 1000:
            line = line[:997] + "..."

        if current_category != item["category"]:
            if current_category is not None and lines:
                sections.append((current_category, "\n".join(lines)))
                if len(sections) >= 10:
                    break
            current_category = item["category"]
            lines = [line]
        else:
            candidate = "\n".join(lines + [line])
            if len(candidate) > 1000:
                sections.append((current_category, "\n".join(lines)))
                if len(sections) >= 10:
                    break
                lines = [line]
            else:
                lines.append(line)

    if current_category is not None and lines and len(sections) < 10:
        sections.append((current_category, "\n".join(lines)))

    for category, body in sections:
        view.add_item(
            build_component_container(
                fields=[(category, body)],
                accent_color=INFO_ACCENT,
            )
        )

    return view


# ============================================================
# Select / Modal
# ============================================================

class CategorySelect(discord.ui.Select):
    def __init__(self, guild_id: int, owner_id: int, mode: str, selected_category: str | None = None):
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.mode = mode

        categories = list_categories(guild_id)
        options = []

        if categories:
            for category in categories[:25]:
                options.append(discord.SelectOption(
                    label=category[:100],
                    value=category,
                    default=(category == selected_category),
                ))

        placeholder = "카테고리 선택"
        disabled = len(options) == 0

        if disabled:
            options = [discord.SelectOption(label="등록된 카테고리가 없어요", value="_none")]

        super().__init__(
            placeholder=placeholder,
            min_values=1,
            max_values=1,
            options=options,
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        category = self.values[0]
        if category == "_none":
            await interaction.response.send_message("등록된 카테고리가 없어요.", ephemeral=True)
            return

        if self.mode == "purchase":
            view = PurchaseView(self.guild_id, self.owner_id, selected_category=category)
            await interaction.response.edit_message(view=view)
        else:
            view = CatalogView(self.guild_id, self.owner_id, selected_category=category)
            await interaction.response.edit_message(view=view)


class PlaceholderProductSelect(discord.ui.Select):
    def __init__(self):
        super().__init__(
            placeholder="먼저 카테고리를 선택하세요",
            min_values=1,
            max_values=1,
            options=[discord.SelectOption(label="먼저 카테고리를 선택하세요", value="_placeholder")],
            disabled=True,
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message("먼저 카테고리를 선택하세요.", ephemeral=True)


class ProductSelect(discord.ui.Select):
    def __init__(self, guild_id: int, owner_id: int, category: str):
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.category = category

        items = list_items_by_category(guild_id, category)
        options = []

        if items:
            for item in items[:25]:
                desc = f"재고 {item['stock']}개 · 총 {item['sales_count']}개 판매"
                options.append(discord.SelectOption(
                    label=item["item_name"][:100],
                    description=desc[:100],
                    value=item["item_key"],
                ))

        disabled = len(options) == 0
        if disabled:
            options = [discord.SelectOption(label="등록된 상품이 없어요", value="_none")]

        super().__init__(
            placeholder="구매할 제품을 선택하세요",
            min_values=1,
            max_values=1,
            options=options,
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        item_key = self.values[0]
        if item_key == "_none":
            await interaction.response.send_message("등록된 상품이 없어요.", ephemeral=True)
            return

        item = get_item(self.guild_id, item_key)
        if item is None:
            await interaction.response.send_message("상품을 찾을 수 없어요.", ephemeral=True)
            return

        view = PurchaseConfirmView(self.guild_id, self.owner_id, self.category, item_key)
        await interaction.response.edit_message(view=view)


class ChargeRequestModal(discord.ui.Modal, title="충전 요청"):
    depositor = discord.ui.TextInput(
        label="입금자명",
        placeholder="예: 홍길동",
        max_length=30,
        required=True,
    )
    amount = discord.ui.TextInput(
        label="금액(원)",
        placeholder="예: 10000",
        max_length=10,
        required=True,
    )

    def __init__(self, guild_id: int, owner_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        depositor_name = self.depositor.value.strip()
        raw_amount = self.amount.value.strip().replace(",", "")

        if not raw_amount.isdigit():
            await interaction.response.send_message("금액은 숫자로만 입력해 주세요.", ephemeral=True)
            return

        amount = int(raw_amount)
        if amount < 1000 or amount > 100000000:
            await interaction.response.send_message(
                "금액은 1,000원 이상 100,000,000원 이하로 입력해 주세요.",
                ephemeral=True,
            )
            return

        settings = get_guild_settings(self.guild_id)
        if not settings["charge_log_channel_id"]:
            await interaction.response.send_message(
                "이 서버는 충전 승인 채널이 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
                ephemeral=True,
            )
            return

        if not is_bank_configured(self.guild_id):
            await interaction.response.send_message(
                "이 서버는 계좌 정보가 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
                ephemeral=True,
            )
            return

        request_id = create_charge_request(self.guild_id, interaction.user.id, amount, depositor_name)

        if interaction.guild is not None:
            await send_charge_log(
                interaction.guild,
                interaction.user.mention,
                request_id,
                amount,
                depositor_name,
            )

        await interaction.response.send_message(
            view=build_component_view(
                build_charge_result_container(self.guild_id, request_id, depositor_name, amount),
                timeout=300,
            ),
            ephemeral=True,
        )


class CoinTypeSelect(discord.ui.Select):
    def __init__(self, guild_id: int, owner_id: int, selected_coin: str | None = None):
        self.guild_id = guild_id
        self.owner_id = owner_id

        options = [
            discord.SelectOption(
                label=coin_symbol,
                value=coin_symbol,
                default=(coin_symbol == selected_coin),
            )
            for coin_symbol in list_configured_coins(guild_id)
        ]

        disabled = len(options) == 0
        if disabled:
            options = [discord.SelectOption(label="설정된 코인 주소가 없어요", value="_none")]

        super().__init__(
            placeholder="코인 종류 선택",
            min_values=1,
            max_values=1,
            options=options,
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        coin_symbol = self.values[0]
        if coin_symbol == "_none":
            await interaction.response.send_message("설정된 코인 주소가 없어요.", ephemeral=True)
            return

        configured_coins = list_configured_coins(self.guild_id)
        price_preview, kimchi_premium, preview_error = await get_coin_price_preview(configured_coins)

        await interaction.response.edit_message(
            view=CoinChargeRequestView(
                self.guild_id,
                self.owner_id,
                selected_coin=coin_symbol,
                price_preview=price_preview,
                preview_error=preview_error,
                kimchi_premium=kimchi_premium,
            ),
        )


class CoinChargeAmountModal(discord.ui.Modal, title="코인 충전 금액 입력"):
    usd_amount = discord.ui.TextInput(
        label="충전 금액(USD)",
        placeholder="예: 10",
        max_length=10,
        required=True,
    )

    def __init__(self, guild_id: int, owner_id: int, coin_symbol: str):
        super().__init__()
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.coin_symbol = coin_symbol

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        raw_amount = self.usd_amount.value.strip().replace(",", "")
        if not raw_amount.isdigit():
            await interaction.response.send_message("달러 금액은 숫자로만 입력해 주세요.", ephemeral=True)
            return

        amount = int(raw_amount)
        if amount < 1 or amount > 1000000:
            await interaction.response.send_message(
                "달러 금액은 1 이상 1,000,000 이하로 입력해 주세요.",
                ephemeral=True,
            )
            return

        settings = get_guild_settings(self.guild_id)
        if not settings["charge_log_channel_id"]:
            await interaction.response.send_message(
                "이 서버는 충전 승인 채널이 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
                ephemeral=True,
            )
            return

        if not is_coin_configured(self.guild_id, self.coin_symbol):
            await interaction.response.send_message(
                f"{self.coin_symbol} 입금 주소가 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
                ephemeral=True,
            )
            return

        try:
            quote = await get_coin_charge_quote(self.coin_symbol, amount)
        except app_commands.AppCommandError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return
        except Exception as e:
            print(f"[coin-charge] unexpected quote error: {e!r}")
            await interaction.response.send_message(
                "실시간 환율 또는 코인 시세를 불러오지 못했어요. 잠시 후 다시 시도해 주세요.",
                ephemeral=True,
            )
            return

        request_id = create_charge_request(
            self.guild_id,
            interaction.user.id,
            amount,
            charge_type="coin",
            coin_symbol=self.coin_symbol,
            credited_amount=quote["credited_amount"],
            usd_krw_rate=quote["usd_krw_rate"],
            coin_price_krw=quote["coin_price_krw"],
            coin_quantity=quote["coin_quantity_text"],
            status="awaiting_txid",
        )

        await interaction.response.send_message(
            view=CoinTxIdSubmitView(
                self.guild_id,
                request_id,
                interaction.user.id,
                quote,
            ),
            ephemeral=True,
        )


class CoinTxIdSubmitButton(discord.ui.Button):
    def __init__(self, guild_id: int, request_id: int, owner_id: int, coin_symbol: str):
        super().__init__(label="트랜잭션 ID 제출", style=discord.ButtonStyle.primary)
        self.guild_id = guild_id
        self.request_id = request_id
        self.owner_id = owner_id
        self.coin_symbol = coin_symbol

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        await interaction.response.send_modal(
            CoinTxIdModal(self.guild_id, self.request_id, self.owner_id, self.coin_symbol)
        )


class CoinTxIdModal(discord.ui.Modal, title="트랜잭션 ID 제출"):
    transaction_id = discord.ui.TextInput(
        label="트랜잭션 ID",
        placeholder="입금 후 받은 TXID를 입력해 주세요",
        max_length=150,
        required=True,
    )

    def __init__(self, guild_id: int, request_id: int, owner_id: int, coin_symbol: str):
        super().__init__()
        self.guild_id = guild_id
        self.request_id = request_id
        self.owner_id = owner_id
        self.coin_symbol = coin_symbol

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        success, message, row = submit_coin_transaction_id(
            self.request_id,
            interaction.user.id,
            self.transaction_id.value,
        )
        if not success or row is None:
            await interaction.response.send_message(message, ephemeral=True)
            return

        if interaction.guild is not None:
            await send_charge_log(
                interaction.guild,
                interaction.user.mention,
                row["id"],
                row["amount"],
                "",
                charge_type=row["charge_type"],
                coin_symbol=row["coin_symbol"],
                transaction_id=row["transaction_id"],
            )

        await interaction.response.send_message(
            view=build_component_view(
                build_coin_txid_submitted_container(
                    row["id"],
                    {
                        "coin_symbol": row["coin_symbol"],
                        "usd_amount": row["amount"],
                        "usd_krw_rate": float(row["usd_krw_rate"] or 0),
                        "credited_amount": int(row["credited_amount"] or 0) or row["amount"],
                        "coin_price_krw": float(row["coin_price_krw"] or 0),
                        "coin_price_usd": (
                            float(row["coin_price_krw"] or 0) / float(row["usd_krw_rate"])
                            if float(row["usd_krw_rate"] or 0) > 0
                            else 0.0
                        ),
                        "coin_quantity_text": row["coin_quantity"] or "-",
                    },
                    row["transaction_id"],
                ),
                timeout=300,
            ),
            ephemeral=True,
        )


class CoinChargeOpenAmountButton(discord.ui.Button):
    def __init__(self, guild_id: int, owner_id: int, selected_coin: str | None = None):
        super().__init__(
            label="달러 금액 입력",
            style=discord.ButtonStyle.primary,
            disabled=selected_coin is None,
        )
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.selected_coin = selected_coin

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        if self.selected_coin is None:
            await interaction.response.send_message("먼저 코인을 선택해 주세요.", ephemeral=True)
            return

        await interaction.response.send_modal(
            CoinChargeAmountModal(self.guild_id, self.owner_id, self.selected_coin)
        )


class CoinChargeBackButton(discord.ui.Button):
    def __init__(self, guild_id: int, owner_id: int):
        super().__init__(label="뒤로가기", style=discord.ButtonStyle.secondary)
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        await interaction.response.edit_message(view=ChargeMethodView(self.guild_id, self.owner_id))


class CoinChargeRequestView(discord.ui.LayoutView):
    def __init__(
        self,
        guild_id: int,
        owner_id: int,
        selected_coin: str | None = None,
        *,
        price_preview: dict[str, float] | None = None,
        preview_error: str | None = None,
        kimchi_premium: float = 0.0,
    ):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.owner_id = owner_id
        configured_coins = list_configured_coins(guild_id)

        self.add_item(build_coin_charge_select_container(
            configured_coins,
            selected_coin,
            price_preview,
            preview_error,
            kimchi_premium,
        ))
        self.add_item(build_action_row(CoinTypeSelect(guild_id, owner_id, selected_coin)))
        self.add_item(build_action_row(
            CoinChargeOpenAmountButton(guild_id, owner_id, selected_coin),
            CoinChargeBackButton(guild_id, owner_id),
        ))


class CoinTxIdSubmitView(discord.ui.LayoutView):
    def __init__(self, guild_id: int, request_id: int, owner_id: int, quote: dict):
        super().__init__(timeout=600)
        self.add_item(build_coin_charge_result_container(guild_id, request_id, quote))
        self.add_item(build_action_row(
            CoinTxIdSubmitButton(guild_id, request_id, owner_id, quote["coin_symbol"]),
        ))


# ============================================================
# View
# ============================================================

class MainMarketButton(discord.ui.Button):
    def __init__(self, action: str):
        config = {
            "buy": ("구매", "🛒"),
            "catalog": ("제품", "🔎"),
            "charge": ("충전", "🎁"),
            "info": ("정보", "⚙️"),
        }
        label, emoji = config[action]
        super().__init__(
            label=label,
            style=discord.ButtonStyle.secondary,
            custom_id=f"market:{action}",
            emoji=emoji,
        )
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("서버에서만 사용할 수 있어요.", ephemeral=True)
            return

        if self.action == "buy":
            view = PurchaseView(guild.id, interaction.user.id)
        elif self.action == "catalog":
            view = CatalogView(guild.id, interaction.user.id)
        elif self.action == "charge":
            view = ChargeMethodView(guild.id, interaction.user.id)
        else:
            view = InfoView(guild.id, interaction.user.id, interaction.user)

        await interaction.response.send_message(view=view, ephemeral=True)


class MainMarketView(discord.ui.LayoutView):
    def __init__(self, guild_id: int | None = None):
        super().__init__(timeout=None)

        if guild_id is not None:
            self.add_item(build_main_container(guild_id))

        self.add_item(build_action_row(
            MainMarketButton("buy"),
            MainMarketButton("catalog"),
            MainMarketButton("charge"),
            MainMarketButton("info"),
        ))


class PurchaseView(discord.ui.LayoutView):
    def __init__(self, guild_id: int, owner_id: int, selected_category: str | None = None):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.owner_id = owner_id

        self.add_item(build_purchase_container(selected_category))
        self.add_item(build_action_row(CategorySelect(guild_id, owner_id, "purchase", selected_category)))
        if selected_category:
            self.add_item(build_action_row(ProductSelect(guild_id, owner_id, selected_category)))
        else:
            self.add_item(build_action_row(PlaceholderProductSelect()))


class CatalogView(discord.ui.LayoutView):
    def __init__(self, guild_id: int, owner_id: int, selected_category: str | None = None):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.owner_id = owner_id

        self.add_item(build_catalog_container(guild_id, selected_category))
        self.add_item(build_action_row(CategorySelect(guild_id, owner_id, "catalog", selected_category)))


class PurchaseConfirmButton(discord.ui.Button):
    def __init__(self, guild_id: int, owner_id: int, category: str, item_key: str, action: str):
        config = {
            "confirm": ("구매하기", discord.ButtonStyle.primary, "🛍️"),
            "back": ("뒤로가기", discord.ButtonStyle.secondary, None),
        }
        label, style, emoji = config[action]
        super().__init__(label=label, style=style, emoji=emoji)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.category = category
        self.item_key = item_key
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        if self.action == "back":
            await interaction.response.edit_message(
                view=PurchaseView(self.guild_id, self.owner_id, self.category),
            )
            return

        success, message, data = create_purchase(self.guild_id, interaction.user.id, self.item_key)
        if not success or data is None:
            await interaction.response.send_message(message, ephemeral=True)
            return

        delivered, delivery_message = await finalize_purchase_delivery(interaction, data)
        if not delivered:
            await interaction.response.send_message(delivery_message, ephemeral=True)
            return

        await interaction.response.edit_message(
            view=build_component_view(build_purchase_success_container(data), timeout=300),
        )


class PurchaseConfirmView(discord.ui.LayoutView):
    def __init__(self, guild_id: int, owner_id: int, category: str, item_key: str):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.category = category
        self.item_key = item_key

        item = get_item(guild_id, item_key)
        if item is not None:
            self.add_item(build_item_detail_container(guild_id, owner_id, item))

        self.add_item(build_action_row(
            PurchaseConfirmButton(guild_id, owner_id, category, item_key, "confirm"),
            PurchaseConfirmButton(guild_id, owner_id, category, item_key, "back"),
        ))


class ChargeMethodButton(discord.ui.Button):
    def __init__(self, guild_id: int, owner_id: int, action: str):
        config = {
            "account": ("계좌이체(account)", discord.ButtonStyle.secondary),
            "coin": ("코인 충전(coin)", discord.ButtonStyle.secondary),
        }
        label, style = config[action]
        super().__init__(label=label, style=style)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        if self.action == "coin":
            settings = get_guild_settings(self.guild_id)

            if not settings["charge_log_channel_id"]:
                await interaction.response.send_message(
                    "이 서버는 충전 승인 채널이 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
                    ephemeral=True,
                )
                return

            configured_coins = list_configured_coins(self.guild_id)
            if not configured_coins:
                await interaction.response.send_message(
                    "설정된 코인 입금 주소가 아직 없어요. 관리자에게 문의해 주세요.",
                    ephemeral=True,
                )
                return

            price_preview, kimchi_premium, preview_error = await get_coin_price_preview(configured_coins)

            await interaction.response.edit_message(
                view=CoinChargeRequestView(
                    self.guild_id,
                    self.owner_id,
                    price_preview=price_preview,
                    preview_error=preview_error,
                    kimchi_premium=kimchi_premium,
                ),
            )
            return

        settings = get_guild_settings(self.guild_id)

        if not settings["charge_log_channel_id"]:
            await interaction.response.send_message(
                "이 서버는 충전 승인 채널이 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
                ephemeral=True,
            )
            return

        if not is_bank_configured(self.guild_id):
            await interaction.response.send_message(
                "이 서버는 계좌 정보가 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
                ephemeral=True,
            )
            return

        await interaction.response.send_modal(ChargeRequestModal(self.guild_id, self.owner_id))


class ChargeMethodView(discord.ui.LayoutView):
    def __init__(self, guild_id: int, owner_id: int):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.owner_id = owner_id

        self.add_item(build_charge_container())
        self.add_item(build_action_row(
            ChargeMethodButton(guild_id, owner_id, "account"),
            ChargeMethodButton(guild_id, owner_id, "coin"),
        ))


class RefreshInfoButton(discord.ui.Button):
    def __init__(self, guild_id: int, owner_id: int):
        super().__init__(label="새로고침", style=discord.ButtonStyle.primary)
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        await interaction.response.edit_message(
            view=InfoView(self.guild_id, self.owner_id, interaction.user),
        )


class BalanceTransferModal(discord.ui.Modal, title="잔액 송금"):
    target_user = discord.ui.TextInput(
        label="받는 유저",
        placeholder="@멘션 또는 유저 ID",
        max_length=30,
        required=True,
    )
    amount = discord.ui.TextInput(
        label="송금 금액(원)",
        placeholder="예: 10000",
        max_length=12,
        required=True,
    )

    def __init__(self, guild_id: int, owner_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("서버에서만 사용할 수 있어요.", ephemeral=True)
            return

        raw_amount = self.amount.value.strip().replace(",", "")
        if not raw_amount.isdigit():
            await interaction.response.send_message("송금 금액은 숫자로만 입력해 주세요.", ephemeral=True)
            return

        amount = int(raw_amount)
        recipient = await resolve_guild_member(guild, self.target_user.value)
        if recipient is None:
            await interaction.response.send_message(
                "받는 유저를 찾지 못했어요. @멘션 또는 유저 ID를 다시 확인해 주세요.",
                ephemeral=True,
            )
            return

        if recipient.bot:
            await interaction.response.send_message("봇에게는 송금할 수 없어요.", ephemeral=True)
            return

        success, message, data = transfer_balance_db(self.guild_id, interaction.user.id, recipient.id, amount)
        if not success or data is None:
            await interaction.response.send_message(message, ephemeral=True)
            return

        await interaction.response.send_message(
            view=build_component_view(
                build_balance_transfer_result_container(recipient, data),
                timeout=300,
            ),
            ephemeral=True,
        )

        await safe_notify_user(
            interaction.client,
            recipient.id,
            (
                f"{interaction.user.display_name}님이 {format_won(data['amount'])}을 송금했어요.\n"
                f"현재 잔액: {format_won(data['new_recipient_balance'])}"
            ),
        )


class BalanceWithdrawModal(discord.ui.Modal, title="잔액 출금"):
    bank_name = discord.ui.TextInput(
        label="은행명",
        placeholder="예: 국민은행",
        max_length=50,
        required=True,
    )
    account_number = discord.ui.TextInput(
        label="받을 계좌",
        placeholder="예: 123-456-789012",
        max_length=40,
        required=True,
    )
    account_holder = discord.ui.TextInput(
        label="예금주명",
        placeholder="예: 홍길동",
        max_length=50,
        required=True,
    )
    amount = discord.ui.TextInput(
        label="출금 신청액(원)",
        placeholder="예: 10000",
        max_length=12,
        required=True,
    )

    def __init__(self, guild_id: int, owner_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("서버에서만 사용할 수 있어요.", ephemeral=True)
            return

        settings = get_guild_settings(self.guild_id)
        if not settings["charge_log_channel_id"]:
            await interaction.response.send_message(
                "이 서버는 출금 요청을 받을 관리자 채널이 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
                ephemeral=True,
            )
            return

        raw_amount = self.amount.value.strip().replace(",", "")
        if not raw_amount.isdigit():
            await interaction.response.send_message("출금 금액은 숫자로만 입력해 주세요.", ephemeral=True)
            return

        request_amount = int(raw_amount)
        success, message, data = create_balance_withdrawal(
            self.guild_id,
            interaction.user.id,
            self.bank_name.value,
            self.account_number.value,
            self.account_holder.value,
            request_amount,
        )
        if not success or data is None:
            await interaction.response.send_message(message, ephemeral=True)
            return

        try:
            await send_balance_withdrawal_log(guild, interaction.user.mention, data)
        except Exception:
            pass

        await interaction.response.send_message(
            view=build_component_view(
                build_balance_withdrawal_result_container(data),
                timeout=300,
            ),
            ephemeral=True,
        )


class OpenBalanceTransferButton(discord.ui.Button):
    def __init__(self, guild_id: int, owner_id: int):
        super().__init__(label="잔액 송금", style=discord.ButtonStyle.success)
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        await interaction.response.send_modal(BalanceTransferModal(self.guild_id, self.owner_id))


class OpenBalanceWithdrawButton(discord.ui.Button):
    def __init__(self, guild_id: int, owner_id: int):
        super().__init__(label="잔액 출금", style=discord.ButtonStyle.secondary)
        self.guild_id = guild_id
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        await interaction.response.send_modal(BalanceWithdrawModal(self.guild_id, self.owner_id))


class InfoView(discord.ui.LayoutView):
    def __init__(self, guild_id: int, owner_id: int, user: discord.abc.User):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.owner_id = owner_id

        self.add_item(build_info_container(guild_id, user))
        settings = get_guild_settings(guild_id)
        review_url = settings["review_url"] or DEFAULT_REVIEW_URL
        row_items: list[discord.ui.Item] = [
            RefreshInfoButton(guild_id, owner_id),
            OpenBalanceTransferButton(guild_id, owner_id),
            OpenBalanceWithdrawButton(guild_id, owner_id),
        ]
        if review_url:
            row_items.append(
                discord.ui.Button(
                    label="후기",
                    style=discord.ButtonStyle.link,
                    url=review_url,
                )
            )
        self.add_item(build_action_row(*row_items))


class ChargeApprovalButton(discord.ui.Button):
    def __init__(self, action: str):
        config = {
            "approve": ("승인", discord.ButtonStyle.green, "✅"),
            "reject": ("거절", discord.ButtonStyle.danger, "❌"),
        }
        label, style, emoji = config[action]
        super().__init__(
            label=label,
            style=style,
            custom_id=f"charge:{action}",
            emoji=emoji,
        )
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        if not is_manager(interaction.user, interaction.guild):
            await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
            return

        request_id = extract_component_field_int(interaction.message, "요청 ID")
        if request_id is None:
            await interaction.response.send_message("요청 ID를 찾을 수 없어요.", ephemeral=True)
            return

        request_row = get_charge_request(request_id)
        if request_row is None:
            await interaction.response.send_message("충전 요청 정보를 찾을 수 없어요.", ephemeral=True)
            return

        charge_type = request_row["charge_type"]
        coin_symbol = request_row["coin_symbol"]
        transaction_id = request_row["transaction_id"]

        if self.action == "approve":
            success, message, approved_row = approve_charge_request(request_id, interaction.user.id)
            if not success or approved_row is None:
                await interaction.response.send_message(message, ephemeral=True)
                return

            user_id = approved_row["discord_user_id"]
            amount = approved_row["amount"]
            credited_amount = int(approved_row["credited_amount"] or 0) or amount

            await interaction.message.edit(
                view=ChargeApprovalView(
                    request_id=request_id,
                    user_mention=f"<@{user_id}>",
                    amount=amount,
                    credited_amount=credited_amount,
                    status="approved",
                    processor_mention=interaction.user.mention,
                    charge_type=charge_type,
                    coin_symbol=coin_symbol,
                    usd_krw_rate=float(approved_row["usd_krw_rate"] or 0),
                    coin_price_krw=float(approved_row["coin_price_krw"] or 0),
                    coin_quantity=approved_row["coin_quantity"] or "",
                    transaction_id=transaction_id,
                    include_actions=False,
                )
            )
            await interaction.response.send_message("승인 완료.", ephemeral=True)

            amount_text = format_charge_amount(amount, charge_type, credited_amount)
            await safe_notify_user(
                interaction.client,
                user_id,
                f"충전 요청이 승인되었어요. {amount_text} 충전이 잔액에 반영됐어요."
            )
            return

        success, message, user_id = reject_charge_request(request_id, interaction.user.id)
        if not success:
            await interaction.response.send_message(message, ephemeral=True)
            return

        await interaction.message.edit(
                view=ChargeApprovalView(
                    request_id=request_id,
                    user_mention=f"<@{user_id}>" if user_id is not None else "-",
                    status="rejected",
                    processor_mention=interaction.user.mention,
                    charge_type=charge_type,
                    coin_symbol=coin_symbol,
                    credited_amount=int(request_row["credited_amount"] or 0),
                    usd_krw_rate=float(request_row["usd_krw_rate"] or 0),
                    coin_price_krw=float(request_row["coin_price_krw"] or 0),
                    coin_quantity=request_row["coin_quantity"] or "",
                    transaction_id=transaction_id,
                    include_actions=False,
                )
            )
        await interaction.response.send_message("거절 완료.", ephemeral=True)

        if user_id is not None:
            await safe_notify_user(
                interaction.client,
                user_id,
                "충전 요청이 거절되었어요. 입금 내역을 다시 확인해 주세요."
            )


class ChargeApprovalView(discord.ui.LayoutView):
    def __init__(
        self,
        *,
        request_id: int | None = None,
        user_mention: str | None = None,
        depositor_name: str | None = None,
        amount: int | None = None,
        credited_amount: int | None = None,
        status: str = "pending",
        processor_mention: str | None = None,
        charge_type: str = "bank",
        coin_symbol: str = "",
        usd_krw_rate: float = 0.0,
        coin_price_krw: float = 0.0,
        coin_quantity: str = "",
        transaction_id: str = "",
        include_actions: bool = True,
    ):
        super().__init__(timeout=None if include_actions else 300)

        if request_id is not None:
            if charge_type == "coin":
                request_row = get_charge_request(request_id)
                if request_row is not None:
                    if amount is None:
                        amount = request_row["amount"]
                    if credited_amount is None:
                        credited_amount = int(request_row["credited_amount"] or 0)
                    if not coin_symbol:
                        coin_symbol = request_row["coin_symbol"] or ""
                    if not transaction_id:
                        transaction_id = request_row["transaction_id"] or ""
                    if usd_krw_rate <= 0:
                        usd_krw_rate = float(request_row["usd_krw_rate"] or 0)
                    if coin_price_krw <= 0:
                        coin_price_krw = float(request_row["coin_price_krw"] or 0)
                    if not coin_quantity:
                        coin_quantity = request_row["coin_quantity"] or ""

            amount_text = format_charge_amount(amount, charge_type, credited_amount) if amount is not None else "-"
            if status == "approved":
                fields = [
                    ("요청 ID", str(request_id)),
                    ("대상 유저", user_mention or "-"),
                    ("충전 방식", charge_type),
                ]
                if charge_type == "coin":
                    display_symbol = get_coin_display_symbol(coin_symbol)
                    fields.extend([
                        ("코인 종류", display_symbol or "-"),
                        ("충전 금액", format_usd(amount or 0)),
                        ("충전 반영 금액", format_won(credited_amount or 0)),
                        ("적용 환율", f"1 USD = {usd_krw_rate:,.2f}원" if usd_krw_rate > 0 else "-"),
                        ("현재 시세", f"1 {display_symbol} = {coin_price_krw:,.2f}원" if coin_price_krw > 0 else "-"),
                        ("예상 전송 수량", f"{coin_quantity} {display_symbol}" if coin_quantity else "-"),
                        ("트랜잭션 ID", transaction_id or "-"),
                    ])
                else:
                    fields.append(("입금자명", depositor_name or "-"))
                    fields.append(("충전 금액", amount_text))
                fields.extend([
                    ("처리자", processor_mention or "-"),
                    ("상태", "approved"),
                ])
                self.add_item(
                    build_component_container(
                        "충전 요청 승인됨",
                        description="충전이 완료되었어요.",
                        fields=fields,
                        accent_color=SUCCESS_ACCENT,
                    )
                )
            elif status == "rejected":
                fields = [
                    ("요청 ID", str(request_id)),
                    ("대상 유저", user_mention or "-"),
                    ("충전 방식", charge_type),
                ]
                if charge_type == "coin":
                    display_symbol = get_coin_display_symbol(coin_symbol)
                    fields.extend([
                        ("코인 종류", display_symbol or "-"),
                        ("충전 금액", format_usd(amount or 0)),
                        ("충전 반영 금액", format_won(credited_amount or 0) if credited_amount else "-"),
                        ("트랜잭션 ID", transaction_id or "-"),
                    ])
                else:
                    fields.append(("입금자명", depositor_name or "-"))
                    fields.append(("충전 금액", amount_text))
                fields.extend([
                    ("처리자", processor_mention or "-"),
                    ("상태", "rejected"),
                ])
                self.add_item(
                    build_component_container(
                        "충전 요청 거절됨",
                        description="충전 요청이 거절되었어요.",
                        fields=fields,
                        accent_color=DANGER_ACCENT,
                    )
                )
            else:
                fields = [
                    ("요청 ID", str(request_id)),
                    ("유저", user_mention or "-"),
                    ("충전 방식", charge_type),
                ]
                if charge_type == "coin":
                    display_symbol = get_coin_display_symbol(coin_symbol)
                    fields.extend([
                        ("코인 종류", display_symbol or "-"),
                        ("충전 금액", format_usd(amount or 0)),
                        ("충전 반영 금액", format_won(credited_amount or 0) if credited_amount else "-"),
                        ("적용 환율", f"1 USD = {usd_krw_rate:,.2f}원" if usd_krw_rate > 0 else "-"),
                        ("현재 시세", f"1 {display_symbol} = {coin_price_krw:,.2f}원" if coin_price_krw > 0 else "-"),
                        ("예상 전송 수량", f"{coin_quantity} {display_symbol}" if coin_quantity else "-"),
                        ("트랜잭션 ID", transaction_id or "-"),
                    ])
                else:
                    fields.append(("입금자명", depositor_name or "-"))
                fields.extend([
                    ("금액", amount_text),
                    ("상태", "pending"),
                ])
                self.add_item(
                    build_component_container(
                        "새 충전 요청",
                        description="실제 입금을 확인한 뒤 승인 또는 거절해 주세요.",
                        fields=fields,
                        accent_color=WARNING_ACCENT,
                    )
                )

        if include_actions:
            self.add_item(build_action_row(
                ChargeApprovalButton("approve"),
                ChargeApprovalButton("reject"),
            ))


class WithdrawalApprovalButton(discord.ui.Button):
    def __init__(self, action: str):
        config = {
            "approve": ("승인", discord.ButtonStyle.green, "✅"),
            "reject": ("거절", discord.ButtonStyle.danger, "❌"),
        }
        label, style, emoji = config[action]
        super().__init__(
            label=label,
            style=style,
            custom_id=f"withdrawal:{action}",
            emoji=emoji,
        )
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        if not is_manager(interaction.user, interaction.guild):
            await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
            return

        withdrawal_id = extract_component_field_int(interaction.message, "출금 ID")
        if withdrawal_id is None:
            await interaction.response.send_message("출금 ID를 찾을 수 없어요.", ephemeral=True)
            return

        row = get_withdrawal(withdrawal_id)
        if row is None:
            await interaction.response.send_message("출금 요청 정보를 찾을 수 없어요.", ephemeral=True)
            return

        if self.action == "approve":
            success, message, approved_row = approve_withdrawal(withdrawal_id, interaction.user.id)
            if not success or approved_row is None:
                await interaction.response.send_message(message, ephemeral=True)
                return

            user_id = approved_row["discord_user_id"]

            await interaction.message.edit(
                view=WithdrawalApprovalView(
                    withdrawal_id=withdrawal_id,
                    user_mention=f"<@{user_id}>",
                    request_amount=approved_row["request_amount"],
                    fee_amount=approved_row["fee_amount"],
                    payout_amount=approved_row["payout_amount"],
                    bank_name=approved_row["bank_name"],
                    account_number=approved_row["account_number"],
                    account_holder=approved_row["account_holder"],
                    status="approved",
                    processor_mention=interaction.user.mention,
                    include_actions=False,
                ),
            )
            await interaction.response.send_message("승인 완료.", ephemeral=True)

            await safe_notify_user(
                interaction.client,
                user_id,
                f"출금 요청이 승인되었어요. {format_won(approved_row['payout_amount'])}이 입력한 계좌로 이체될 예정이에요.",
            )
            return

        success, message, rejected_row = reject_withdrawal(withdrawal_id, interaction.user.id)
        if not success or rejected_row is None:
            await interaction.response.send_message(message, ephemeral=True)
            return

        user_id = rejected_row["discord_user_id"]

        await interaction.message.edit(
            view=WithdrawalApprovalView(
                withdrawal_id=withdrawal_id,
                user_mention=f"<@{user_id}>",
                request_amount=rejected_row["request_amount"],
                fee_amount=rejected_row["fee_amount"],
                payout_amount=rejected_row["payout_amount"],
                bank_name=rejected_row["bank_name"],
                account_number=rejected_row["account_number"],
                account_holder=rejected_row["account_holder"],
                status="rejected",
                processor_mention=interaction.user.mention,
                include_actions=False,
            ),
        )
        await interaction.response.send_message("거절 완료.", ephemeral=True)

        await safe_notify_user(
            interaction.client,
            user_id,
            f"출금 요청이 거절되어 {format_won(rejected_row['request_amount'])}이 잔액으로 복구되었어요.",
        )


class WithdrawalApprovalView(discord.ui.LayoutView):
    def __init__(
        self,
        *,
        withdrawal_id: int,
        user_mention: str = "-",
        request_amount: int = 0,
        fee_amount: int = 0,
        payout_amount: int = 0,
        bank_name: str = "",
        account_number: str = "",
        account_holder: str = "",
        status: str = "pending",
        processor_mention: str | None = None,
        include_actions: bool = True,
    ):
        super().__init__(timeout=None if include_actions else 300)

        fields = [
            ("유저", user_mention),
            ("출금 ID", str(withdrawal_id)),
            ("출금 신청액", format_won(request_amount)),
            ("수수료", f"{WITHDRAW_FEE_PERCENT}% ({format_won(fee_amount)})"),
            ("실수령액", format_won(payout_amount)),
            ("은행", bank_name),
            ("계좌번호", account_number),
            ("예금주", account_holder),
        ]

        if status == "approved":
            fields.append(("처리자", processor_mention or "-"))
            fields.append(("상태", "approved"))
            self.add_item(
                build_component_container(
                    "출금 요청 승인됨",
                    description="출금 요청이 승인되었어요.",
                    fields=fields,
                    accent_color=SUCCESS_ACCENT,
                )
            )
        elif status == "rejected":
            fields.append(("처리자", processor_mention or "-"))
            fields.append(("상태", "rejected"))
            self.add_item(
                build_component_container(
                    "출금 요청 거절됨",
                    description="출금 요청이 거절되었어요. 잔액이 복구되었습니다.",
                    fields=fields,
                    accent_color=DANGER_ACCENT,
                )
            )
        else:
            fields.append(("상태", "pending"))
            self.add_item(
                build_component_container(
                    "새 잔액 출금 요청",
                    description="유저가 잔액 출금을 신청했어요. 입력된 계좌로 수동 이체를 진행해 주세요.",
                    fields=fields,
                    accent_color=WARNING_ACCENT,
                )
            )

        if include_actions:
            self.add_item(build_action_row(
                WithdrawalApprovalButton("approve"),
                WithdrawalApprovalButton("reject"),
            ))


class PurchaseDeliveryButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="지급 완료",
            style=discord.ButtonStyle.green,
            custom_id="purchase:deliver",
            emoji="📦",
        )

    async def callback(self, interaction: discord.Interaction):
        if not is_manager(interaction.user, interaction.guild):
            await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
            return

        purchase_id = extract_component_field_int(interaction.message, "구매 ID")
        if purchase_id is None:
            await interaction.response.send_message("구매 ID를 찾을 수 없어요.", ephemeral=True)
            return

        success, message, user_id, item_name = mark_purchase_delivered(purchase_id, interaction.user.id)
        if not success:
            await interaction.response.send_message(message, ephemeral=True)
            return

        await interaction.message.edit(
            view=PurchaseDeliveryView(
                purchase_id=purchase_id,
                buyer_mention=f"<@{user_id}>" if user_id is not None else "-",
                item_name=item_name or "-",
                status="delivered",
                processor_mention=interaction.user.mention,
                include_actions=False,
            )
        )
        await interaction.response.send_message("지급 완료 처리했어요.", ephemeral=True)

        if user_id is not None and item_name is not None:
            await safe_notify_user(
                interaction.client,
                user_id,
                f"구매한 아이템 `{item_name}` 지급이 완료되었어요."
            )


class PurchaseDeliveryView(discord.ui.LayoutView):
    def __init__(
        self,
        *,
        purchase_id: int | None = None,
        buyer_mention: str | None = None,
        item_name: str | None = None,
        price_paid: int | None = None,
        status: str = "pending_delivery",
        processor_mention: str | None = None,
        include_actions: bool = True,
    ):
        super().__init__(timeout=None if include_actions else 300)

        if purchase_id is not None:
            if status == "delivered":
                self.add_item(
                    build_component_container(
                        "구매 지급 완료",
                        description="아이템 지급이 완료되었어요.",
                        fields=[
                            ("구매 ID", str(purchase_id)),
                            ("대상 유저", buyer_mention or "-"),
                            ("상품명", item_name or "-"),
                            ("처리자", processor_mention or "-"),
                            ("상태", "delivered"),
                        ],
                        accent_color=SUCCESS_ACCENT,
                    )
                )
            else:
                self.add_item(
                    build_component_container(
                        "새 구매 요청",
                        description="게임 아이템 지급 후 `지급 완료` 버튼을 눌러 주세요.",
                        fields=[
                            ("구매 ID", str(purchase_id)),
                            ("구매자", buyer_mention or "-"),
                            ("상품명", item_name or "-"),
                            ("결제 금액", f"{price_paid}원" if price_paid is not None else "-"),
                            ("상태", "pending_delivery"),
                        ],
                        accent_color=INFO_ACCENT,
                    )
                )

        if include_actions:
            self.add_item(build_action_row(PurchaseDeliveryButton()))


class TicketCreateButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="티켓 만들기",
            style=discord.ButtonStyle.green,
            custom_id="ticket:create",
            emoji="🎫",
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("서버에서만 사용할 수 있어요.", ephemeral=True)
            return

        limit_message = get_ticket_limit_message(interaction.guild, interaction.user.id)
        if limit_message is not None:
            await interaction.response.send_message(limit_message, ephemeral=True)
            return

        try:
            channel = await create_ticket_channel(interaction, "inquiry")
            await interaction.response.send_message(f"티켓을 만들었어요: {channel.mention}", ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message(
                "티켓 채널을 만들 권한이 없어요. 봇 권한을 확인해 주세요.",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(
                f"티켓 생성 중 오류가 발생했어요: {e}",
                ephemeral=True,
            )


class TicketPanelView(discord.ui.LayoutView):
    def __init__(self, include_panel: bool = False):
        super().__init__(timeout=None)

        if include_panel:
            self.add_item(
                build_component_container(
                    "고객지원 티켓",
                    description=(
                        "아래 버튼을 한 번 누르면 전용 티켓 채널이 바로 생성됩니다.\n"
                        "일반 유저도 사용할 수 있어요."
                    ),
                    fields=[("안내", f"한 사람당 열린 티켓은 최대 {MAX_OPEN_TICKETS_PER_USER}개까지 만들 수 있어요.")],
                    accent_color=INFO_ACCENT,
                )
            )

        rows: dict[int, list[discord.ui.Item]] = {}
        for label, issue_key, style, row in TICKET_ISSUE_TYPES:
            rows.setdefault(row, []).append(TicketIssueButton(label, issue_key, style, row))

        for row_index in sorted(rows):
            self.add_item(build_action_row(*rows[row_index]))


class TicketIssuePanelView(discord.ui.LayoutView):
    def __init__(self):
        super().__init__(timeout=None)

        rows: dict[int, list[discord.ui.Item]] = {}
        for label, issue_key, style, row in TICKET_ISSUE_TYPES:
            rows.setdefault(row, []).append(TicketIssueButton(label, issue_key, style, row))

        for row_index in sorted(rows):
            self.add_item(build_action_row(*rows[row_index]))


class TicketIssueButton(discord.ui.Button):
    def __init__(self, label: str, issue_key: str, style: discord.ButtonStyle, row: int):
        super().__init__(
            label=label,
            style=style,
            custom_id=f"ticket:create:{issue_key}",
            row=row,
        )
        self.issue_key = issue_key

    async def callback(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("서버에서만 사용할 수 있어요.", ephemeral=True)
            return

        limit_message = get_ticket_limit_message(interaction.guild, interaction.user.id)
        if limit_message is not None:
            await interaction.response.send_message(limit_message, ephemeral=True)
            return

        # 모달이 필요한 유형: 먼저 모달을 띄우고, 모달 제출 시 티켓 생성
        if self.issue_key == "purchase":
            await interaction.response.send_modal(PurchaseTicketModal())
            return

        if self.issue_key == "user_report":
            await interaction.response.send_modal(UserReportModal())
            return

        if self.issue_key == "auction":
            await interaction.response.send_modal(AuctionModal())
            return

        # 후원: 티켓 생성 후 계좌/코인 선택 버튼
        if self.issue_key == "support":
            try:
                await interaction.response.defer(ephemeral=True)
                channel = await create_ticket_channel(interaction, self.issue_key)
                await channel.send(
                    view=SupportMethodView(interaction.guild.id),
                )
                await interaction.followup.send(
                    f"티켓을 만들었어요: {channel.mention}",
                    ephemeral=True,
                )
            except discord.Forbidden:
                await interaction.followup.send(
                    "티켓 채널을 만들 권한이 없어요. 봇 권한을 확인해 주세요.",
                    ephemeral=True,
                )
            except Exception as e:
                await interaction.followup.send(
                    f"티켓 생성 중 오류가 발생했어요: {e}",
                    ephemeral=True,
                )
            return

        # 문의, 중개: 기본 동작 (티켓 생성 + 역할 멘션 + 커스텀 메시지)
        try:
            channel = await create_ticket_channel(interaction, self.issue_key)
            await interaction.response.send_message(
                f"티켓을 만들었어요: {channel.mention}",
                ephemeral=True,
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "티켓 채널을 만들 권한이 없어요. 봇 권한을 확인해 주세요.",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(
                f"티켓 생성 중 오류가 발생했어요: {e}",
                ephemeral=True,
            )


# -- 구매하기 모달 --

class PurchaseTicketModal(discord.ui.Modal, title="구매하기"):
    seller_name = discord.ui.TextInput(
        label="셀러명",
        placeholder="구매할 셀러의 이름을 입력해 주세요",
        max_length=50,
        required=True,
    )
    item_name = discord.ui.TextInput(
        label="물품명",
        placeholder="구매할 물품명을 입력해 주세요",
        max_length=100,
        required=True,
    )
    quantity = discord.ui.TextInput(
        label="수량",
        placeholder="예: 1",
        max_length=10,
        required=True,
    )

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("서버에서만 사용할 수 있어요.", ephemeral=True)
            return

        raw_qty = self.quantity.value.strip().replace(",", "")
        if not raw_qty.isdigit() or int(raw_qty) < 1:
            await interaction.response.send_message("수량은 1 이상의 숫자로 입력해 주세요.", ephemeral=True)
            return

        limit_message = get_ticket_limit_message(interaction.guild, interaction.user.id)
        if limit_message is not None:
            await interaction.response.send_message(limit_message, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            channel = await create_ticket_channel(interaction, "purchase")
        except discord.Forbidden:
            await interaction.followup.send(
                "티켓 채널을 만들 권한이 없어요. 봇 권한을 확인해 주세요.",
                ephemeral=True,
            )
            return
        except Exception as e:
            await interaction.followup.send(
                f"티켓 생성 중 오류가 발생했어요: {e}",
                ephemeral=True,
            )
            return

        seller = sanitize_plain_text(self.seller_name.value, max_length=50)
        item = sanitize_plain_text(self.item_name.value, max_length=100)
        qty = int(raw_qty)

        mention_role = get_ticket_issue_role(interaction.guild, "purchase") or get_ticket_staff_role(interaction.guild)
        mention_content = mention_role.mention if mention_role else None

        await send_component_view(
            channel,
            build_component_view(
                build_component_container(
                    "구매 요청",
                    description=f"{interaction.user.mention} 님이 구매를 요청했어요.",
                    fields=[
                        ("셀러", seller),
                        ("물품", item),
                        ("수량", f"{qty}개"),
                    ],
                    accent_color=SUCCESS_ACCENT,
                ),
                timeout=300,
            ),
            mention_text=mention_content,
            allowed_mentions=discord.AllowedMentions(roles=True),
        )

        await interaction.followup.send(
            f"티켓을 만들었어요: {channel.mention}",
            ephemeral=True,
        )


# -- 유저 신고 모달 --

class UserReportModal(discord.ui.Modal, title="유저 신고"):
    reported_user_id = discord.ui.TextInput(
        label="신고할 유저 ID",
        placeholder="유저 ID (숫자)를 입력해 주세요",
        max_length=20,
        required=True,
    )
    reported_nickname = discord.ui.TextInput(
        label="신고할 유저 닉네임",
        placeholder="유저 닉네임을 입력해 주세요",
        max_length=50,
        required=True,
    )

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("서버에서만 사용할 수 있어요.", ephemeral=True)
            return

        limit_message = get_ticket_limit_message(interaction.guild, interaction.user.id)
        if limit_message is not None:
            await interaction.response.send_message(limit_message, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            channel = await create_ticket_channel(interaction, "user_report")
        except discord.Forbidden:
            await interaction.followup.send(
                "티켓 채널을 만들 권한이 없어요. 봇 권한을 확인해 주세요.",
                ephemeral=True,
            )
            return
        except Exception as e:
            await interaction.followup.send(
                f"티켓 생성 중 오류가 발생했어요: {e}",
                ephemeral=True,
            )
            return

        user_id_val = sanitize_plain_text(self.reported_user_id.value, max_length=20)
        nickname_val = sanitize_plain_text(self.reported_nickname.value, max_length=50)

        await channel.send(
            view=build_component_view(
                build_component_container(
                    "유저 신고 접수",
                    description=f"{interaction.user.mention} 님이 유저를 신고했어요.",
                    fields=[
                        ("신고 대상 ID", user_id_val),
                        ("신고 대상 닉네임", nickname_val),
                    ],
                    accent_color=DANGER_ACCENT,
                ),
                timeout=300,
            ),
        )

        # 3분 이내 증거사진 안내
        await channel.send(
            view=build_component_view(
                build_component_container(
                    "📷 증거사진 안내",
                    description=(
                        "**3분 이내로 증거사진을 이 채널에 올려주세요.**\n"
                        "사진을 올리면 담당 스태프에게 자동으로 알림이 갑니다."
                    ),
                    accent_color=WARNING_ACCENT,
                ),
                timeout=300,
            ),
        )

        # 채널을 증거 대기 목록에 등록
        _pending_evidence_channels[channel.id] = {
            "user_id": interaction.user.id,
            "guild_id": interaction.guild.id,
            "issue_key": "user_report",
        }

        # 3분 후 자동 해제
        async def _remove_pending():
            await asyncio.sleep(180)
            _pending_evidence_channels.pop(channel.id, None)

        asyncio.ensure_future(_remove_pending())

        await interaction.followup.send(
            f"티켓을 만들었어요: {channel.mention}",
            ephemeral=True,
        )


# -- 경매 모달 --

class AuctionModal(discord.ui.Modal, title="경매 요청"):
    auction_item = discord.ui.TextInput(
        label="물품명",
        placeholder="경매에 올릴 물품명을 입력해 주세요",
        max_length=100,
        required=True,
    )
    base_price = discord.ui.TextInput(
        label="기본가",
        placeholder="예: 10000",
        max_length=15,
        required=True,
    )
    increment_price = discord.ui.TextInput(
        label="상승가",
        placeholder="예: 1000",
        max_length=15,
        required=True,
    )

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("서버에서만 사용할 수 있어요.", ephemeral=True)
            return

        raw_base = self.base_price.value.strip().replace(",", "")
        raw_inc = self.increment_price.value.strip().replace(",", "")

        if not raw_base.isdigit() or int(raw_base) < 1:
            await interaction.response.send_message("기본가는 1 이상의 숫자로 입력해 주세요.", ephemeral=True)
            return

        if not raw_inc.isdigit() or int(raw_inc) < 1:
            await interaction.response.send_message("상승가는 1 이상의 숫자로 입력해 주세요.", ephemeral=True)
            return

        limit_message = get_ticket_limit_message(interaction.guild, interaction.user.id)
        if limit_message is not None:
            await interaction.response.send_message(limit_message, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            channel = await create_ticket_channel(interaction, "auction")
        except discord.Forbidden:
            await interaction.followup.send(
                "티켓 채널을 만들 권한이 없어요. 봇 권한을 확인해 주세요.",
                ephemeral=True,
            )
            return
        except Exception as e:
            await interaction.followup.send(
                f"티켓 생성 중 오류가 발생했어요: {e}",
                ephemeral=True,
            )
            return

        item_name = sanitize_plain_text(self.auction_item.value, max_length=100)
        base = int(raw_base)
        inc = int(raw_inc)

        # 경매 채널에 경매 정보 전송
        auction_ch = get_auction_channel(interaction.guild)
        mention_role = get_ticket_issue_role(interaction.guild, "auction") or get_ticket_staff_role(interaction.guild)
        mention_content = mention_role.mention if mention_role else None

        auction_view = build_component_view(
            build_component_container(
                "🔨 새 경매 요청",
                description=f"{interaction.user.mention} 님이 경매를 요청했어요.",
                fields=[
                    ("물품", item_name),
                    ("기본가", f"{base:,}원"),
                    ("상승가", f"{inc:,}원"),
                ],
                accent_color=MARKET_ACCENT,
            ),
            timeout=300,
        )

        if auction_ch is not None:
            await send_component_view(
                auction_ch,
                auction_view,
                mention_text=mention_content,
                allowed_mentions=discord.AllowedMentions(roles=True),
            )
        else:
            # 경매 채널 미설정 시 티켓 채널에 전송
            await send_component_view(
                channel,
                auction_view,
                mention_text=mention_content,
                allowed_mentions=discord.AllowedMentions(roles=True),
            )

        # 티켓 채널에도 경매 내용 요약
        await channel.send(
            view=build_component_view(
                build_component_container(
                    "경매 요청 접수",
                    description="경매 요청이 접수되었어요." + (f" {auction_ch.mention} 채널에 등록되었어요." if auction_ch else " 경매 채널이 설정되지 않아 이 채널에 표시했어요."),
                    fields=[
                        ("물품", item_name),
                        ("기본가", f"{base:,}원"),
                        ("상승가", f"{inc:,}원"),
                    ],
                    accent_color=INFO_ACCENT,
                ),
                timeout=300,
            ),
        )

        await interaction.followup.send(
            f"티켓을 만들었어요: {channel.mention}",
            ephemeral=True,
        )


# -- 후원 방식 선택 뷰 --

class SupportMethodButton(discord.ui.Button):
    def __init__(self, guild_id: int, method: str):
        config = {
            "bank": ("계좌 후원", discord.ButtonStyle.primary, "🏦"),
            "coin": ("코인 후원", discord.ButtonStyle.primary, "🪙"),
        }
        label, style, emoji = config[method]
        super().__init__(label=label, style=style, emoji=emoji)
        self.guild_id = guild_id
        self.method = method

    async def callback(self, interaction: discord.Interaction):
        if self.method == "bank":
            settings = get_guild_settings(self.guild_id)
            bank_name = settings.get("bank_name", "").strip()
            account_number = settings.get("bank_account_number", "").strip()
            account_holder = settings.get("bank_account_holder", "").strip()

            if not bank_name or not account_number or not account_holder:
                await interaction.response.send_message(
                    "이 서버에 계좌 정보가 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
                    ephemeral=True,
                )
                return

            await interaction.response.send_message(
                view=build_component_view(
                    build_component_container(
                        "🏦 계좌 후원 안내",
                        description="아래 계좌로 후원해 주세요.",
                        fields=[
                            ("은행", bank_name),
                            ("계좌번호", account_number),
                            ("예금주", account_holder),
                        ],
                        accent_color=SUCCESS_ACCENT,
                    ),
                    timeout=300,
                ),
                ephemeral=True,
            )
        else:
            configured_coins = list_configured_coins(self.guild_id)
            if not configured_coins:
                await interaction.response.send_message(
                    "설정된 코인 입금 주소가 아직 없어요. 관리자에게 문의해 주세요.",
                    ephemeral=True,
                )
                return

            fields = []
            for coin in configured_coins:
                addr = get_coin_wallet_address(self.guild_id, coin)
                if addr:
                    fields.append((coin, addr))

            await interaction.response.send_message(
                view=build_component_view(
                    build_component_container(
                        "🪙 코인 후원 안내",
                        description="아래 주소로 후원해 주세요. 네트워크를 꼭 확인하세요.",
                        fields=fields,
                        accent_color=SUCCESS_ACCENT,
                    ),
                    timeout=300,
                ),
                ephemeral=True,
            )


class SupportMethodView(discord.ui.LayoutView):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.add_item(
            build_component_container(
                "후원 방식 선택",
                description="원하시는 후원 방식을 선택해 주세요.",
                accent_color=INFO_ACCENT,
            )
        )
        self.add_item(build_action_row(
            SupportMethodButton(guild_id, "bank"),
            SupportMethodButton(guild_id, "coin"),
        ))


class TicketActionButton(discord.ui.Button):
    def __init__(self, action: str):
        config = {
            "close": ("닫기", discord.ButtonStyle.secondary, "🔒"),
            "delete": ("삭제", discord.ButtonStyle.danger, "🗑️"),
        }
        label, style, emoji = config[action]
        super().__init__(
            label=label,
            style=style,
            custom_id=f"ticket:{action}",
            emoji=emoji,
        )
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        channel = interaction.channel

        if not isinstance(channel, discord.TextChannel) or not is_ticket_channel(channel):
            await interaction.response.send_message("티켓 채널에서만 사용할 수 있어요.", ephemeral=True)
            return

        if not can_manage_ticket(interaction.user, channel):
            await interaction.response.send_message(
                f"이 티켓을 {'닫을' if self.action == 'close' else '삭제할'} 권한이 없어요.",
                ephemeral=True,
            )
            return

        if self.action == "close":
            if is_ticket_closed(channel):
                await interaction.response.send_message("이미 닫힌 티켓이에요.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            try:
                await close_ticket_channel(channel, interaction.user)
                await channel.send("티켓이 닫혔어요. 필요하면 `/ticket_delete` 또는 삭제 버튼을 사용해 주세요.")
                await interaction.followup.send("티켓을 닫았어요.", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"오류가 발생했어요: {e}", ephemeral=True)
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("서버 멤버 정보가 필요해요.", ephemeral=True)
            return

        if not has_staff_access(interaction.user) and not is_ticket_closed(channel):
            await interaction.response.send_message("먼저 티켓을 닫은 뒤 삭제해 주세요.", ephemeral=True)
            return

        await interaction.response.send_message("티켓 채널을 삭제할게요.", ephemeral=True)

        try:
            await delete_ticket_channel(channel, interaction.user)
        except Exception:
            pass


class TicketControlsView(discord.ui.LayoutView):
    def __init__(
        self,
        owner_mention: str | None = None,
        issue_text: str | None = None,
    ):
        super().__init__(timeout=None)

        if owner_mention and issue_text:
            self.add_item(
                build_component_container(
                    "티켓이 생성되었어요",
                    description=f"{owner_mention} 님의 문의가 접수되었어요.",
                    fields=[("문의 유형", issue_text)],
                    footer="아래 버튼으로 티켓을 닫거나 삭제할 수 있어요.",
                    accent_color=INFO_ACCENT,
                )
            )

        self.add_item(build_action_row(
            TicketActionButton("close"),
            TicketActionButton("delete"),
        ))


# ============================================================
# 봇 본체
# ============================================================

class MyBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self._cleaned_guild_ids: set[int] = set()
        self._tree_synced = False

    async def setup_hook(self):
        init_db()

        self.add_view(MainMarketView())
        self.add_view(ChargeApprovalView())
        self.add_view(WithdrawalApprovalView(withdrawal_id=0))
        self.add_view(TicketPanelView())
        self.add_view(TicketControlsView())

    async def sync_global_commands(self):
        if self._tree_synced:
            return

        await self.tree.sync()
        self._tree_synced = True

    async def clear_guild_command_overrides(self, guild: discord.abc.Snowflake):
        self.tree.clear_commands(guild=guild)
        await self.tree.sync(guild=guild)
        self._cleaned_guild_ids.add(guild.id)


bot = MyBot()


@bot.event
async def on_ready():
    print(f"로그인 완료: {bot.user}")

    if not bot._tree_synced:
        try:
            await bot.sync_global_commands()
            print("글로벌 명령어 동기화 완료")
        except Exception as e:
            print(f"글로벌 명령어 동기화 실패: {e}")

    for guild in bot.guilds:
        if guild.id in bot._cleaned_guild_ids:
            continue

        try:
            await bot.clear_guild_command_overrides(guild)
            print(f"길드 기존 명령어 설정 정리 완료: {guild.name} ({guild.id})")
        except Exception as e:
            print(f"길드 기존 명령어 설정 정리 실패: {guild.name} ({guild.id}) - {e}")


@bot.event
async def on_guild_join(guild: discord.Guild):
    try:
        await bot.clear_guild_command_overrides(guild)
        print(f"새 길드 기존 명령어 설정 정리 완료: {guild.name} ({guild.id})")
    except Exception as e:
        print(f"새 길드 기존 명령어 설정 정리 실패: {guild.name} ({guild.id}) - {e}")


@bot.event
async def on_message(message: discord.Message):
    # 봇 메시지 무시
    if message.author.bot:
        return

    # 유저 신고 증거사진 감지
    if message.channel.id in _pending_evidence_channels:
        has_image = any(
            att.content_type and att.content_type.startswith("image/")
            for att in message.attachments
        )
        if not has_image:
            return

        info = _pending_evidence_channels.pop(message.channel.id, None)
        if info is None:
            return

        guild = message.guild
        if guild is None:
            return

        mention_role = get_ticket_issue_role(guild, info["issue_key"]) or get_ticket_staff_role(guild)
        mention_content = mention_role.mention if mention_role else None

        await send_component_view(
            message.channel,
            build_component_view(
                build_component_container(
                    "✅ 증거사진 접수 완료",
                    description="증거사진이 접수되었어요. 담당 스태프가 확인할 거예요.",
                    accent_color=SUCCESS_ACCENT,
                ),
                timeout=300,
            ),
            mention_text=mention_content,
            allowed_mentions=discord.AllowedMentions(roles=True),
        )


# ============================================================
# 관리 명령어
# ============================================================

@bot.tree.command(name="kick", description="유저를 서버에서 추방합니다.")
@app_commands.describe(member="추방할 유저", reason="사유")
async def kick(interaction: discord.Interaction, member: discord.Member, reason: str | None = None):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("서버 멤버 정보가 필요해요.", ephemeral=True)
        return

    if not interaction.user.guild_permissions.kick_members:
        await interaction.response.send_message(
            "이 명령어를 사용할 권한이 없어요. `Kick Members` 권한이 필요합니다.",
            ephemeral=True,
        )
        return

    bot_member = interaction.guild.me
    if bot_member is None or not bot_member.guild_permissions.kick_members:
        await interaction.response.send_message("봇에 `Kick Members` 권한이 없어요.", ephemeral=True)
        return

    ok, error_message = can_act_on_member(interaction.user, member, interaction.guild)
    if not ok:
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    try:
        await member.kick(reason=reason or f"By {interaction.user}")
        await interaction.response.send_message(f"{member.mention} 님을 추방했어요. 사유: {reason or '없음'}")
    except discord.Forbidden:
        await interaction.response.send_message("역할 순서 또는 권한 문제로 추방하지 못했어요.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"오류가 발생했어요: {e}", ephemeral=True)


@bot.tree.command(name="ban", description="유저를 서버에서 차단합니다.")
@app_commands.describe(member="차단할 유저", reason="사유")
async def ban(interaction: discord.Interaction, member: discord.Member, reason: str | None = None):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("서버 멤버 정보가 필요해요.", ephemeral=True)
        return

    if not interaction.user.guild_permissions.ban_members:
        await interaction.response.send_message(
            "이 명령어를 사용할 권한이 없어요. `Ban Members` 권한이 필요합니다.",
            ephemeral=True,
        )
        return

    bot_member = interaction.guild.me
    if bot_member is None or not bot_member.guild_permissions.ban_members:
        await interaction.response.send_message("봇에 `Ban Members` 권한이 없어요.", ephemeral=True)
        return

    ok, error_message = can_act_on_member(interaction.user, member, interaction.guild)
    if not ok:
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    try:
        await interaction.guild.ban(member, reason=reason or f"By {interaction.user}")
        await interaction.response.send_message(f"{member.mention} 님을 차단했어요. 사유: {reason or '없음'}")
    except discord.Forbidden:
        await interaction.response.send_message("역할 순서 또는 권한 문제로 차단하지 못했어요.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"오류가 발생했어요: {e}", ephemeral=True)


@bot.tree.command(name="unban", description="유저의 서버 차단을 해제합니다.")
@app_commands.describe(user_id="차단 해제할 유저 ID", reason="사유")
async def unban(interaction: discord.Interaction, user_id: str, reason: str | None = None):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("서버 멤버 정보가 필요해요.", ephemeral=True)
        return

    if not interaction.user.guild_permissions.ban_members:
        await interaction.response.send_message(
            "이 명령어를 사용할 권한이 없어요. `Ban Members` 권한이 필요합니다.",
            ephemeral=True,
        )
        return

    bot_member = interaction.guild.me
    if bot_member is None or not bot_member.guild_permissions.ban_members:
        await interaction.response.send_message("봇에 `Ban Members` 권한이 없어요.", ephemeral=True)
        return

    clean_user_id = user_id.strip()
    if not re.fullmatch(r"\d{17,20}", clean_user_id):
        await interaction.response.send_message("유효한 디스코드 유저 ID를 입력해 주세요.", ephemeral=True)
        return

    target = discord.Object(id=int(clean_user_id))

    try:
        ban_entry = await interaction.guild.fetch_ban(target)
    except discord.NotFound:
        await interaction.response.send_message("해당 유저는 현재 차단 목록에 없어요.", ephemeral=True)
        return
    except discord.Forbidden:
        await interaction.response.send_message("차단 목록을 확인할 권한이 없어요.", ephemeral=True)
        return
    except Exception as e:
        await interaction.response.send_message(f"차단 정보를 확인하는 중 오류가 발생했어요: {e}", ephemeral=True)
        return

    try:
        await interaction.guild.unban(ban_entry.user, reason=reason or f"By {interaction.user}")
        await interaction.response.send_message(
            f"{ban_entry.user} 님의 차단을 해제했어요. 사유: {reason or '없음'}"
        )
    except discord.Forbidden:
        await interaction.response.send_message("권한 문제로 차단을 해제하지 못했어요.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"오류가 발생했어요: {e}", ephemeral=True)


@bot.tree.command(name="clear", description="최근 메시지를 삭제합니다.")
@app_commands.describe(count="삭제할 메시지 수(1~100)")
async def clear(interaction: discord.Interaction, count: app_commands.Range[int, 1, 100]):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("서버 멤버 정보가 필요해요.", ephemeral=True)
        return

    if not interaction.user.guild_permissions.manage_messages:
        await interaction.response.send_message(
            "이 명령어를 사용할 권한이 없어요. `Manage Messages` 권한이 필요합니다.",
            ephemeral=True,
        )
        return

    bot_member = interaction.guild.me
    if bot_member is None or not bot_member.guild_permissions.manage_messages:
        await interaction.response.send_message("봇에 `Manage Messages` 권한이 없어요.", ephemeral=True)
        return

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message("텍스트 채널에서만 사용할 수 있어요.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        deleted = await channel.purge(limit=count)
        await interaction.followup.send(f"{len(deleted)}개의 메시지를 삭제했어요.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"오류가 발생했어요: {e}", ephemeral=True)


@bot.tree.command(name="timeout", description="유저를 일정 시간 동안 타임아웃합니다.")
@app_commands.describe(member="타임아웃할 유저", minutes="타임아웃 시간(분, 최대 40320)", reason="사유")
async def timeout_member(
    interaction: discord.Interaction,
    member: discord.Member,
    minutes: app_commands.Range[int, 1, 40320],
    reason: str | None = None,
):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("서버 멤버 정보가 필요해요.", ephemeral=True)
        return

    if not interaction.user.guild_permissions.moderate_members:
        await interaction.response.send_message(
            "이 명령어를 사용할 권한이 없어요. `Moderate Members` 권한이 필요합니다.",
            ephemeral=True,
        )
        return

    bot_member = interaction.guild.me
    if bot_member is None or not bot_member.guild_permissions.moderate_members:
        await interaction.response.send_message("봇에 `Moderate Members` 권한이 없어요.", ephemeral=True)
        return

    ok, error_message = can_act_on_member(interaction.user, member, interaction.guild)
    if not ok:
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)

    try:
        await member.edit(timed_out_until=until, reason=reason or f"Timed out by {interaction.user}")
        await interaction.response.send_message(
            f"{member.mention} 님을 타임아웃했어요.\n"
            f"해제 시각: <t:{int(until.timestamp())}:F>\n"
            f"사유: {reason or '없음'}"
        )
    except discord.Forbidden:
        await interaction.response.send_message("권한 또는 역할 순서 문제로 타임아웃하지 못했어요.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"오류가 발생했어요: {e}", ephemeral=True)


@bot.tree.command(name="untimeout", description="유저의 타임아웃을 해제합니다.")
@app_commands.describe(member="타임아웃 해제할 유저", reason="사유")
async def untimeout_member(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str | None = None,
):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("서버 멤버 정보가 필요해요.", ephemeral=True)
        return

    if not interaction.user.guild_permissions.moderate_members:
        await interaction.response.send_message(
            "이 명령어를 사용할 권한이 없어요. `Moderate Members` 권한이 필요합니다.",
            ephemeral=True,
        )
        return

    bot_member = interaction.guild.me
    if bot_member is None or not bot_member.guild_permissions.moderate_members:
        await interaction.response.send_message("봇에 `Moderate Members` 권한이 없어요.", ephemeral=True)
        return

    ok, error_message = can_act_on_member(interaction.user, member, interaction.guild)
    if not ok:
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    try:
        await member.edit(timed_out_until=None, reason=reason or f"Timeout removed by {interaction.user}")
        await interaction.response.send_message(
            f"{member.mention} 님의 타임아웃을 해제했어요.\n"
            f"사유: {reason or '없음'}"
        )
    except discord.Forbidden:
        await interaction.response.send_message("권한 또는 역할 순서 문제로 타임아웃을 해제하지 못했어요.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"오류가 발생했어요: {e}", ephemeral=True)


# ============================================================
# AI
# ============================================================

@bot.tree.command(name="ask", description="AI에게 질문합니다.")
@app_commands.describe(question="질문 내용")
async def ask(interaction: discord.Interaction, question: str):
    if gemini_client is None:
        await interaction.response.send_message("GEMINI_API_KEY가 설정되지 않았어요.", ephemeral=True)
        return

    clean_question = sanitize_plain_text(question, max_length=2000, multiline=True)
    if len(clean_question) < 3:
        await interaction.response.send_message("질문은 3자 이상 입력해 주세요.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=clean_question,
            config=types.GenerateContentConfig(
                system_instruction=(
                    "너는 디스코드 서버용 도우미 봇이다. "
                    "답변은 한국어로 하고, 너무 길면 핵심 위주로 정리해라."
                )
            ),
        )

        try:
            answer = (response.text or "").strip()
        except Exception:
            answer = ""

        if not answer:
            answer = "응답이 비어 있어요."

        answer = sanitize_plain_text(answer, max_length=6000, multiline=True)

        if len(answer) <= 1900:
            await interaction.followup.send(answer, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        else:
            for i in range(0, len(answer), 1900):
                await interaction.followup.send(
                    answer[i:i + 1900],
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )

    except Exception as e:
        await interaction.followup.send(f"Gemini 호출 중 오류가 발생했어요: {e}", ephemeral=True)


# ============================================================
# 티켓 명령어
# ============================================================

@bot.tree.command(name="ticket_panel", description="현재 채널에 티켓 생성 패널을 보냅니다.")
async def ticket_panel(interaction: discord.Interaction):
    ensure_guild(interaction)

    if not has_staff_access(interaction.user):
        await interaction.response.send_message(
            "이 명령어는 관리자 또는 티켓 스태프만 사용할 수 있어요.",
            ephemeral=True,
        )
        return

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message("텍스트 채널에서만 사용할 수 있어요.", ephemeral=True)
        return

    await channel.send(view=TicketPanelView(include_panel=True))
    await interaction.response.send_message("티켓 패널을 올렸어요.", ephemeral=True)


@bot.tree.command(name="ticket_close", description="현재 티켓 채널을 닫습니다.")
async def ticket_close(interaction: discord.Interaction):
    ensure_guild(interaction)

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel) or not is_ticket_channel(channel):
        await interaction.response.send_message("티켓 채널에서만 사용할 수 있어요.", ephemeral=True)
        return

    if not can_manage_ticket(interaction.user, channel):
        await interaction.response.send_message("이 티켓을 닫을 권한이 없어요.", ephemeral=True)
        return

    if is_ticket_closed(channel):
        await interaction.response.send_message("이미 닫힌 티켓이에요.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        await close_ticket_channel(channel, interaction.user)
        await channel.send("티켓이 닫혔어요. 필요하면 `/ticket_delete` 또는 삭제 버튼을 사용해 주세요.")
        await interaction.followup.send("티켓을 닫았어요.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"오류가 발생했어요: {e}", ephemeral=True)


@bot.tree.command(name="ticket_delete", description="현재 티켓 채널을 삭제합니다.")
async def ticket_delete(interaction: discord.Interaction):
    ensure_guild(interaction)

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel) or not is_ticket_channel(channel):
        await interaction.response.send_message("티켓 채널에서만 사용할 수 있어요.", ephemeral=True)
        return

    if not can_manage_ticket(interaction.user, channel):
        await interaction.response.send_message("이 티켓을 삭제할 권한이 없어요.", ephemeral=True)
        return

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("서버 멤버 정보가 필요해요.", ephemeral=True)
        return

    if not has_staff_access(interaction.user) and not is_ticket_closed(channel):
        await interaction.response.send_message("먼저 티켓을 닫은 뒤 삭제해 주세요.", ephemeral=True)
        return

    await interaction.response.send_message("티켓 채널을 삭제할게요.", ephemeral=True)

    try:
        await delete_ticket_channel(channel, interaction.user)
    except Exception:
        pass


@bot.tree.command(name="ticket_add", description="현재 티켓에 유저를 추가합니다.")
@app_commands.describe(member="추가할 유저")
async def ticket_add(interaction: discord.Interaction, member: discord.Member):
    ensure_guild(interaction)

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel) or not is_ticket_channel(channel):
        await interaction.response.send_message("티켓 채널에서만 사용할 수 있어요.", ephemeral=True)
        return

    if not can_manage_ticket(interaction.user, channel):
        await interaction.response.send_message("이 티켓에 유저를 추가할 권한이 없어요.", ephemeral=True)
        return

    try:
        await channel.set_permissions(
            member,
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
            reason=f"Added to ticket by {interaction.user}",
        )
        await interaction.response.send_message(f"{member.mention} 님을 티켓에 추가했어요.")
    except Exception as e:
        await interaction.response.send_message(f"오류가 발생했어요: {e}", ephemeral=True)


@bot.tree.command(name="ticket_remove", description="현재 티켓에서 유저를 제거합니다.")
@app_commands.describe(member="제거할 유저")
async def ticket_remove(interaction: discord.Interaction, member: discord.Member):
    ensure_guild(interaction)

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel) or not is_ticket_channel(channel):
        await interaction.response.send_message("티켓 채널에서만 사용할 수 있어요.", ephemeral=True)
        return

    if not can_manage_ticket(interaction.user, channel):
        await interaction.response.send_message("이 티켓에서 유저를 제거할 권한이 없어요.", ephemeral=True)
        return

    owner_id = get_ticket_owner_id(channel)
    if owner_id == member.id:
        await interaction.response.send_message("티켓 생성자는 제거할 수 없어요.", ephemeral=True)
        return

    try:
        await channel.set_permissions(
            member,
            overwrite=None,
            reason=f"Removed from ticket by {interaction.user}",
        )
        await interaction.response.send_message(f"{member.mention} 님을 티켓에서 제거했어요.")
    except Exception as e:
        await interaction.response.send_message(f"오류가 발생했어요: {e}", ephemeral=True)


# ============================================================
# 서버 설정 명령어
# ============================================================

@bot.tree.command(name="setup_charge_channel", description="이 서버의 충전 승인 채널을 설정합니다.")
@app_commands.describe(channel="충전 승인 로그를 받을 채널")
async def setup_charge_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    update_guild_settings(interaction.guild.id, charge_log_channel_id=channel.id)
    await interaction.response.send_message(f"충전 승인 채널을 {channel.mention} 으로 설정했어요.")


@bot.tree.command(name="setup_purchase_channel", description="이 서버의 구매 로그 채널을 설정합니다.")
@app_commands.describe(channel="구매 로그를 받을 채널")
async def setup_purchase_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    update_guild_settings(interaction.guild.id, purchase_log_channel_id=channel.id)
    await interaction.response.send_message(f"구매 로그 채널을 {channel.mention} 으로 설정했어요.")


@bot.tree.command(name="setup_bank", description="이 서버의 입금 계좌 정보를 설정합니다.")
@app_commands.describe(bank_name="은행명", account_number="계좌번호", account_holder="예금주명")
async def setup_bank(
    interaction: discord.Interaction,
    bank_name: str,
    account_number: str,
    account_holder: str,
):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    clean_bank_name = sanitize_plain_text(bank_name, max_length=50)
    clean_account_number = normalize_money_account(account_number, max_length=40)
    clean_account_holder = sanitize_plain_text(account_holder, max_length=50)

    if not clean_bank_name or not clean_account_number or not clean_account_holder:
        await interaction.response.send_message("은행명, 계좌번호, 예금주는 비워둘 수 없어요.", ephemeral=True)
        return

    update_guild_settings(
        interaction.guild.id,
        bank_name=clean_bank_name,
        bank_account_number=clean_account_number,
        bank_account_holder=clean_account_holder,
    )

    await interaction.response.send_message("이 서버의 계좌 정보를 설정했어요.")


@bot.tree.command(name="setup_coin_wallet", description="이 서버의 코인 입금 주소를 설정합니다.")
@app_commands.describe(coin="코인 종류", address="입금 받을 지갑 주소")
@app_commands.choices(coin=COIN_CHOICES)
async def setup_coin_wallet(
    interaction: discord.Interaction,
    coin: app_commands.Choice[str],
    address: str,
):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    column_name = COIN_WALLET_COLUMNS.get(coin.value)
    if column_name is None:
        await interaction.response.send_message("지원하지 않는 코인 종류예요.", ephemeral=True)
        return

    clean_address = normalize_wallet_address(address)
    if not clean_address:
        await interaction.response.send_message("지갑 주소를 비워둘 수 없어요.", ephemeral=True)
        return

    update_guild_settings(interaction.guild.id, **{column_name: clean_address})
    await interaction.response.send_message(f"{coin.value} 입금 주소를 설정했어요.")


@bot.tree.command(name="setup_market", description="이 서버의 마켓 제목과 후기 링크를 설정합니다.")
@app_commands.describe(title="마켓 제목", review_url="후기 링크(없으면 비워두세요)")
async def setup_market(
    interaction: discord.Interaction,
    title: str,
    review_url: str | None = None,
):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    clean_title = sanitize_plain_text(title, max_length=80)
    if not clean_title:
        await interaction.response.send_message("마켓 제목을 비워둘 수 없어요.", ephemeral=True)
        return

    update_guild_settings(
        interaction.guild.id,
        market_title=clean_title,
        review_url=normalize_review_url(review_url),
    )

    await interaction.response.send_message("이 서버의 마켓 UI 설정을 저장했어요.")


@bot.tree.command(name="setup_ticket_role", description="이 서버의 티켓 스태프 역할을 설정합니다.")
@app_commands.describe(role="티켓을 자동으로 볼 스태프 역할")
async def setup_ticket_role(interaction: discord.Interaction, role: discord.Role):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    update_guild_settings(interaction.guild.id, ticket_staff_role_id=role.id)
    await interaction.response.send_message(f"티켓 스태프 역할을 {role.mention} 로 설정했어요.")


@bot.tree.command(name="setup_ticket_category", description="이 서버의 티켓 카테고리 이름을 설정합니다.")
@app_commands.describe(name="티켓 카테고리 이름")
async def setup_ticket_category(interaction: discord.Interaction, name: str):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    clean_name = sanitize_plain_text(name, max_length=80)
    if not clean_name:
        await interaction.response.send_message("티켓 카테고리 이름을 비워둘 수 없어요.", ephemeral=True)
        return

    update_guild_settings(interaction.guild.id, ticket_category_name=clean_name)
    await interaction.response.send_message(f"티켓 카테고리 이름을 `{name.strip()}` 으로 설정했어요.")


@bot.tree.command(name="setup_auction_channel", description="이 서버의 경매 요청 채널을 설정합니다.")
@app_commands.describe(channel="경매 요청이 올라갈 채널")
async def setup_auction_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    update_guild_settings(interaction.guild.id, auction_channel_id=channel.id)
    await interaction.response.send_message(f"경매 요청 채널을 {channel.mention} 으로 설정했어요.")


@bot.tree.command(name="setup_ticket_message", description="티켓 생성 시 표시할 커스텀 안내 메시지를 설정합니다.")
@app_commands.describe(issue_type="티켓 유형", message="안내 메시지 텍스트")
@app_commands.choices(issue_type=TICKET_ISSUE_CHOICES)
async def setup_ticket_message(interaction: discord.Interaction, issue_type: app_commands.Choice[str], message: str):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    clean_message = message.strip()
    column_name = TICKET_ISSUE_MESSAGE_COLUMNS.get(issue_type.value)
    if not column_name:
        await interaction.response.send_message("알 수 없는 티켓 유형이에요.", ephemeral=True)
        return

    update_guild_settings(interaction.guild.id, **{column_name: clean_message})
    await interaction.response.send_message(f"{issue_type.name} 티켓의 안내 메시지를 설정했어요:\n{clean_message}")


@bot.tree.command(name="setup_ticket_issue_role", description="특정 티켓 유형에 멘션할 역할을 설정합니다.")
@app_commands.describe(issue_type="티켓 유형", role="멘션할 역할")
@app_commands.choices(issue_type=TICKET_ISSUE_CHOICES)
async def setup_ticket_issue_role(interaction: discord.Interaction, issue_type: app_commands.Choice[str], role: discord.Role):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    column_name = TICKET_ISSUE_ROLE_COLUMNS.get(issue_type.value)
    if not column_name:
        await interaction.response.send_message("알 수 없는 티켓 유형이에요.", ephemeral=True)
        return

    update_guild_settings(interaction.guild.id, **{column_name: role.id})
    await interaction.response.send_message(f"{issue_type.name} 티켓의 멘션 역할을 {role.mention} 로 설정했어요.")


@bot.tree.command(name="show_server_settings", description="이 서버의 현재 설정을 확인합니다.")
async def show_server_settings(interaction: discord.Interaction):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    settings = get_guild_settings(interaction.guild.id)

    await interaction.response.send_message(
        view=build_component_view(
            build_component_container(
                "현재 서버 설정",
                fields=[
                    ("충전 승인 채널", f"<#{settings['charge_log_channel_id']}>" if settings["charge_log_channel_id"] else "미설정"),
                    ("구매 로그 채널", f"<#{settings['purchase_log_channel_id']}>" if settings["purchase_log_channel_id"] else "미설정"),
                    ("은행", settings["bank_name"] or "미설정"),
                    ("계좌번호", settings["bank_account_number"] or "미설정"),
                    ("예금주", settings["bank_account_holder"] or "미설정"),
                    ("티켓 스태프 역할", f"<@&{settings['ticket_staff_role_id']}>" if settings["ticket_staff_role_id"] else "미설정"),
                    ("티켓 카테고리", settings["ticket_category_name"] or "미설정"),
                    ("마켓 제목", settings["market_title"] or "미설정"),
                    ("후기 링크", settings["review_url"] or "미설정"),
                    ("경매 요청 채널", f"<#{settings['auction_channel_id']}>" if settings["auction_channel_id"] else "미설정"),
                ],
                accent_color=INFO_ACCENT,
            ),
            build_component_container(
                "티켓 유형별 멘션 역할",
                fields=[
                    ("문의", f"<@&{settings['ticket_inquiry_role_id']}>" if settings["ticket_inquiry_role_id"] else "미설정"),
                    ("중개", f"<@&{settings['ticket_brokerage_role_id']}>" if settings["ticket_brokerage_role_id"] else "미설정"),
                    ("경매", f"<@&{settings['ticket_auction_role_id']}>" if settings["ticket_auction_role_id"] else "미설정"),
                    ("후원", f"<@&{settings['ticket_support_role_id']}>" if settings["ticket_support_role_id"] else "미설정"),
                    ("유저 신고", f"<@&{settings['ticket_user_report_role_id']}>" if settings["ticket_user_report_role_id"] else "미설정"),
                    ("구매하기", f"<@&{settings['ticket_purchase_role_id']}>" if settings["ticket_purchase_role_id"] else "미설정"),
                ],
                accent_color=INFO_ACCENT,
            ),
            build_component_container(
                "코인 입금 주소",
                fields=[
                    ("LTC 주소", settings["coin_ltc_wallet"] or "미설정"),
                    ("USDT 주소", settings["coin_usdt_wallet"] or "미설정"),
                    ("Tron 주소", settings["coin_tron_wallet"] or "미설정"),
                    ("Bitcoin 주소", settings["coin_bitcoin_wallet"] or "미설정"),
                ],
                accent_color=INFO_ACCENT,
            ),
            timeout=300,
        ),
        ephemeral=True,
    )


# ============================================================
# 마켓 / 충전 / 잔액
# ============================================================

@bot.tree.command(name="market_panel", description="마켓 메인 패널을 현재 채널에 올립니다.")
async def market_panel(interaction: discord.Interaction):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("이 명령어는 관리자만 사용할 수 있어요.", ephemeral=True)
        return

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message("텍스트 채널에서만 사용할 수 있어요.", ephemeral=True)
        return

    await channel.send(view=MainMarketView(interaction.guild.id))
    await interaction.response.send_message("마켓 패널을 올렸어요.", ephemeral=True)


@bot.tree.command(name="admin_add_item", description="이 서버에 상품을 추가하거나 수정합니다.")
@app_commands.describe(
    category="카테고리명 예: 입양하세요",
    item_name="상품 이름",
    price="가격",
    stock="재고",
    display_order="정렬 순서",
    description="상품 설명",
)
async def admin_add_item(
    interaction: discord.Interaction,
    category: str,
    item_name: str,
    price: app_commands.Range[int, 1, 100000000],
    stock: app_commands.Range[int, 0, 1000000],
    display_order: app_commands.Range[int, 0, 100000] = 0,
    description: str | None = None,
):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    existing_items = find_items_by_name(interaction.guild.id, item_name)
    if len(existing_items) > 1:
        await interaction.response.send_message(
            "같은 이름의 상품이 여러 개 있어요. 먼저 상품명을 정리한 뒤 다시 시도해 주세요.",
            ephemeral=True,
        )
        return

    item_key = existing_items[0]["item_key"] if existing_items else ensure_unique_item_key(interaction.guild.id, item_name)

    upsert_item(
        interaction.guild.id,
        item_key,
        category,
        item_name,
        price,
        stock,
        description,
        display_order,
    )

    await interaction.response.send_message(
        f"이 서버 상품을 저장했어요.\n"
        f"- 카테고리: {category}\n"
        f"- 이름: {item_name}\n"
        f"- 가격: {price}원\n"
        f"- 재고: {stock}개"
    )


@bot.tree.command(name="admin_set_stock", description="이 서버 상품 재고를 변경합니다.")
@app_commands.describe(item_name="상품 이름", stock="새 재고")
async def admin_set_stock(
    interaction: discord.Interaction,
    item_name: str,
    stock: app_commands.Range[int, 0, 1000000],
):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    changed, result = set_item_stock_by_name(interaction.guild.id, item_name, stock)
    if not changed:
        await interaction.response.send_message(result, ephemeral=True)
        return

    await interaction.response.send_message(f"{result} 재고를 {stock}개로 변경했어요.")


@bot.tree.command(name="admin_delete_item", description="이 서버 상품을 삭제합니다.")
@app_commands.describe(item_name="삭제할 상품 이름")
async def admin_delete_item(interaction: discord.Interaction, item_name: str):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    changed, result = delete_item_by_name(interaction.guild.id, item_name)
    if not changed:
        await interaction.response.send_message(result, ephemeral=True)
        return

    await interaction.response.send_message(f"{result} 상품을 삭제했어요.")


@bot.tree.command(name="admin_balance_add", description="이 서버에서 유저 잔액을 수동 추가합니다.")
@app_commands.describe(member="대상 유저", amount="추가할 금액")
async def admin_balance_add(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 1, 100000000],
):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    add_balance_db(interaction.guild.id, member.id, amount)
    stats = get_user_stats(interaction.guild.id, member.id)

    await interaction.response.send_message(
        f"{member.mention} 잔액에 {amount}원을 추가했어요.\n현재 잔액: {stats['balance']}원"
    )


@bot.tree.command(name="admin_balance_sub", description="이 서버에서 유저 잔액을 수동 차감합니다.")
@app_commands.describe(member="대상 유저", amount="차감할 금액")
async def admin_balance_sub(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 1, 100000000],
):
    ensure_guild(interaction)

    if not is_manager(interaction.user, interaction.guild):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    ok, new_balance = subtract_balance_db(interaction.guild.id, member.id, amount)
    if not ok:
        await interaction.response.send_message(
            f"잔액이 부족해요. 현재 잔액: {new_balance}원",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"{member.mention} 잔액에서 {amount}원을 차감했어요.\n현재 잔액: {new_balance}원"
    )


@bot.tree.command(name="balance", description="이 서버 기준 내 잔액을 확인합니다.")
async def balance(interaction: discord.Interaction):
    ensure_guild(interaction)
    stats = get_user_stats(interaction.guild.id, interaction.user.id)
    await interaction.response.send_message(
        f"현재 잔액: {stats['balance']}원\n"
        f"누적 구매금액: {stats['total_spent']}원\n"
        f"적용 할인: {stats['discount_text']}",
        ephemeral=True,
    )


@bot.tree.command(name="my_balance", description="이 서버 기준 내 잔액을 확인합니다.")
async def my_balance(interaction: discord.Interaction):
    ensure_guild(interaction)
    stats = get_user_stats(interaction.guild.id, interaction.user.id)
    await interaction.response.send_message(
        f"현재 잔액: {stats['balance']}원\n"
        f"누적 구매금액: {stats['total_spent']}원\n"
        f"적용 할인: {stats['discount_text']}",
        ephemeral=True,
    )


@bot.tree.command(name="charge_request", description="이 서버 기준 충전 요청을 생성합니다.")
@app_commands.describe(amount="충전 요청 금액", depositor="입금자명")
async def charge_request(
    interaction: discord.Interaction,
    amount: app_commands.Range[int, 1000, 100000000],
    depositor: str,
):
    ensure_guild(interaction)

    settings = get_guild_settings(interaction.guild.id)

    if not settings["charge_log_channel_id"]:
        await interaction.response.send_message(
            "이 서버는 충전 승인 채널이 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
            ephemeral=True,
        )
        return

    if not is_bank_configured(interaction.guild.id):
        await interaction.response.send_message(
            "이 서버는 계좌 정보가 아직 설정되지 않았어요. 관리자에게 문의해 주세요.",
            ephemeral=True,
        )
        return

    request_id = create_charge_request(interaction.guild.id, interaction.user.id, amount, depositor)

    await send_charge_log(
        interaction.guild,
        interaction.user.mention,
        request_id,
        amount,
        depositor,
    )

    await interaction.response.send_message(
        view=build_component_view(
            build_charge_result_container(interaction.guild.id, request_id, depositor, amount),
            timeout=300,
        ),
        ephemeral=True,
    )


@bot.tree.command(name="shop", description="이 서버 상점 전체 목록을 확인합니다.")
async def shop(interaction: discord.Interaction):
    ensure_guild(interaction)

    items = list_all_items(interaction.guild.id)
    if not items:
        await interaction.response.send_message("등록된 상품이 아직 없어요.", ephemeral=True)
        return

    await interaction.response.send_message(view=build_shop_view(items), ephemeral=True)


@bot.tree.command(name="buy", description="이 서버 상품 이름을 입력해서 구매합니다.")
@app_commands.describe(item_name="구매할 상품 이름")
async def buy(interaction: discord.Interaction, item_name: str):
    ensure_guild(interaction)

    success, message, data = create_purchase_by_name(interaction.guild.id, interaction.user.id, item_name)

    if not success or data is None:
        await interaction.response.send_message(message, ephemeral=True)
        return

    delivered, delivery_message = await finalize_purchase_delivery(interaction, data)
    if not delivered:
        await interaction.response.send_message(delivery_message, ephemeral=True)
        return

    await interaction.response.send_message(
        view=build_component_view(build_purchase_success_container(data), timeout=300),
        ephemeral=True,
    )


# ============================================================
# 에러 처리
# ============================================================


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError,
):
    try:
        if interaction.response.is_done():
            await interaction.followup.send(f"오류: {error}", ephemeral=True)
        else:
            await interaction.response.send_message(f"오류: {error}", ephemeral=True)
    except Exception:
        pass


# ============================================================
# 시작
# ============================================================

if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN 환경변수가 설정되지 않았어요.")

bot.run(DISCORD_TOKEN)
