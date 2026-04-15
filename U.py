# ============================================================
# Discord 종합봇 + 마켓 UI 통합 최종본
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
# $env:CHARGE_LOG_CHANNEL_ID="충전승인_관리자채널_ID"
# $env:BANK_NAME="은행명"
# $env:BANK_ACCOUNT_NUMBER="계좌번호"
# $env:BANK_ACCOUNT_HOLDER="예금주명"
#
# [선택]
# $env:GEMINI_API_KEY="제미니_API_키"
# $env:GEMINI_MODEL="gemini-2.5-flash"
# $env:TICKET_CATEGORY_NAME="Tickets"
# $env:TICKET_STAFF_ROLE_ID="스태프_역할_ID"
# $env:PURCHASE_LOG_CHANNEL_ID="구매로그_관리자채널_ID"
# $env:MARKET_TITLE="MungChi Market"
# $env:REVIEW_URL="후기_링크"
#
# Python 3.10 이상 권장
# ============================================================

import os
import sqlite3
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from google import genai
from google.genai import types


# ============================================================
# 환경변수
# ============================================================

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]

# 마켓 충전 승인 채널 / 계좌정보
CHARGE_LOG_CHANNEL_ID = int(os.environ["CHARGE_LOG_CHANNEL_ID"])
BANK_NAME = os.environ["BANK_NAME"]
BANK_ACCOUNT_NUMBER = os.environ["BANK_ACCOUNT_NUMBER"]
BANK_ACCOUNT_HOLDER = os.environ["BANK_ACCOUNT_HOLDER"]

# 선택값
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

TICKET_CATEGORY_NAME = os.getenv("TICKET_CATEGORY_NAME", "Tickets")
TICKET_STAFF_ROLE_ID = int(os.getenv("TICKET_STAFF_ROLE_ID", "0") or 0)

PURCHASE_LOG_CHANNEL_ID = int(
    os.getenv("PURCHASE_LOG_CHANNEL_ID", os.getenv("SHOP_LOG_CHANNEL_ID", "0")) or 0
)

MARKET_TITLE = os.getenv("MARKET_TITLE", "MungChi Market")
REVIEW_URL = os.getenv("REVIEW_URL", "").strip()

gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


# ============================================================
# DB
# ============================================================

DB_PATH = "bot_data.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        discord_user_id INTEGER PRIMARY KEY,
        balance INTEGER NOT NULL DEFAULT 0,
        total_spent INTEGER NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS charge_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        discord_user_id INTEGER NOT NULL,
        amount INTEGER NOT NULL,
        depositor_name TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL,
        processed_at TEXT,
        processed_by INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS items (
        item_key TEXT PRIMARY KEY,
        category TEXT NOT NULL,
        item_name TEXT NOT NULL,
        price INTEGER NOT NULL,
        stock INTEGER NOT NULL DEFAULT 0,
        description TEXT,
        sales_count INTEGER NOT NULL DEFAULT 0,
        display_order INTEGER NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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


def is_manager(member: discord.Member) -> bool:
    return member.guild_permissions.administrator or member.guild_permissions.manage_guild


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


def extract_embed_field_int(embed: discord.Embed, field_name: str) -> int | None:
    for field in embed.fields:
        if field.name == field_name:
            try:
                return int(field.value)
            except ValueError:
                return None
    return None


# ============================================================
# 유저 / 잔액 / 할인
# ============================================================

def ensure_user_row(user_id: int):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT discord_user_id FROM users WHERE discord_user_id = ?", (user_id,))
    row = cur.fetchone()

    if row is None:
        cur.execute(
            "INSERT INTO users (discord_user_id, balance, total_spent) VALUES (?, 0, 0)",
            (user_id,)
        )
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
    if percent <= 0:
        return "없음"
    return f"{percent}%"


def apply_discount(price: int, discount_percent: int) -> int:
    return int(price * (100 - discount_percent) / 100)


def get_user_stats(user_id: int) -> dict:
    ensure_user_row(user_id)

    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        "SELECT balance, total_spent FROM users WHERE discord_user_id = ?",
        (user_id,)
    )
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


def add_balance_db(user_id: int, amount: int):
    ensure_user_row(user_id)

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET balance = balance + ? WHERE discord_user_id = ?",
        (amount, user_id)
    )
    conn.commit()
    conn.close()


def subtract_balance_db(user_id: int, amount: int) -> tuple[bool, int]:
    ensure_user_row(user_id)

    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        "SELECT balance FROM users WHERE discord_user_id = ?",
        (user_id,)
    )
    row = cur.fetchone()
    current_balance = row["balance"]

    if current_balance < amount:
        conn.close()
        return False, current_balance

    new_balance = current_balance - amount
    cur.execute(
        "UPDATE users SET balance = ? WHERE discord_user_id = ?",
        (new_balance, user_id)
    )
    conn.commit()
    conn.close()
    return True, new_balance


# ============================================================
# 충전 DB
# ============================================================

def create_charge_request(user_id: int, amount: int, depositor_name: str) -> int:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO charge_requests (
            discord_user_id, amount, depositor_name, status, created_at
        ) VALUES (?, ?, ?, 'pending', ?)
    """, (
        user_id,
        amount,
        depositor_name.strip(),
        now_iso(),
    ))

    request_id = cur.lastrowid
    conn.commit()
    conn.close()
    return request_id


def approve_charge_request(request_id: int, admin_user_id: int) -> tuple[bool, str, int | None, int | None]:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM charge_requests WHERE id = ?", (request_id,))
    row = cur.fetchone()

    if row is None:
        conn.close()
        return False, "존재하지 않는 충전 요청이에요.", None, None

    if row["status"] != "pending":
        conn.close()
        return False, "이미 처리된 충전 요청이에요.", None, None

    user_id = row["discord_user_id"]
    amount = row["amount"]

    ensure_user_row(user_id)

    cur.execute(
        "UPDATE users SET balance = balance + ? WHERE discord_user_id = ?",
        (amount, user_id)
    )
    cur.execute("""
        UPDATE charge_requests
        SET status = 'approved',
            processed_at = ?,
            processed_by = ?
        WHERE id = ?
    """, (
        now_iso(),
        admin_user_id,
        request_id,
    ))

    conn.commit()
    conn.close()

    return True, "승인 완료", user_id, amount


def reject_charge_request(request_id: int, admin_user_id: int) -> tuple[bool, str, int | None]:
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM charge_requests WHERE id = ?", (request_id,))
    row = cur.fetchone()

    if row is None:
        conn.close()
        return False, "존재하지 않는 충전 요청이에요.", None

    if row["status"] != "pending":
        conn.close()
        return False, "이미 처리된 충전 요청이에요.", None

    user_id = row["discord_user_id"]

    cur.execute("""
        UPDATE charge_requests
        SET status = 'rejected',
            processed_at = ?,
            processed_by = ?
        WHERE id = ?
    """, (
        now_iso(),
        admin_user_id,
        request_id,
    ))

    conn.commit()
    conn.close()

    return True, "거절 완료", user_id


# ============================================================
# 상품 / 구매 DB
# ============================================================

def upsert_item(
    item_key: str,
    category: str,
    item_name: str,
    price: int,
    stock: int,
    description: str | None,
    display_order: int,
):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO items (
            item_key, category, item_name, price, stock, description, sales_count, display_order
        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?)
        ON CONFLICT(item_key) DO UPDATE SET
            category = excluded.category,
            item_name = excluded.item_name,
            price = excluded.price,
            stock = excluded.stock,
            description = excluded.description,
            display_order = excluded.display_order
    """, (
        normalize_item_key(item_key),
        category.strip(),
        item_name.strip(),
        price,
        stock,
        description.strip() if description else None,
        display_order,
    ))

    conn.commit()
    conn.close()


def set_item_stock(item_key: str, stock: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE items SET stock = ? WHERE item_key = ?",
        (stock, normalize_item_key(item_key))
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def delete_item(item_key: str) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM items WHERE item_key = ?",
        (normalize_item_key(item_key),)
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def get_item(item_key: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM items WHERE item_key = ?",
        (normalize_item_key(item_key),)
    )
    row = cur.fetchone()
    conn.close()
    return row


def list_categories() -> list[str]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT category
        FROM items
        ORDER BY category COLLATE NOCASE ASC
    """)
    rows = cur.fetchall()
    conn.close()
    return [row["category"] for row in rows]


def list_items_by_category(category: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM items
        WHERE category = ?
        ORDER BY display_order ASC, item_name COLLATE NOCASE ASC
    """, (category,))
    rows = cur.fetchall()
    conn.close()
    return rows


def list_all_items():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM items
        ORDER BY category COLLATE NOCASE ASC, display_order ASC, item_name COLLATE NOCASE ASC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def create_purchase(user_id: int, item_key: str) -> tuple[bool, str, dict | None]:
    ensure_user_row(user_id)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM items WHERE item_key = ?", (normalize_item_key(item_key),))
    item = cur.fetchone()
    if item is None:
        conn.close()
        return False, "존재하지 않는 상품이에요.", None

    if item["stock"] <= 0:
        conn.close()
        return False, "재고가 없어요.", None

    cur.execute(
        "SELECT balance, total_spent FROM users WHERE discord_user_id = ?",
        (user_id,)
    )
    user = cur.fetchone()

    current_balance = user["balance"]
    total_spent = user["total_spent"]
    discount_percent = get_discount_percent(total_spent)
    final_price = apply_discount(item["price"], discount_percent)

    if current_balance < final_price:
        conn.close()
        return False, f"잔액이 부족해요. 현재 잔액: {current_balance}원", None

    new_balance = current_balance - final_price
    new_stock = item["stock"] - 1
    new_total_spent = total_spent + final_price
    new_sales_count = item["sales_count"] + 1

    cur.execute(
        "UPDATE users SET balance = ?, total_spent = ? WHERE discord_user_id = ?",
        (new_balance, new_total_spent, user_id)
    )
    cur.execute(
        "UPDATE items SET stock = ?, sales_count = ? WHERE item_key = ?",
        (new_stock, new_sales_count, normalize_item_key(item_key))
    )
    cur.execute("""
        INSERT INTO purchases (
            discord_user_id, item_key, item_name, price_paid, original_price,
            discount_percent, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'pending_delivery', ?)
    """, (
        user_id,
        normalize_item_key(item_key),
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
    if PURCHASE_LOG_CHANNEL_ID == 0:
        return

    log_channel = guild.get_channel(PURCHASE_LOG_CHANNEL_ID)
    if not isinstance(log_channel, discord.TextChannel):
        return

    embed = discord.Embed(
        title="새 구매 요청",
        description="게임 아이템 지급 후 `지급 완료` 버튼을 눌러 주세요.",
    )
    embed.add_field(name="구매 ID", value=str(data["purchase_id"]), inline=False)
    embed.add_field(name="구매자", value=buyer_mention, inline=False)
    embed.add_field(name="상품명", value=data["item_name"], inline=False)
    embed.add_field(name="상품 키", value=data["item_key"], inline=False)
    embed.add_field(name="결제 금액", value=f"{data['price_paid']}원", inline=False)
    embed.add_field(name="상태", value="pending_delivery", inline=False)

    await log_channel.send(embed=embed, view=PurchaseDeliveryView())


async def send_charge_log(guild: discord.Guild, user_mention: str, request_id: int, amount: int, depositor_name: str):
    log_channel = guild.get_channel(CHARGE_LOG_CHANNEL_ID)
    if not isinstance(log_channel, discord.TextChannel):
        return

    embed = discord.Embed(
        title="새 충전 요청",
        description="실제 입금을 확인한 뒤 승인 또는 거절해 주세요.",
    )
    embed.add_field(name="요청 ID", value=str(request_id), inline=False)
    embed.add_field(name="유저", value=user_mention, inline=False)
    embed.add_field(name="입금자명", value=depositor_name, inline=False)
    embed.add_field(name="금액", value=f"{amount}원", inline=False)
    embed.add_field(name="상태", value="pending", inline=False)

    await log_channel.send(embed=embed, view=ChargeApprovalView())


# ============================================================
# 티켓 유틸
# ============================================================

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


def get_staff_role(guild: discord.Guild) -> discord.Role | None:
    if not TICKET_STAFF_ROLE_ID:
        return None
    return guild.get_role(TICKET_STAFF_ROLE_ID)


def has_staff_access(member: discord.Member) -> bool:
    if member.guild_permissions.administrator or member.guild_permissions.manage_channels:
        return True

    if TICKET_STAFF_ROLE_ID:
        for role in member.roles:
            if role.id == TICKET_STAFF_ROLE_ID:
                return True

    return False


def can_manage_ticket(member: discord.Member, channel) -> bool:
    owner_id = get_ticket_owner_id(channel)
    if owner_id is None:
        return False
    return member.id == owner_id or has_staff_access(member)


def find_open_ticket_channel(guild: discord.Guild, user_id: int) -> discord.TextChannel | None:
    for channel in guild.text_channels:
        if channel.topic == f"ticket_owner:{user_id}":
            return channel
    return None


async def get_or_create_ticket_category(guild: discord.Guild) -> discord.CategoryChannel:
    category = discord.utils.get(guild.categories, name=TICKET_CATEGORY_NAME)
    if category is not None:
        return category

    return await guild.create_category(
        TICKET_CATEGORY_NAME,
        reason="Ticket system setup"
    )


async def create_ticket_channel(
    interaction: discord.Interaction,
    issue_text: str
) -> discord.TextChannel:
    guild = interaction.guild
    if guild is None:
        raise RuntimeError("서버에서만 사용할 수 있어요.")

    bot_member = guild.me
    if bot_member is None:
        raise RuntimeError("봇 멤버 정보를 가져오지 못했어요.")

    category = await get_or_create_ticket_category(guild)
    staff_role = get_staff_role(guild)

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

    if staff_role is not None:
        overwrites[staff_role] = discord.PermissionOverwrite(
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

    embed = discord.Embed(
        title="티켓이 생성되었어요",
        description=(
            f"{interaction.user.mention} 님의 문의가 접수됐어요.\n\n"
            f"**문의 유형**\n{issue_text}"
        ),
    )
    embed.set_footer(text="아래 버튼으로 티켓을 닫거나 삭제할 수 있어요.")

    content = staff_role.mention if staff_role is not None else None

    await channel.send(
        content=content,
        embed=embed,
        view=TicketControlsView(),
        allowed_mentions=discord.AllowedMentions(roles=True),
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
# 임베드 빌더 - 마켓 UI
# ============================================================

def build_main_embed() -> discord.Embed:
    embed = discord.Embed(
        title=MARKET_TITLE,
        description=(
            "• 이용하실 서비스를 눌러주세요.\n\n"
            "• 구매 및 충전 전 안내사항을 먼저 확인해 주세요.\n\n"
            "• 아래 버튼을 누르면 본인만 볼 수 있는 화면이 열립니다."
        ),
    )
    return embed


def build_purchase_embed(selected_category: str | None = None) -> discord.Embed:
    embed = discord.Embed(
        title="구매하기",
        description="카테고리를 선택하면 제품 목록이 열립니다.",
    )
    if selected_category:
        embed.add_field(name="선택된 카테고리", value=selected_category, inline=False)
    return embed


def build_catalog_embed(selected_category: str | None = None) -> discord.Embed:
    embed = discord.Embed(
        title="제품 목록",
        description="카테고리를 선택하면 제품 목록이 표시됩니다.",
    )

    if selected_category:
        items = list_items_by_category(selected_category)
        if not items:
            embed.add_field(name=selected_category, value="등록된 상품이 없어요.", inline=False)
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

            embed.add_field(
                name=f"{selected_category} ({len(items)}개)",
                value=value[:1024],
                inline=False,
            )

    return embed


def build_item_detail_embed(user_id: int, item) -> discord.Embed:
    stats = get_user_stats(user_id)
    discount_percent = stats["discount_percent"]
    final_price = apply_discount(item["price"], discount_percent)

    embed = discord.Embed(
        title=item["item_name"],
        description=item["description"] or "설명 없음",
    )

    embed.add_field(name="카테고리", value=item["category"], inline=True)
    embed.add_field(name="재고", value=f"{item['stock']}개", inline=True)
    embed.add_field(name="판매량", value=f"{item['sales_count']}개", inline=True)

    if discount_percent > 0:
        embed.add_field(name="정가", value=f"{item['price']}원", inline=True)
        embed.add_field(name="적용 할인", value=f"{discount_percent}%", inline=True)
        embed.add_field(name="결제 금액", value=f"{final_price}원", inline=True)
    else:
        embed.add_field(name="가격", value=f"{item['price']}원", inline=True)
        embed.add_field(name="적용 할인", value="없음", inline=True)
        embed.add_field(name="결제 금액", value=f"{final_price}원", inline=True)

    return embed


def build_charge_embed() -> discord.Embed:
    return discord.Embed(
        title="충전하기",
        description="원하는 충전 방식을 선택하세요.",
    )


def build_charge_result_embed(request_id: int, depositor: str, amount: int) -> discord.Embed:
    embed = discord.Embed(
        title="충전 요청 접수 완료",
        description=(
            "아래 계좌로 정확한 금액을 입금해 주세요.\n"
            "관리자가 확인 후 승인하면 잔액이 충전돼요."
        ),
    )
    embed.add_field(name="요청 ID", value=str(request_id), inline=False)
    embed.add_field(name="입금자명", value=depositor, inline=True)
    embed.add_field(name="금액", value=f"{amount}원", inline=True)
    embed.add_field(name="은행", value=BANK_NAME, inline=False)
    embed.add_field(name="계좌번호", value=BANK_ACCOUNT_NUMBER, inline=False)
    embed.add_field(name="예금주", value=BANK_ACCOUNT_HOLDER, inline=False)
    embed.set_footer(text="입금자명과 금액이 다르면 승인 지연이 생길 수 있어요.")
    return embed


def build_info_embed(user: discord.abc.User) -> discord.Embed:
    stats = get_user_stats(user.id)

    embed = discord.Embed(title="정보")
    embed.add_field(name="유저", value=user.mention, inline=False)
    embed.add_field(name="잔액", value=f"{stats['balance']}원", inline=False)
    embed.add_field(name="누적 구매금액", value=f"{stats['total_spent']}원", inline=False)
    embed.add_field(name="적용 할인", value=stats["discount_text"], inline=False)
    return embed


def build_purchase_success_embed(data: dict) -> discord.Embed:
    embed = discord.Embed(
        title="구매 완료",
        description="구매가 완료되었어요. 관리자가 아이템 지급 후 DM으로 안내합니다.",
    )
    embed.add_field(name="구매 ID", value=str(data["purchase_id"]), inline=False)
    embed.add_field(name="상품명", value=data["item_name"], inline=False)
    embed.add_field(name="결제 금액", value=f"{data['price_paid']}원", inline=True)
    embed.add_field(name="남은 잔액", value=f"{data['new_balance']}원", inline=True)
    embed.add_field(name="남은 재고", value=f"{data['new_stock']}개", inline=True)
    return embed


# ============================================================
# Select / Modal
# ============================================================

class CategorySelect(discord.ui.Select):
    def __init__(self, owner_id: int, mode: str, selected_category: str | None = None):
        self.owner_id = owner_id
        self.mode = mode

        categories = list_categories()
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
            view = PurchaseView(self.owner_id, selected_category=category)
            await interaction.response.edit_message(
                embed=build_purchase_embed(category),
                view=view,
            )
        else:
            view = CatalogView(self.owner_id, selected_category=category)
            await interaction.response.edit_message(
                embed=build_catalog_embed(category),
                view=view,
            )


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
    def __init__(self, owner_id: int, category: str):
        self.owner_id = owner_id
        self.category = category

        items = list_items_by_category(category)
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

        item = get_item(item_key)
        if item is None:
            await interaction.response.send_message("상품을 찾을 수 없어요.", ephemeral=True)
            return

        view = PurchaseConfirmView(self.owner_id, self.category, item_key)
        await interaction.response.edit_message(
            embed=build_item_detail_embed(interaction.user.id, item),
            view=view,
        )


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

    def __init__(self, owner_id: int):
        super().__init__()
        self.owner_id = owner_id

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        depositor_name = self.depositor.value.strip()
        raw_amount = self.amount.value.strip().replace(",", "")

        if not raw_amount.isdigit():
            await interaction.response.send_message(
                "금액은 숫자로만 입력해 주세요.",
                ephemeral=True,
            )
            return

        amount = int(raw_amount)
        if amount < 1000 or amount > 100000000:
            await interaction.response.send_message(
                "금액은 1,000원 이상 100,000,000원 이하로 입력해 주세요.",
                ephemeral=True,
            )
            return

        request_id = create_charge_request(interaction.user.id, amount, depositor_name)

        if interaction.guild is not None:
            await send_charge_log(
                interaction.guild,
                interaction.user.mention,
                request_id,
                amount,
                depositor_name,
            )

        await interaction.response.send_message(
            embed=build_charge_result_embed(request_id, depositor_name, amount),
            ephemeral=True,
        )


# ============================================================
# View
# ============================================================

class MainMarketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="구매",
        style=discord.ButtonStyle.secondary,
        custom_id="market:buy",
        emoji="🛒",
    )
    async def buy_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = PurchaseView(interaction.user.id)
        await interaction.response.send_message(
            embed=build_purchase_embed(),
            view=view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="제품",
        style=discord.ButtonStyle.secondary,
        custom_id="market:catalog",
        emoji="🔎",
    )
    async def catalog_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = CatalogView(interaction.user.id)
        await interaction.response.send_message(
            embed=build_catalog_embed(),
            view=view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="충전",
        style=discord.ButtonStyle.secondary,
        custom_id="market:charge",
        emoji="🎁",
    )
    async def charge_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ChargeMethodView(interaction.user.id)
        await interaction.response.send_message(
            embed=build_charge_embed(),
            view=view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="정보",
        style=discord.ButtonStyle.secondary,
        custom_id="market:info",
        emoji="⚙️",
    )
    async def info_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = InfoView(interaction.user.id)
        await interaction.response.send_message(
            embed=build_info_embed(interaction.user),
            view=view,
            ephemeral=True,
        )


class PurchaseView(discord.ui.View):
    def __init__(self, owner_id: int, selected_category: str | None = None):
        super().__init__(timeout=600)
        self.owner_id = owner_id

        self.add_item(CategorySelect(owner_id, "purchase", selected_category))
        if selected_category:
            self.add_item(ProductSelect(owner_id, selected_category))
        else:
            self.add_item(PlaceholderProductSelect())


class CatalogView(discord.ui.View):
    def __init__(self, owner_id: int, selected_category: str | None = None):
        super().__init__(timeout=600)
        self.owner_id = owner_id
        self.add_item(CategorySelect(owner_id, "catalog", selected_category))


class PurchaseConfirmView(discord.ui.View):
    def __init__(self, owner_id: int, category: str, item_key: str):
        super().__init__(timeout=600)
        self.owner_id = owner_id
        self.category = category
        self.item_key = item_key

    @discord.ui.button(label="구매하기", style=discord.ButtonStyle.primary, emoji="🛍️")
    async def confirm_buy(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        success, message, data = create_purchase(interaction.user.id, self.item_key)
        if not success or data is None:
            await interaction.response.send_message(message, ephemeral=True)
            return

        if interaction.guild is not None:
            await send_purchase_log(interaction.guild, interaction.user.mention, data)

        await interaction.response.edit_message(
            embed=build_purchase_success_embed(data),
            view=None,
        )

    @discord.ui.button(label="뒤로가기", style=discord.ButtonStyle.secondary)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        view = PurchaseView(self.owner_id, self.category)
        await interaction.response.edit_message(
            embed=build_purchase_embed(self.category),
            view=view,
        )


class ChargeMethodView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=300)
        self.owner_id = owner_id

    @discord.ui.button(label="계좌이체(account)", style=discord.ButtonStyle.secondary)
    async def account_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        await interaction.response.send_modal(ChargeRequestModal(self.owner_id))

    @discord.ui.button(label="코인 충전(coin)", style=discord.ButtonStyle.secondary)
    async def coin_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        await interaction.response.send_message(
            "코인 충전 기능은 아직 준비 중이에요.",
            ephemeral=True,
        )


class RefreshInfoButton(discord.ui.Button):
    def __init__(self, owner_id: int):
        super().__init__(label="새로고침", style=discord.ButtonStyle.primary)
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("본인만 사용할 수 있어요.", ephemeral=True)
            return

        view = InfoView(self.owner_id)
        await interaction.response.edit_message(
            embed=build_info_embed(interaction.user),
            view=view,
        )


class InfoView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=300)
        self.owner_id = owner_id
        self.add_item(RefreshInfoButton(owner_id))

        if REVIEW_URL:
            self.add_item(
                discord.ui.Button(
                    label="후기",
                    style=discord.ButtonStyle.link,
                    url=REVIEW_URL,
                )
            )


class ChargeApprovalView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="승인",
        style=discord.ButtonStyle.green,
        custom_id="charge:approve",
        emoji="✅",
    )
    async def approve_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not isinstance(interaction.user, discord.Member) or not is_manager(interaction.user):
            await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
            return

        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("요청 정보를 찾을 수 없어요.", ephemeral=True)
            return

        embed = interaction.message.embeds[0]
        request_id = extract_embed_field_int(embed, "요청 ID")

        if request_id is None:
            await interaction.response.send_message("요청 ID를 찾을 수 없어요.", ephemeral=True)
            return

        success, message, user_id, amount = approve_charge_request(request_id, interaction.user.id)
        if not success:
            await interaction.response.send_message(message, ephemeral=True)
            return

        new_embed = discord.Embed(
            title="충전 요청 승인됨",
            description="충전이 완료되었어요.",
        )
        new_embed.add_field(name="요청 ID", value=str(request_id), inline=False)
        new_embed.add_field(name="대상 유저", value=f"<@{user_id}>", inline=False)
        new_embed.add_field(name="충전 금액", value=f"{amount}원", inline=False)
        new_embed.add_field(name="처리자", value=interaction.user.mention, inline=False)
        new_embed.add_field(name="상태", value="approved", inline=False)

        await interaction.message.edit(embed=new_embed, view=None)
        await interaction.response.send_message("승인 완료.", ephemeral=True)

        if user_id is not None and amount is not None:
            await safe_notify_user(
                interaction.client,
                user_id,
                f"충전 요청이 승인되었어요. {amount}원이 잔액에 반영됐어요."
            )

    @discord.ui.button(
        label="거절",
        style=discord.ButtonStyle.danger,
        custom_id="charge:reject",
        emoji="❌",
    )
    async def reject_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not isinstance(interaction.user, discord.Member) or not is_manager(interaction.user):
            await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
            return

        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("요청 정보를 찾을 수 없어요.", ephemeral=True)
            return

        embed = interaction.message.embeds[0]
        request_id = extract_embed_field_int(embed, "요청 ID")

        if request_id is None:
            await interaction.response.send_message("요청 ID를 찾을 수 없어요.", ephemeral=True)
            return

        success, message, user_id = reject_charge_request(request_id, interaction.user.id)
        if not success:
            await interaction.response.send_message(message, ephemeral=True)
            return

        new_embed = discord.Embed(
            title="충전 요청 거절됨",
            description="충전 요청이 거절되었어요.",
        )
        new_embed.add_field(name="요청 ID", value=str(request_id), inline=False)
        new_embed.add_field(name="대상 유저", value=f"<@{user_id}>", inline=False)
        new_embed.add_field(name="처리자", value=interaction.user.mention, inline=False)
        new_embed.add_field(name="상태", value="rejected", inline=False)

        await interaction.message.edit(embed=new_embed, view=None)
        await interaction.response.send_message("거절 완료.", ephemeral=True)

        if user_id is not None:
            await safe_notify_user(
                interaction.client,
                user_id,
                "충전 요청이 거절되었어요. 입금 내역을 다시 확인해 주세요."
            )


class PurchaseDeliveryView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="지급 완료",
        style=discord.ButtonStyle.green,
        custom_id="purchase:deliver",
        emoji="📦",
    )
    async def deliver_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not isinstance(interaction.user, discord.Member) or not is_manager(interaction.user):
            await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
            return

        if not interaction.message or not interaction.message.embeds:
            await interaction.response.send_message("구매 정보를 찾을 수 없어요.", ephemeral=True)
            return

        embed = interaction.message.embeds[0]
        purchase_id = extract_embed_field_int(embed, "구매 ID")

        if purchase_id is None:
            await interaction.response.send_message("구매 ID를 찾을 수 없어요.", ephemeral=True)
            return

        success, message, user_id, item_name = mark_purchase_delivered(purchase_id, interaction.user.id)
        if not success:
            await interaction.response.send_message(message, ephemeral=True)
            return

        new_embed = discord.Embed(
            title="구매 지급 완료",
            description="아이템 지급이 완료되었어요.",
        )
        new_embed.add_field(name="구매 ID", value=str(purchase_id), inline=False)
        new_embed.add_field(name="대상 유저", value=f"<@{user_id}>", inline=False)
        new_embed.add_field(name="상품명", value=item_name or "-", inline=False)
        new_embed.add_field(name="처리자", value=interaction.user.mention, inline=False)
        new_embed.add_field(name="상태", value="delivered", inline=False)

        await interaction.message.edit(embed=new_embed, view=None)
        await interaction.response.send_message("지급 완료 처리했어요.", ephemeral=True)

        if user_id is not None and item_name is not None:
            await safe_notify_user(
                interaction.client,
                user_id,
                f"구매한 아이템 `{item_name}` 지급이 완료되었어요."
            )


class TicketPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="티켓 만들기",
        style=discord.ButtonStyle.green,
        custom_id="ticket:create",
        emoji="🎫",
    )
    async def create_ticket_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if interaction.guild is None:
            await interaction.response.send_message(
                "서버에서만 사용할 수 있어요.",
                ephemeral=True,
            )
            return

        existing = find_open_ticket_channel(interaction.guild, interaction.user.id)
        if existing is not None:
            await interaction.response.send_message(
                f"이미 열린 티켓이 있어요: {existing.mention}",
                ephemeral=True,
            )
            return

        try:
            channel = await create_ticket_channel(interaction, "일반 문의")
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


class TicketControlsView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="닫기",
        style=discord.ButtonStyle.secondary,
        custom_id="ticket:close",
        emoji="🔒",
    )
    async def close_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        channel = interaction.channel

        if not isinstance(channel, discord.TextChannel) or not is_ticket_channel(channel):
            await interaction.response.send_message(
                "티켓 채널에서만 사용할 수 있어요.",
                ephemeral=True,
            )
            return

        if not can_manage_ticket(interaction.user, channel):
            await interaction.response.send_message(
                "이 티켓을 닫을 권한이 없어요.",
                ephemeral=True,
            )
            return

        if is_ticket_closed(channel):
            await interaction.response.send_message(
                "이미 닫힌 티켓이에요.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            await close_ticket_channel(channel, interaction.user)
            await channel.send("티켓이 닫혔어요. 필요하면 `삭제` 버튼이나 `/ticket_delete`를 사용해 주세요.")
            await interaction.followup.send("티켓을 닫았어요.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(
                f"티켓을 닫는 중 오류가 발생했어요: {e}",
                ephemeral=True,
            )

    @discord.ui.button(
        label="삭제",
        style=discord.ButtonStyle.danger,
        custom_id="ticket:delete",
        emoji="🗑️",
    )
    async def delete_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        channel = interaction.channel

        if not isinstance(channel, discord.TextChannel) or not is_ticket_channel(channel):
            await interaction.response.send_message(
                "티켓 채널에서만 사용할 수 있어요.",
                ephemeral=True,
            )
            return

        if not can_manage_ticket(interaction.user, channel):
            await interaction.response.send_message(
                "이 티켓을 삭제할 권한이 없어요.",
                ephemeral=True,
            )
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "서버 멤버 정보가 필요해요.",
                ephemeral=True,
            )
            return

        if not has_staff_access(interaction.user) and not is_ticket_closed(channel):
            await interaction.response.send_message(
                "먼저 티켓을 닫은 뒤 삭제해 주세요.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            "티켓 채널을 삭제할게요.",
            ephemeral=True,
        )

        try:
            await delete_ticket_channel(channel, interaction.user)
        except Exception:
            pass


# ============================================================
# 봇 본체
# ============================================================

class MyBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        init_db()

        self.add_view(MainMarketView())
        self.add_view(ChargeApprovalView())
        self.add_view(PurchaseDeliveryView())
        self.add_view(TicketPanelView())
        self.add_view(TicketControlsView())

        await self.tree.sync()


bot = MyBot()


@bot.event
async def on_ready():
    print(f"로그인 완료: {bot.user}")


# ============================================================
# 관리 명령어
# ============================================================

@bot.tree.command(name="kick", description="유저를 서버에서 추방합니다.")
@app_commands.describe(member="추방할 유저", reason="사유")
async def kick(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str | None = None,
):
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
        await interaction.response.send_message(
            "봇에 `Kick Members` 권한이 없어요.",
            ephemeral=True,
        )
        return

    ok, error_message = can_act_on_member(interaction.user, member, interaction.guild)
    if not ok:
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    try:
        await member.kick(reason=reason or f"By {interaction.user}")
        await interaction.response.send_message(
            f"{member.mention} 님을 추방했어요. 사유: {reason or '없음'}"
        )
    except discord.Forbidden:
        await interaction.response.send_message(
            "역할 순서 또는 권한 문제로 추방하지 못했어요.",
            ephemeral=True,
        )
    except Exception as e:
        await interaction.response.send_message(
            f"오류가 발생했어요: {e}",
            ephemeral=True,
        )


@bot.tree.command(name="ban", description="유저를 서버에서 차단합니다.")
@app_commands.describe(member="차단할 유저", reason="사유")
async def ban(
    interaction: discord.Interaction,
    member: discord.Member,
    reason: str | None = None,
):
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
        await interaction.response.send_message(
            "봇에 `Ban Members` 권한이 없어요.",
            ephemeral=True,
        )
        return

    ok, error_message = can_act_on_member(interaction.user, member, interaction.guild)
    if not ok:
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    try:
        await interaction.guild.ban(
            member,
            reason=reason or f"By {interaction.user}",
        )
        await interaction.response.send_message(
            f"{member.mention} 님을 차단했어요. 사유: {reason or '없음'}"
        )
    except discord.Forbidden:
        await interaction.response.send_message(
            "역할 순서 또는 권한 문제로 차단하지 못했어요.",
            ephemeral=True,
        )
    except Exception as e:
        await interaction.response.send_message(
            f"오류가 발생했어요: {e}",
            ephemeral=True,
        )


@bot.tree.command(name="clear", description="최근 메시지를 삭제합니다.")
@app_commands.describe(count="삭제할 메시지 수(1~100)")
async def clear(
    interaction: discord.Interaction,
    count: app_commands.Range[int, 1, 100],
):
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
        await interaction.response.send_message(
            "봇에 `Manage Messages` 권한이 없어요.",
            ephemeral=True,
        )
        return

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message(
            "텍스트 채널에서만 사용할 수 있어요.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)

    try:
        deleted = await channel.purge(limit=count)
        await interaction.followup.send(
            f"{len(deleted)}개의 메시지를 삭제했어요.",
            ephemeral=True,
        )
    except Exception as e:
        await interaction.followup.send(
            f"오류가 발생했어요: {e}",
            ephemeral=True,
        )


@bot.tree.command(name="timeout", description="유저를 일정 시간 동안 타임아웃합니다.")
@app_commands.describe(
    member="타임아웃할 유저",
    minutes="타임아웃 시간(분 단위, 최대 40320분 = 28일)",
    reason="사유",
)
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
        await interaction.response.send_message(
            "봇에 `Moderate Members` 권한이 없어요.",
            ephemeral=True,
        )
        return

    ok, error_message = can_act_on_member(interaction.user, member, interaction.guild)
    if not ok:
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)

    try:
        await member.edit(
            timed_out_until=until,
            reason=reason or f"Timed out by {interaction.user}",
        )
        await interaction.response.send_message(
            f"{member.mention} 님을 타임아웃했어요.\n"
            f"해제 시각: <t:{int(until.timestamp())}:F>\n"
            f"사유: {reason or '없음'}"
        )
    except discord.Forbidden:
        await interaction.response.send_message(
            "권한 또는 역할 순서 문제로 타임아웃하지 못했어요.",
            ephemeral=True,
        )
    except Exception as e:
        await interaction.response.send_message(
            f"오류가 발생했어요: {e}",
            ephemeral=True,
        )


@bot.tree.command(name="untimeout", description="유저의 타임아웃을 해제합니다.")
@app_commands.describe(member="타임아웃을 해제할 유저", reason="사유")
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
        await interaction.response.send_message(
            "봇에 `Moderate Members` 권한이 없어요.",
            ephemeral=True,
        )
        return

    ok, error_message = can_act_on_member(interaction.user, member, interaction.guild)
    if not ok:
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    try:
        await member.edit(
            timed_out_until=None,
            reason=reason or f"Timeout removed by {interaction.user}",
        )
        await interaction.response.send_message(
            f"{member.mention} 님의 타임아웃을 해제했어요.\n"
            f"사유: {reason or '없음'}"
        )
    except discord.Forbidden:
        await interaction.response.send_message(
            "권한 또는 역할 순서 문제로 타임아웃을 해제하지 못했어요.",
            ephemeral=True,
        )
    except Exception as e:
        await interaction.response.send_message(
            f"오류가 발생했어요: {e}",
            ephemeral=True,
        )


# ============================================================
# AI
# ============================================================

@bot.tree.command(name="ask", description="AI에게 질문합니다.")
@app_commands.describe(question="질문 내용")
async def ask(interaction: discord.Interaction, question: str):
    if gemini_client is None:
        await interaction.response.send_message(
            "GEMINI_API_KEY가 설정되지 않았어요.",
            ephemeral=True,
        )
        return

    await interaction.response.defer()

    try:
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=question,
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

        if len(answer) <= 1900:
            await interaction.followup.send(answer)
        else:
            for i in range(0, len(answer), 1900):
                await interaction.followup.send(answer[i:i + 1900])

    except Exception as e:
        await interaction.followup.send(f"Gemini 호출 중 오류가 발생했어요: {e}")


# ============================================================
# 티켓 명령어
# ============================================================

@bot.tree.command(name="ticket_panel", description="현재 채널에 티켓 생성 패널을 보냅니다.")
async def ticket_panel(interaction: discord.Interaction):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member) or not has_staff_access(interaction.user):
        await interaction.response.send_message(
            "이 명령어는 관리자 또는 티켓 스태프만 사용할 수 있어요.",
            ephemeral=True,
        )
        return

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message(
            "텍스트 채널에서만 사용할 수 있어요.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title="고객지원 티켓",
        description=(
            "아래 버튼을 한 번 누르면 전용 티켓 채널이 바로 생성됩니다.\n"
            "일반 유저도 사용할 수 있어요."
        ),
    )
    embed.add_field(
        name="안내",
        value="한 사람당 열린 티켓은 1개만 만들 수 있어요.",
        inline=False,
    )

    await channel.send(embed=embed, view=TicketPanelView())
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
# 마켓 UI / 상품 / 충전 / 잔액
# ============================================================

@bot.tree.command(name="market_panel", description="마켓 메인 패널을 현재 채널에 올립니다.")
async def market_panel(interaction: discord.Interaction):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member) or not is_manager(interaction.user):
        await interaction.response.send_message("이 명령어는 관리자만 사용할 수 있어요.", ephemeral=True)
        return

    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message("텍스트 채널에서만 사용할 수 있어요.", ephemeral=True)
        return

    await channel.send(embed=build_main_embed(), view=MainMarketView())
    await interaction.response.send_message("마켓 패널을 올렸어요.", ephemeral=True)


@bot.tree.command(name="admin_add_item", description="상품을 추가하거나 수정합니다.")
@app_commands.describe(
    category="카테고리명 예: 입양하세요",
    item_key="상품 키 예: age_box_68k",
    item_name="상품 이름",
    price="가격",
    stock="재고",
    display_order="정렬 순서",
    description="상품 설명",
)
async def admin_add_item(
    interaction: discord.Interaction,
    category: str,
    item_key: str,
    item_name: str,
    price: app_commands.Range[int, 1, 100000000],
    stock: app_commands.Range[int, 0, 1000000],
    display_order: app_commands.Range[int, 0, 100000] = 0,
    description: str | None = None,
):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member) or not is_manager(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    upsert_item(item_key, category, item_name, price, stock, description, display_order)

    await interaction.response.send_message(
        f"상품을 저장했어요.\n"
        f"- 카테고리: {category}\n"
        f"- 키: {normalize_item_key(item_key)}\n"
        f"- 이름: {item_name}\n"
        f"- 가격: {price}원\n"
        f"- 재고: {stock}개"
    )


@bot.tree.command(name="admin_set_stock", description="상품 재고를 변경합니다.")
@app_commands.describe(item_key="상품 키", stock="새 재고")
async def admin_set_stock(
    interaction: discord.Interaction,
    item_key: str,
    stock: app_commands.Range[int, 0, 1000000],
):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member) or not is_manager(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    changed = set_item_stock(item_key, stock)
    if not changed:
        await interaction.response.send_message("존재하지 않는 상품 키예요.", ephemeral=True)
        return

    await interaction.response.send_message(
        f"{normalize_item_key(item_key)} 재고를 {stock}개로 변경했어요."
    )


@bot.tree.command(name="admin_delete_item", description="상품을 삭제합니다.")
@app_commands.describe(item_key="삭제할 상품 키")
async def admin_delete_item(interaction: discord.Interaction, item_key: str):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member) or not is_manager(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    changed = delete_item(item_key)
    if not changed:
        await interaction.response.send_message("존재하지 않는 상품 키예요.", ephemeral=True)
        return

    await interaction.response.send_message(f"{normalize_item_key(item_key)} 상품을 삭제했어요.")


@bot.tree.command(name="admin_balance_add", description="유저 잔액을 수동으로 추가합니다.")
@app_commands.describe(member="대상 유저", amount="추가할 금액")
async def admin_balance_add(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 1, 100000000],
):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member) or not is_manager(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    add_balance_db(member.id, amount)
    stats = get_user_stats(member.id)

    await interaction.response.send_message(
        f"{member.mention} 잔액에 {amount}원을 추가했어요.\n현재 잔액: {stats['balance']}원"
    )


@bot.tree.command(name="admin_balance_sub", description="유저 잔액을 수동으로 차감합니다.")
@app_commands.describe(member="대상 유저", amount="차감할 금액")
async def admin_balance_sub(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: app_commands.Range[int, 1, 100000000],
):
    ensure_guild(interaction)

    if not isinstance(interaction.user, discord.Member) or not is_manager(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있어요.", ephemeral=True)
        return

    ok, new_balance = subtract_balance_db(member.id, amount)
    if not ok:
        await interaction.response.send_message(
            f"잔액이 부족해요. 현재 잔액: {new_balance}원",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"{member.mention} 잔액에서 {amount}원을 차감했어요.\n현재 잔액: {new_balance}원"
    )


@bot.tree.command(name="balance", description="내 잔액을 확인합니다.")
async def balance(interaction: discord.Interaction):
    stats = get_user_stats(interaction.user.id)
    await interaction.response.send_message(
        f"현재 잔액: {stats['balance']}원\n"
        f"누적 구매금액: {stats['total_spent']}원\n"
        f"적용 할인: {stats['discount_text']}",
        ephemeral=True,
    )


@bot.tree.command(name="my_balance", description="내 잔액을 확인합니다.")
async def my_balance(interaction: discord.Interaction):
    stats = get_user_stats(interaction.user.id)
    await interaction.response.send_message(
        f"현재 잔액: {stats['balance']}원\n"
        f"누적 구매금액: {stats['total_spent']}원\n"
        f"적용 할인: {stats['discount_text']}",
        ephemeral=True,
    )


@bot.tree.command(name="charge_request", description="슬래시 명령으로 충전 요청을 생성합니다.")
@app_commands.describe(amount="충전 요청 금액", depositor="입금자명")
async def charge_request(
    interaction: discord.Interaction,
    amount: app_commands.Range[int, 1000, 100000000],
    depositor: str,
):
    ensure_guild(interaction)

    request_id = create_charge_request(interaction.user.id, amount, depositor)

    if interaction.guild is not None:
        await send_charge_log(
            interaction.guild,
            interaction.user.mention,
            request_id,
            amount,
            depositor,
        )

    await interaction.response.send_message(
        embed=build_charge_result_embed(request_id, depositor, amount),
        ephemeral=True,
    )


@bot.tree.command(name="shop", description="전체 상품 목록을 확인합니다.")
async def shop(interaction: discord.Interaction):
    items = list_all_items()
    if not items:
        await interaction.response.send_message("등록된 상품이 아직 없어요.", ephemeral=True)
        return

    embed = discord.Embed(
        title="상점 목록",
        description="상품 키를 확인한 뒤 `/buy`로 구매하거나, 마켓 패널의 `구매` 버튼을 사용하세요.",
    )

    current_category = None
    lines = []
    shown = 0

    for item in items:
        line = (
            f"`{item['item_key']}` - **{item['item_name']}** / "
            f"{item['price']}원 / 재고 {item['stock']}개 / 판매 {item['sales_count']}개"
        )
        if len(line) > 1000:
            line = line[:997] + "..."

        if current_category != item["category"]:
            if current_category is not None and lines:
                embed.add_field(name=current_category, value="\n".join(lines)[:1024], inline=False)
                shown += len(lines)
                if len(embed.fields) >= 10:
                    break
            current_category = item["category"]
            lines = [line]
        else:
            lines.append(line)
            if len("\n".join(lines)) > 900:
                lines.pop()
                embed.add_field(name=current_category, value="\n".join(lines)[:1024], inline=False)
                shown += len(lines)
                current_category = item["category"]
                lines = [line]
                if len(embed.fields) >= 10:
                    break

    if current_category is not None and lines and len(embed.fields) < 10:
        embed.add_field(name=current_category, value="\n".join(lines)[:1024], inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="buy", description="상품 키를 입력해서 구매합니다.")
@app_commands.describe(item_key="구매할 상품 키")
async def buy(interaction: discord.Interaction, item_key: str):
    success, message, data = create_purchase(interaction.user.id, item_key)

    if not success or data is None:
        await interaction.response.send_message(message, ephemeral=True)
        return

    if interaction.guild is not None:
        await send_purchase_log(interaction.guild, interaction.user.mention, data)

    await interaction.response.send_message(
        embed=build_purchase_success_embed(data),
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

bot.run(DISCORD_TOKEN)
