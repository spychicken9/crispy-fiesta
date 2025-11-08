# at the top of db.py
import os
from pathlib import Path

# DB_PATH will be:
# - /data/roster.sqlite3 on Railway
# - roster.sqlite3 locally
DB_PATH = Path(os.environ.get("DB_PATH", "roster.sqlite3"))

# SQLite storage for classes, members, socials, and family relations.
import sqlite3
from typing import Optional, Iterable


# ---------- connection ----------
def _conn() -> sqlite3.Connection:
    cx = sqlite3.connect(DB_PATH)
    # Return rows as tuples (default). If you want dict-like rows, set row_factory.
    return cx


# ---------- schema ----------
def init_db() -> None:
    """Create tables if they don't exist."""
    with _conn() as cx:
        # Classes are ordered globally by order_index (lower appears earlier)
        cx.execute("""
            CREATE TABLE IF NOT EXISTS classes(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                order_index INTEGER NOT NULL
            );
        """)

        # Members: split names + global roll number + class join order
        cx.execute("""
            CREATE TABLE IF NOT EXISTS members(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                class_id   INTEGER NOT NULL,
                first_name TEXT    NOT NULL,
                last_name  TEXT    NOT NULL,
                nickname   TEXT    NOT NULL,
                full_name  TEXT,              -- optional convenience
                join_order INTEGER NOT NULL,  -- order within the class
                roll_number INTEGER UNIQUE,   -- global running number
                honorific  TEXT NOT NULL DEFAULT 'Mr.',
                bio        TEXT DEFAULT NULL,
                UNIQUE(class_id, nickname),
                FOREIGN KEY(class_id) REFERENCES classes(id)
            );
        """)

        # Social handles: 1 row per platform
        cx.execute("""
            CREATE TABLE IF NOT EXISTS member_socials(
                member_id INTEGER NOT NULL,
                platform  TEXT    NOT NULL,
                handle    TEXT    NOT NULL,
                PRIMARY KEY(member_id, platform),
                FOREIGN KEY(member_id) REFERENCES members(id)
            );
        """)

        # Family: each member can have one big; littles are the members pointing to you
        cx.execute("""
            CREATE TABLE IF NOT EXISTS family(
                member_id INTEGER PRIMARY KEY,
                big_id    INTEGER,
                FOREIGN KEY(member_id) REFERENCES members(id),
                FOREIGN KEY(big_id)    REFERENCES members(id)
            );
        """)


# ---------- id helpers ----------
def _class_id(name: str) -> Optional[int]:
    with _conn() as cx:
        row = cx.execute("SELECT id FROM classes WHERE name=?", (name,)).fetchone()
        return row[0] if row else None


def _member_id_by_nick(nick: str) -> Optional[int]:
    with _conn() as cx:
        row = cx.execute("SELECT id FROM members WHERE LOWER(nickname)=LOWER(?)", (nick,)).fetchone()
        return row[0] if row else None


def _next_roll_number() -> int:
    with _conn() as cx:
        # Get the max assigned roll number
        last = cx.execute("SELECT MAX(roll_number) FROM members").fetchone()[0]
        if last is None:
            last = 1  # we start counting from 2 anyway below

        # Load all skipped numbers into a set
        skipped = {row[0] for row in cx.execute("SELECT roll_number FROM skipped_numbers")}

        # Start from last+1 and find the next number not in skipped
        next_num = last + 1
        while next_num in skipped:
            next_num += 1

        return next_num


# ---------- classes ----------
def add_class(name: str, order_index: int) -> None:
    """Create a class that displays in the given global order."""
    with _conn() as cx:
        cx.execute("INSERT INTO classes(name, order_index) VALUES(?, ?)", (name.strip(), order_index))


def remove_class(name: str) -> None:
    """Remove a class and everything under it (members, socials, family edges)."""
    with _conn() as cx:
        # delete socials for members in this class
        cx.execute("""
            DELETE FROM member_socials
            WHERE member_id IN (
                SELECT m.id FROM members m JOIN classes c ON m.class_id=c.id WHERE c.name=?
            )
        """, (name,))
        # delete family edges for members in this class (as child or as big)
        cx.execute("""
            DELETE FROM family
            WHERE member_id IN (
                SELECT m.id FROM members m JOIN classes c ON m.class_id=c.id WHERE c.name=?
            )
            OR big_id IN (
                SELECT m.id FROM members m JOIN classes c ON m.class_id=c.id WHERE c.name=?
            )
        """, (name, name))
        # delete members and class
        cx.execute("DELETE FROM members WHERE class_id=(SELECT id FROM classes WHERE name=?)", (name,))
        cx.execute("DELETE FROM classes WHERE name=?", (name,))


def list_classes() -> Iterable[tuple[int, str, int]]:
    with _conn() as cx:
        return cx.execute("SELECT id, name, order_index FROM classes ORDER BY order_index ASC").fetchall()


# ---------- members ----------
def add_member(class_name: str, first_name: str, last_name: str, nickname: str, bio: Optional[str] = None) -> None:
    """Add a member to a class; assigns next join_order in class and next global roll_number."""
    cid = _class_id(class_name)
    if cid is None:
        raise ValueError(f"Class '{class_name}' does not exist.")

    first_name = first_name.strip()
    last_name  = last_name.strip()
    nickname   = nickname.strip()

    with _conn() as cx:
        join_order = cx.execute(
            "SELECT COALESCE(MAX(join_order), 0) + 1 FROM members WHERE class_id=?", (cid,)
        ).fetchone()[0]
        roll_number = _next_roll_number()
        full = f"{first_name} {last_name}"

        cx.execute("""
            INSERT INTO members(class_id, first_name, last_name, nickname, full_name,
                                join_order, roll_number, bio)
            VALUES(?,?,?,?,?,?,?,?)
        """, (cid, first_name, last_name, nickname, full, join_order, roll_number, bio))

        cx.execute("""
    CREATE TABLE IF NOT EXISTS skipped_numbers (
        roll_number INTEGER PRIMARY KEY
    )
""")


def remove_member(nickname: str) -> None:
    """Remove a member everywhere by nickname."""
    mid = _member_id_by_nick(nickname)
    if mid is None:
        return
    with _conn() as cx:
        cx.execute("DELETE FROM member_socials WHERE member_id=?", (mid,))
        cx.execute("DELETE FROM family WHERE member_id=? OR big_id=?", (mid, mid))
        cx.execute("DELETE FROM members WHERE id=?", (mid,))


# ---------- roster fetch (RAW fields for bot formatting) ----------
def get_roster():
    """
    Returns rows grouped by class. Each row is:
        (class_name, first_name, nickname, last_name, roll_number, honorific)
    If a class has no members yet, first_name will be NULL (None in Python) for that row.
    Ordered by class order then join order within class.
    """
    with _conn() as cx:
        rows = cx.execute("""
            SELECT
                c.name       AS class_name,
                m.first_name AS first_name,
                m.nickname   AS nickname,
                m.last_name  AS last_name,
                m.roll_number,
                m.honorific
            FROM classes c
            LEFT JOIN members m ON m.class_id = c.id
            ORDER BY c.order_index ASC, m.join_order ASC
        """).fetchall()
    return rows


def get_class_roster(class_name: str):
    """
    Returns only the specified class. Each row is:
        (first_name, nickname, last_name, roll_number, honorific)
    Ordered by join order within that class.
    """
    with _conn() as cx:
        rows = cx.execute("""
            SELECT
                m.first_name,
                m.nickname,
                m.last_name,
                m.roll_number,
                m.honorific
            FROM members m
            JOIN classes c ON m.class_id = c.id
            WHERE c.name = ?
            ORDER BY m.join_order ASC
        """, (class_name,)).fetchall()
    return rows


# ---------- member detail / search ----------
def get_member_card_by(fields: dict):
    """
    Search by any of: number (int), first (str), last (str), nick (str).
    OR-logic across provided keys. Returns a dict or None.
    """
    where, args = [], []
    if fields.get("number") is not None:
        where.append("roll_number = ?"); args.append(int(fields["number"]))
    if fields.get("first"):
        where.append("LOWER(first_name) = LOWER(?)"); args.append(fields["first"])
    if fields.get("last"):
        where.append("LOWER(last_name) = LOWER(?)"); args.append(fields["last"])
    if fields.get("nick"):
        where.append("LOWER(nickname) = LOWER(?)"); args.append(fields["nick"])

    if not where:
        return None

    sql = f"""
      SELECT m.id, m.first_name, m.last_name, m.nickname, m.roll_number, m.honorific, m.bio, c.name
      FROM members m
      JOIN classes c ON m.class_id=c.id
      WHERE {' OR '.join(where)}
      LIMIT 1
    """
    with _conn() as cx:
        row = cx.execute(sql, tuple(args)).fetchone()
        if not row:
            return None
        (mid, first, last, nick, roll, honor, bio, classname) = row

        socials = dict(cx.execute(
            "SELECT platform, handle FROM member_socials WHERE member_id=?", (mid,)
        ).fetchall())

        # family
        big_row = cx.execute("SELECT big_id FROM family WHERE member_id=?", (mid,)).fetchone()
        big = None
        if big_row and big_row[0] is not None:
            br = cx.execute("SELECT nickname FROM members WHERE id=?", (big_row[0],)).fetchone()
            big = br[0] if br else None

        littles = [r[0] for r in cx.execute("""
            SELECT m.nickname
            FROM family f JOIN members m ON f.member_id=m.id
            WHERE f.big_id=?
        """, (mid,)).fetchall()]

    return {
        "first": first, "last": last, "nick": nick, "roll": roll, "honor": honor,
        "class": classname, "bio": bio, "socials": socials,
        "big": big, "littles": littles
    }


# ---------- socials ----------
def set_social(nickname: str, platform: str, handle: str) -> None:
    mid = _member_id_by_nick(nickname)
    if mid is None:
        raise ValueError("Member not found.")
    with _conn() as cx:
        cx.execute("""
            INSERT INTO member_socials(member_id, platform, handle)
            VALUES(?,?,?)
            ON CONFLICT(member_id, platform) DO UPDATE SET handle=excluded.handle
        """, (mid, platform.lower(), handle))


def remove_social(nickname: str, platform: str) -> None:
    mid = _member_id_by_nick(nickname)
    if mid is None:
        return
    with _conn() as cx:
        cx.execute("DELETE FROM member_socials WHERE member_id=? AND platform=?", (mid, platform.lower()))


# ---------- family ----------
def set_big(nickname: str, big_nickname: Optional[str]) -> None:
    """Set or clear a member's big (pass None/'' to clear)."""
    mid = _member_id_by_nick(nickname)
    if mid is None:
        raise ValueError("Member not found.")

    bid = None
    if big_nickname:
        bid = _member_id_by_nick(big_nickname)
        if bid is None:
            raise ValueError("Big not found.")

    with _conn() as cx:
        cx.execute("""
            INSERT INTO family(member_id, big_id) VALUES(?,?)
            ON CONFLICT(member_id) DO UPDATE SET big_id=excluded.big_id
        """, (mid, bid))


def get_big(nickname: str) -> Optional[str]:
    mid = _member_id_by_nick(nickname)
    if mid is None:
        return None
    with _conn() as cx:
        row = cx.execute("""
            SELECT m.nickname
            FROM family f JOIN members m ON f.big_id = m.id
            WHERE f.member_id=?
        """, (mid,)).fetchone()
        return row[0] if row else None


def get_littles(nickname: str) -> list[str]:
    mid = _member_id_by_nick(nickname)
    if mid is None:
        return []
    with _conn() as cx:
        return [r[0] for r in cx.execute("""
            SELECT m.nickname
            FROM family f JOIN members m ON f.member_id = m.id
            WHERE f.big_id=?
        """, (mid,)).fetchall()]

def add_skipped_number(number: int):
    with _conn() as cx:
        cx.execute("INSERT OR IGNORE INTO skipped_numbers (roll_number) VALUES (?)", (number,))
        cx.commit()

def remove_skipped_number(number: int):
    with _conn() as cx:
        cx.execute("DELETE FROM skipped_numbers WHERE roll_number = ?", (number,))
        cx.commit()

def get_skipped_numbers():
    with _conn() as cx:
        rows = cx.execute("SELECT roll_number FROM skipped_numbers ORDER BY roll_number ASC").fetchall()
    return [row[0] for row in rows]
