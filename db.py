# db.py
# SQLite storage and helpers for the fraternity bot.

import os
import sqlite3
from pathlib import Path
from typing import Optional, Iterable

# On Railway set: DB_PATH=/data/roster.sqlite3 (persistent volume mounted at /data)
DB_PATH = Path(os.environ.get("DB_PATH", "roster.sqlite3"))


# ---------- connection ----------
def _conn() -> sqlite3.Connection:
    cx = sqlite3.connect(DB_PATH)
    return cx


# ---------- schema ----------
def _add_column_if_missing(table: str, col: str, decl: str):
    with _conn() as cx:
        cols = [r[1] for r in cx.execute(f"PRAGMA table_info({table})").fetchall()]
        if col not in cols:
            cx.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
            cx.commit()


def init_db() -> None:
    """Create tables if they don't exist, and add new columns idempotently."""
    with _conn() as cx:
        # Classes: global ordered list
        cx.execute("""
            CREATE TABLE IF NOT EXISTS classes(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                order_index INTEGER NOT NULL
            );
        """)

        # Members
        cx.execute("""
            CREATE TABLE IF NOT EXISTS members(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                class_id   INTEGER NOT NULL,
                first_name TEXT    NOT NULL,
                last_name  TEXT    NOT NULL,
                nickname   TEXT    NOT NULL,
                full_name  TEXT,
                join_order INTEGER NOT NULL,
                roll_number INTEGER UNIQUE,
                honorific  TEXT NOT NULL DEFAULT 'Mr.',
                bio        TEXT DEFAULT NULL,
                FOREIGN KEY(class_id) REFERENCES classes(id),
                UNIQUE(class_id, nickname)
            );
        """)

        # Member socials
        cx.execute("""
            CREATE TABLE IF NOT EXISTS member_socials(
                member_id INTEGER NOT NULL,
                platform  TEXT    NOT NULL,
                handle    TEXT    NOT NULL,
                PRIMARY KEY(member_id, platform),
                FOREIGN KEY(member_id) REFERENCES members(id)
            );
        """)

        # Family (one big; multiple littles via reverse lookup)
        cx.execute("""
            CREATE TABLE IF NOT EXISTS family(
                member_id INTEGER PRIMARY KEY,
                big_id    INTEGER,
                FOREIGN KEY(member_id) REFERENCES members(id),
                FOREIGN KEY(big_id)    REFERENCES members(id)
            );
        """)

        # Skipped/blackballed numbers (never reassigned)
        cx.execute("""
            CREATE TABLE IF NOT EXISTS skipped_numbers(
                roll_number INTEGER PRIMARY KEY
            );
        """)

    # Profile fields (added safely)
    _add_column_if_missing("members", "major", "TEXT")
    _add_column_if_missing("members", "age", "INTEGER")
    _add_column_if_missing("members", "ethnicity", "TEXT")
    _add_column_if_missing("members", "hometown", "TEXT")
    _add_column_if_missing("members", "discord_handle", "TEXT")


# ---------- id helpers ----------
def _class_id(name: str) -> Optional[int]:
    with _conn() as cx:
        row = cx.execute("SELECT id FROM classes WHERE name=?", (name,)).fetchone()
        return row[0] if row else None


def _member_id_by_nick(nick: str) -> Optional[int]:
    with _conn() as cx:
        row = cx.execute("SELECT id FROM members WHERE LOWER(nickname)=LOWER(?)", (nick,)).fetchone()
        return row[0] if row else None


# ---------- skipped numbers ----------
def add_skipped_number(number: int):
    with _conn() as cx:
        cx.execute("INSERT OR IGNORE INTO skipped_numbers(roll_number) VALUES(?)", (number,))
        cx.commit()


def remove_skipped_number(number: int):
    with _conn() as cx:
        cx.execute("DELETE FROM skipped_numbers WHERE roll_number=?", (number,))
        cx.commit()


def get_skipped_numbers() -> list[int]:
    with _conn() as cx:
        rows = cx.execute("SELECT roll_number FROM skipped_numbers ORDER BY roll_number ASC").fetchall()
    return [r[0] for r in rows]


# ---------- roll numbering ----------
def _next_roll_number() -> int:
    """Next roll number, starting at 2, skipping blackballed numbers."""
    with _conn() as cx:
        last = cx.execute("SELECT MAX(roll_number) FROM members").fetchone()[0]
        if last is None:
            last = 1  # start at 2
        skipped = {r[0] for r in cx.execute("SELECT roll_number FROM skipped_numbers").fetchall()}
        n = last + 1
        while n in skipped:
            n += 1
        return n


# ---------- classes ----------
def add_class(name: str, order_index: int) -> None:
    with _conn() as cx:
        cx.execute("INSERT INTO classes(name, order_index) VALUES(?, ?)", (name.strip(), order_index))
        cx.commit()


def remove_class(name: str) -> None:
    with _conn() as cx:
        # Remove socials/family for members in this class
        cx.execute("""
            DELETE FROM member_socials
            WHERE member_id IN (
              SELECT m.id FROM members m JOIN classes c ON m.class_id=c.id WHERE c.name=?
            )
        """, (name,))
        cx.execute("""
            DELETE FROM family
            WHERE member_id IN (
              SELECT m.id FROM members m JOIN classes c ON m.class_id=c.id WHERE c.name=?
            )
            OR big_id IN (
              SELECT m.id FROM members m JOIN classes c ON m.class_id=c.id WHERE c.name=?
            )
        """, (name, name))
        cx.execute("DELETE FROM members WHERE class_id=(SELECT id FROM classes WHERE name=?)", (name,))
        cx.execute("DELETE FROM classes WHERE name=?", (name,))
        cx.commit()


def list_classes() -> Iterable[tuple[int, str, int]]:
    with _conn() as cx:
        return cx.execute("SELECT id, name, order_index FROM classes ORDER BY order_index ASC").fetchall()


# ---------- members ----------
def add_member(class_name: str, first_name: str, last_name: str, nickname: str, bio: Optional[str] = None) -> int:
    cid = _class_id(class_name)
    if cid is None:
        raise ValueError(f"Class '{class_name}' does not exist.")

    first_name, last_name, nickname = first_name.strip(), last_name.strip(), nickname.strip()
    with _conn() as cx:
        join_order = cx.execute("SELECT COALESCE(MAX(join_order), 0) + 1 FROM members WHERE class_id=?", (cid,)).fetchone()[0]
        roll_number = _next_roll_number()
        full = f"{first_name} {last_name}"
        cx.execute("""
            INSERT INTO members(class_id, first_name, last_name, nickname, full_name, join_order, roll_number, bio)
            VALUES(?,?,?,?,?,?,?,?)
        """, (cid, first_name, last_name, nickname, full, join_order, roll_number, bio))
        cx.commit()
        return roll_number


def remove_member(nickname: str) -> None:
    mid = _member_id_by_nick(nickname)
    if mid is None:
        return
    with _conn() as cx:
        cx.execute("DELETE FROM member_socials WHERE member_id=?", (mid,))
        cx.execute("DELETE FROM family WHERE member_id=? OR big_id=?", (mid, mid))
        cx.execute("DELETE FROM members WHERE id=?", (mid,))
        cx.commit()


# ---------- roster fetch (RAW fields) ----------
def get_roster():
    """
    Rows grouped by class:
    (class_name, first_name, nickname, last_name, roll_number, honorific)
    If a class has no members yet, first_name is NULL for that row.
    """
    with _conn() as cx:
        rows = cx.execute("""
            SELECT c.name, m.first_name, m.nickname, m.last_name, m.roll_number, m.honorific
            FROM classes c
            LEFT JOIN members m ON m.class_id=c.id
            ORDER BY c.order_index ASC, m.join_order ASC
        """).fetchall()
    return rows


def get_class_roster(class_name: str):
    with _conn() as cx:
        rows = cx.execute("""
            SELECT m.first_name, m.nickname, m.last_name, m.roll_number, m.honorific
            FROM members m
            JOIN classes c ON m.class_id=c.id
            WHERE c.name=?
            ORDER BY m.join_order ASC
        """, (class_name,)).fetchall()
    return rows


# ---------- lookups / cards ----------
def lookup_members(first=None, last=None, nick=None, number=None):
    q = ("SELECT m.roll_number, m.first_name, m.nickname, m.last_name, c.name "
         "FROM members m JOIN classes c ON m.class_id=c.id WHERE 1=1")
    args = []
    if first:   q += " AND LOWER(m.first_name)=LOWER(?)"; args.append(first)
    if last:    q += " AND LOWER(m.last_name)=LOWER(?)";  args.append(last)
    if nick:    q += " AND LOWER(m.nickname)=LOWER(?)";   args.append(nick)
    if number:  q += " AND m.roll_number=?";              args.append(number)
    q += " ORDER BY m.roll_number ASC"
    with _conn() as cx:
        return cx.execute(q, tuple(args)).fetchall()


def get_member_card_by(fields: dict):
    where, args = [], []
    if fields.get("number") is not None:
        where.append("m.roll_number = ?"); args.append(int(fields["number"]))
    if fields.get("first"):
        where.append("LOWER(m.first_name) = LOWER(?)"); args.append(fields["first"])
    if fields.get("last"):
        where.append("LOWER(m.last_name) = LOWER(?)"); args.append(fields["last"])
    if fields.get("nick"):
        where.append("LOWER(m.nickname) = LOWER(?)"); args.append(fields["nick"])
    if not where:
        return None

    sql = f"""
      SELECT m.id, m.first_name, m.last_name, m.nickname, m.roll_number, m.honorific, m.bio,
             c.name, m.major, m.age, m.ethnicity, m.hometown, m.discord_handle
      FROM members m JOIN classes c ON m.class_id=c.id
      WHERE {' OR '.join(where)}
      LIMIT 1
    """
    with _conn() as cx:
        row = cx.execute(sql, tuple(args)).fetchone()
        if not row:
            return None
        (mid, first, last, nick, roll, honor, bio, classname,
         major, age, ethnicity, hometown, discord_handle) = row

        socials = dict(cx.execute("SELECT platform, handle FROM member_socials WHERE member_id=?", (mid,)).fetchall())

        big_row = cx.execute("SELECT big_id FROM family WHERE member_id=?", (mid,)).fetchone()
        big = None
        if big_row and big_row[0] is not None:
            br = cx.execute("SELECT nickname FROM members WHERE id=?", (big_row[0],)).fetchone()
            big = br[0] if br else None

        littles = [r[0] for r in cx.execute("""
            SELECT m.nickname FROM family f JOIN members m ON f.member_id=m.id
            WHERE f.big_id=?
        """, (mid,)).fetchall()]

    return {
        "first": first, "last": last, "nick": nick, "roll": roll, "honor": honor,
        "class": classname, "bio": bio, "socials": socials,
        "big": big, "littles": littles,
        "major": major, "age": age, "ethnicity": ethnicity,
        "hometown": hometown, "discord": discord_handle
    }


def update_member_profile(nickname: str,
                          major: str | None = None,
                          age: int | None = None,
                          ethnicity: str | None = None,
                          hometown: str | None = None,
                          discord_handle: str | None = None):
    mid = _member_id_by_nick(nickname)
    if mid is None:
        raise ValueError("Member not found.")
    sets, args = [], []
    if major is not None: sets.append("major=?"); args.append(major)
    if age is not None: sets.append("age=?"); args.append(int(age))
    if ethnicity is not None: sets.append("ethnicity=?"); args.append(ethnicity)
    if hometown is not None: sets.append("hometown=?"); args.append(hometown)
    if discord_handle is not None: sets.append("discord_handle=?"); args.append(discord_handle)
    if not sets: return
    args.append(mid)
    with _conn() as cx:
        cx.execute(f"UPDATE members SET {', '.join(sets)} WHERE id=?", tuple(args))
        cx.commit()


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
        cx.commit()


def remove_social(nickname: str, platform: str) -> None:
    mid = _member_id_by_nick(nickname)
    if mid is None:
        return
    with _conn() as cx:
        cx.execute("DELETE FROM member_socials WHERE member_id=? AND platform=?", (mid, platform.lower()))
        cx.commit()


# ---------- family ----------
def set_big(nickname: str, big_nickname: Optional[str]) -> None:
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
        cx.commit()


# ---------- swapping ----------
def reorder_member_swap(old_number: int, new_number: int):
    """Swap the members occupying two roll numbers (keep roll numbers fixed)."""
    with _conn() as cx:
        a = cx.execute("SELECT id FROM members WHERE roll_number=?", (old_number,)).fetchone()
        b = cx.execute("SELECT id FROM members WHERE roll_number=?", (new_number,)).fetchone()

        if not a or not b:
            raise ValueError("Both roll numbers must exist to swap members.")

        a_id, b_id = a[0], b[0]

        # Use a temporary placeholder to avoid UNIQUE constraint conflicts
        temp = -999999
        cx.execute("UPDATE members SET roll_number=? WHERE id=?", (temp, a_id))
        cx.execute("UPDATE members SET roll_number=? WHERE id=?", (old_number, b_id))
        cx.execute("UPDATE members SET roll_number=? WHERE id=?", (new_number, a_id))
        cx.commit()
