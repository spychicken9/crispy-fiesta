# db.py — SQLite storage and Excel import/export for the fraternity bot.

import os
import re
import sqlite3
from pathlib import Path
from typing import Optional, Iterable

import pandas as pd

# On Railway, mount a volume at /data and set DB_PATH=/data/roster.sqlite3
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
    with _conn() as cx:
        # Classes (pledge classes) — ordered globally
        cx.execute("""
            CREATE TABLE IF NOT EXISTS classes(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                order_index INTEGER NOT NULL
            );
        """)

        # Members (join_order is REAL so we can place 0.5 then renormalize)
        cx.execute("""
            CREATE TABLE IF NOT EXISTS members(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                class_id     INTEGER NOT NULL,
                first_name   TEXT    NOT NULL,
                last_name    TEXT    NOT NULL,
                nickname     TEXT    NOT NULL,
                full_name    TEXT,
                join_order   REAL    NOT NULL,
                roll_number  INTEGER UNIQUE,
                honorific    TEXT NOT NULL DEFAULT 'Mr.',
                bio          TEXT,

                -- profile core
                major        TEXT,
                age          INTEGER,
                ethnicity    TEXT,
                hometown     TEXT,
                discord_handle TEXT,

                -- NEW: from Excel Contact sheet
                phone        TEXT,
                su_email     TEXT,
                personal_email TEXT,
                su_id        TEXT,
                standing     TEXT,
                shirt_size   TEXT,
                birthday     TEXT,        -- store as text YYYY-MM-DD
                lineage      TEXT,
                personality16 TEXT,
                love_language TEXT,
                fascination_advantage TEXT,
                notes        TEXT,
                interest     TEXT,

                FOREIGN KEY(class_id) REFERENCES classes(id),
                UNIQUE(class_id, nickname)
            );
        """)

        # Socials
        cx.execute("""
            CREATE TABLE IF NOT EXISTS member_socials(
                member_id INTEGER NOT NULL,
                platform  TEXT    NOT NULL,
                handle    TEXT    NOT NULL,
                PRIMARY KEY(member_id, platform),
                FOREIGN KEY(member_id) REFERENCES members(id)
            );
        """)

        # Family (big/little via reverse lookup of 'big_id')
        cx.execute("""
            CREATE TABLE IF NOT EXISTS family(
                member_id INTEGER PRIMARY KEY,
                big_id    INTEGER,
                FOREIGN KEY(member_id) REFERENCES members(id),
                FOREIGN KEY(big_id)    REFERENCES members(id)
            );
        """)

        # Skipped/blackballed roll numbers
        cx.execute("""
            CREATE TABLE IF NOT EXISTS skipped_numbers(
                roll_number INTEGER PRIMARY KEY
            );
        """)

    # Idempotent adds (future safe)
    for col, decl in [
        ("major","TEXT"),("age","INTEGER"),("ethnicity","TEXT"),("hometown","TEXT"),("discord_handle","TEXT"),
        ("phone","TEXT"),("su_email","TEXT"),("personal_email","TEXT"),("su_id","TEXT"),("standing","TEXT"),
        ("shirt_size","TEXT"),("birthday","TEXT"),("lineage","TEXT"),("personality16","TEXT"),
        ("love_language","TEXT"),("fascination_advantage","TEXT"),("notes","TEXT"),("interest","TEXT"),
    ]:
        _add_column_if_missing("members", col, decl)

# ---------- helpers ----------
def _class_id(name: str) -> Optional[int]:
    with _conn() as cx:
        row = cx.execute("SELECT id FROM classes WHERE name=?", (name,)).fetchone()
        return row[0] if row else None

def _ensure_class(name: str) -> int:
    cid = _class_id(name)
    if cid is not None:
        return cid
    with _conn() as cx:
        next_idx = cx.execute("SELECT COALESCE(MAX(order_index), 0) + 1 FROM classes").fetchone()[0]
        cx.execute("INSERT INTO classes(name, order_index) VALUES(?,?)", (name.strip(), next_idx))
        cx.commit()
    return _class_id(name)

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
    """Next roll number (start at #2), skip blackballed."""
    with _conn() as cx:
        last = cx.execute("SELECT MAX(roll_number) FROM members").fetchone()[0]
        if last is None:
            last = 1
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
    cid = _ensure_class(class_name)
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

# ---------- roster fetch ----------
def get_roster():
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

# ---------- lookup / card ----------
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
             c.name, m.major, m.age, m.ethnicity, m.hometown, m.discord_handle,
             m.phone, m.su_email, m.personal_email, m.su_id, m.standing, m.shirt_size,
             m.birthday, m.lineage, m.personality16, m.love_language, m.fascination_advantage,
             m.notes, m.interest
      FROM members m JOIN classes c ON m.class_id=c.id
      WHERE {' OR '.join(where)}
      LIMIT 1
    """
    with _conn() as cx:
        row = cx.execute(sql, tuple(args)).fetchone()
        if not row:
            return None
        (mid, first, last, nick, roll, honor, bio, classname,
         major, age, ethnicity, hometown, discord_handle,
         phone, su_email, personal_email, su_id, standing, shirt_size,
         birthday, lineage, personality16, love_language, fascination_advantage,
         notes, interest) = row

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
        "major": major, "age": age, "ethnicity": ethnicity, "hometown": hometown, "discord": discord_handle,
        "phone": phone, "su_email": su_email, "personal_email": personal_email, "su_id": su_id,
        "standing": standing, "shirt_size": shirt_size, "birthday": birthday, "lineage": lineage,
        "personality16": personality16, "love_language": love_language,
        "fascination_advantage": fascination_advantage, "notes": notes, "interest": interest
    }

# ---------- profiles / socials / family ----------
def update_member_profile(nickname: str,
                          major: str | None = None, age: int | None = None,
                          ethnicity: str | None = None, hometown: str | None = None,
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

def update_member_name(nickname: str,
                       first_name: str | None = None,
                       last_name:  str | None = None,
                       new_nickname: str | None = None,
                       honorific: str | None = None):
    mid = _member_id_by_nick(nickname)
    if mid is None:
        raise ValueError("Member not found.")
    with _conn() as cx:
        cur = cx.execute("SELECT first_name, last_name FROM members WHERE id=?", (mid,)).fetchone()
        cur_first, cur_last = cur[0], cur[1]
        new_first = first_name if first_name is not None else cur_first
        new_last  = last_name  if last_name  is not None else cur_last
        new_full  = f"{new_first} {new_last}"
        sets, args = [], []
        if first_name is not None:  sets.append("first_name=?");  args.append(first_name)
        if last_name  is not None:  sets.append("last_name=?");   args.append(last_name)
        if new_nickname is not None: sets.append("nickname=?");   args.append(new_nickname)
        if honorific is not None:   sets.append("honorific=?");   args.append(honorific)
        if first_name is not None or last_name is not None:
            sets.append("full_name=?"); args.append(new_full)
        if not sets: return
        args.append(mid)
        cx.execute(f"UPDATE members SET {', '.join(sets)} WHERE id=?", tuple(args))
        cx.commit()

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

# ---------- display-only reordering ----------
def _member_core_by_roll(roll_number: int):
    with _conn() as cx:
        row = cx.execute(
            "SELECT id, class_id, join_order, nickname FROM members WHERE roll_number=?",
            (roll_number,)
        ).fetchone()
        return row

def _renormalize_join_order(class_id: int):
    with _conn() as cx:
        rows = cx.execute(
            "SELECT id FROM members WHERE class_id=? ORDER BY join_order ASC, id ASC",
            (class_id,)
        ).fetchall()
        for i, (mid,) in enumerate(rows, start=1):
            cx.execute("UPDATE members SET join_order=? WHERE id=?", (i, mid))
        cx.commit()

def swap_display_positions(number_a: int, number_b: int):
    a = _member_core_by_roll(number_a)
    b = _member_core_by_roll(number_b)
    if not a or not b:
        raise ValueError("Both roll numbers must exist.")
    a_id, a_cid, a_ord, _ = a
    b_id, b_cid, b_ord, _ = b
    if a_cid != b_cid:
        raise ValueError("Members must be in the same class to swap display positions.")
    with _conn() as cx:
        cx.execute("UPDATE members SET join_order=? WHERE id=?", (-1, a_id))
        cx.execute("UPDATE members SET join_order=? WHERE id=?", (a_ord, b_id))
        cx.execute("UPDATE members SET join_order=? WHERE id=?", (b_ord, a_id))
        cx.commit()
    _renormalize_join_order(a_cid)

def move_display_after(number: int, target_after: int):
    src = _member_core_by_roll(number)
    tgt = _member_core_by_roll(target_after)
    if not src or not tgt:
        raise ValueError("Both roll numbers must exist.")
    s_id, s_cid, s_ord, _ = src
    t_id, t_cid, t_ord, _ = tgt
    if s_cid != t_cid:
        raise ValueError("Members must be in the same class to move display order.")
    with _conn() as cx:
        cx.execute("UPDATE members SET join_order=? WHERE id=?", (t_ord + 0.5, s_id))
        cx.commit()
    _renormalize_join_order(s_cid)

# ---------- Excel import/export ----------
_CONTACT_MAP = {
    # Excel column name (case-insensitive) -> member field
    "last name": "last_name",
    "first name": "first_name",
    "phone": "phone",
    "syracuse email": "su_email",
    "personal (calendar)": "personal_email",
    "su id": "su_id",
    "nickname": "nickname",
    "standing": "standing",
    "major": "major",
    "ethnicity": "ethnicity",
    "hometown": "hometown",
    "shirt size": "shirt_size",
    "birthday": "birthday",
    "lineage": "lineage",
    "16 personalities": "personality16",
    "love language": "love_language",
    "fascination advantage": "fascination_advantage",
    "notes": "notes",
    "interest": "interest",
}

def _clean_phone(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    s = str(v)
    digits = re.sub(r"\D", "", s)
    if not digits: return None
    return digits

def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: str(c).strip().lower(): c for c in df.columns})

def import_roster_dataframe(df: pd.DataFrame, clear_existing: bool = False, create_missing: bool = True, default_class: str = "Imported"):
    # Use Contact-like headers (case-insensitive)
    df = _normalize_headers(df)
    lower_cols = {c.lower(): c for c in df.columns}

    # Basic presence check
    required = {"first name", "last name", "nickname"}
    if not required.issubset(lower_cols.keys()):
        raise ValueError(f"Missing required columns: {required - set(lower_cols.keys())}")

    if clear_existing:
        with _conn() as cx:
            cx.execute("DELETE FROM member_socials")
            cx.execute("DELETE FROM family")
            cx.execute("DELETE FROM members")
            cx.commit()

    cid_default = _ensure_class(default_class)

    with _conn() as cx:
        for _, row in df.iterrows():
            rec = {}
            for src_lower, field in _CONTACT_MAP.items():
                if src_lower in lower_cols:
                    rec[field] = row[lower_cols[src_lower]]

            first = str(rec.get("first_name") or row[lower_cols["first name"]]).strip()
            last  = str(rec.get("last_name")  or row[lower_cols["last name"]]).strip()
            nick  = str(rec.get("nickname")   or row[lower_cols["nickname"]]).strip()
            if not first or not last or not nick:
                continue

            phone = _clean_phone(rec.get("phone"))

            # match existing by nickname OR (first+last)
            existing = cx.execute("""
                SELECT id, class_id FROM members
                WHERE LOWER(nickname)=LOWER(?) OR (LOWER(first_name)=LOWER(?) AND LOWER(last_name)=LOWER(?))
                LIMIT 1
            """, (nick, first, last)).fetchone()

            if existing:
                mid, class_id = existing
                sets, args = [], []
                # Always keep full_name consistent
                sets.append("full_name=?"); args.append(f"{first} {last}")
                for k in ["first_name","last_name","nickname","phone","su_email","personal_email","su_id",
                          "standing","major","ethnicity","hometown","shirt_size","birthday","lineage",
                          "personality16","love_language","fascination_advantage","notes","interest"]:
                    if k in rec:
                        val = rec[k]
                        sets.append(f"{k}=?"); args.append(val)
                if phone is not None:
                    sets.append("phone=?"); args.append(phone)
                args.append(mid)
                cx.execute(f"UPDATE members SET {', '.join(sets)} WHERE id=?", tuple(args))
            else:
                if not create_missing:
                    continue
                # create in default class with next roll number and appended join_order
                jo = cx.execute("SELECT COALESCE(MAX(join_order), 0) + 1 FROM members WHERE class_id=?", (cid_default,)).fetchone()[0]
                roll_number = _next_roll_number()
                cx.execute("""
                    INSERT INTO members(class_id, first_name, last_name, nickname, full_name,
                                        join_order, roll_number,
                                        phone, su_email, personal_email, su_id, standing,
                                        major, ethnicity, hometown, shirt_size, birthday, lineage,
                                        personality16, love_language, fascination_advantage, notes, interest,
                                        honorific)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (cid_default, first, last, nick, f"{first} {last}",
                      jo, roll_number,
                      phone, rec.get("su_email"), rec.get("personal_email"), rec.get("su_id"), rec.get("standing"),
                      rec.get("major"), rec.get("ethnicity"), rec.get("hometown"), rec.get("shirt_size"),
                      rec.get("birthday"), rec.get("lineage"),
                      rec.get("personality16"), rec.get("love_language"), rec.get("fascination_advantage"),
                      rec.get("notes"), rec.get("interest"),
                      "Mr."))
        cx.commit()

    # Renormalize each class after bulk changes
    with _conn() as cx:
        cids = [r[0] for r in cx.execute("SELECT id FROM classes").fetchall()]
    for cid in cids:
        _renormalize_join_order(cid)

def import_roster_from_path(path: str, **kwargs):
    ext = Path(path).suffix.lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, sheet_name="Contact")
    elif ext == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError("Unsupported file type. Use .xlsx or .csv")
    import_roster_dataframe(df, **kwargs)

def export_roster_dataframe() -> pd.DataFrame:
    with _conn() as cx:
        rows = cx.execute("""
          SELECT m.roll_number, c.name as class_name, m.first_name, m.nickname, m.last_name,
                 m.honorific, m.bio, m.major, m.age, m.ethnicity, m.hometown, m.discord_handle,
                 m.phone, m.su_email, m.personal_email, m.su_id, m.standing, m.shirt_size, m.birthday,
                 m.lineage, m.personality16, m.love_language, m.fascination_advantage, m.notes, m.interest,
                 m.id
          FROM members m JOIN classes c ON m.class_id=c.id
          ORDER BY m.roll_number ASC
        """).fetchall()
    df = pd.DataFrame(rows, columns=[
        "roll_number","class_name","first_name","nickname","last_name",
        "honorific","bio","major","age","ethnicity","hometown","discord_handle",
        "phone","su_email","personal_email","su_id","standing","shirt_size","birthday",
        "lineage","personality16","love_language","fascination_advantage","notes","interest",
        "member_id"
    ])
    # socials
    with _conn() as cx:
        socials_map = {}
        for mid in df["member_id"].tolist():
            s = dict(cx.execute("SELECT platform, handle FROM member_socials WHERE member_id=?", (mid,)).fetchall())
            socials_map[mid] = s
        bigs = []
        for mid in df["member_id"].tolist():
            r = cx.execute("SELECT m.nickname FROM family f JOIN members m ON f.big_id=m.id WHERE f.member_id=?", (mid,)).fetchone()
            bigs.append(r[0] if r else None)
    df.drop(columns=["member_id"], inplace=True)
    df["big_nickname"] = bigs
    # explode common socials if present
    for plat in ("instagram","x","linkedin","other"):
        df[plat] = [socials_map.get(mid, {}).get(plat) for mid in []]  # noop placeholder
    return df