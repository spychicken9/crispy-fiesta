# bot.py
import os
import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Select
from discord import SelectOption

import db

# ---------- CONFIG ----------
TOKEN = os.environ.get("DISCORD_TOKEN")
GUILD_ID = os.environ.get("1047618310012936313")  # optional; set to force instant guild sync

# Officer roles allowed to modify roster
OFFICER_ROLES = {"President", "PD"}  # edit names to match your server

# Intents (Message Content not required for slash commands)
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents)

# ---------- PERMISSIONS ----------
def officer_only(interaction: discord.Interaction) -> bool:
    if interaction.user.guild_permissions.administrator:
        return True
    role_names = {r.name for r in getattr(interaction.user, "roles", [])}
    return bool(OFFICER_ROLES & role_names)

async def is_pd_or_president(interaction: discord.Interaction) -> bool:
    return officer_only(interaction)

# ---------- UTIL: colored line formatter ----------
# ANSI 256 colors: orange ~ 208, light blue ~ 117
def format_member_line_colored(first: str, nick: str, last: str, roll: int, honor: str) -> str:
    num = f"\x1b[38;5;208m#{roll}\x1b[0m"
    nn  = f"\x1b[38;5;117m“{nick}”\x1b[0m"
    return f"{num} {honor} {first} {nn} {last}"

# ---------- EVENTS ----------
@bot.event
async def on_ready():
    db.init_db()
    try:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            bot.tree.copy_global_to(guild=guild)
            await bot.tree.sync(guild=guild)
            print(f"Slash commands synced to guild {GUILD_ID}.")
        else:
            await bot.tree.sync()
            print("Slash commands synced.")
    except Exception as e:
        print("Command sync error:", e)
    print(f"Logged in as {bot.user}")

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: Exception):
    print("APP CMD ERROR:", repr(error))
    try:
        if interaction.response.is_done():
            await interaction.followup.send(f"Error: {error}", ephemeral=True)
        else:
            await interaction.response.send_message(f"Error: {error}", ephemeral=True)
    except Exception as e:
        print("Failed to notify user:", e)

# ---------- COMMANDS: Classes & Roster ----------
@bot.tree.command(name="add_class", description="(Officers) Add a class.")
@app_commands.describe(name="Class name", order_index="Display order (lower = earlier)")
async def add_class(interaction: discord.Interaction, name: str, order_index: int):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    try:
        db.add_class(name, order_index)
        await interaction.response.send_message(f"Class **{name}** added (order {order_index}).", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="remove_class", description="(Officers) Remove a class (and all members in it).")
@app_commands.describe(name="Class name")
async def remove_class(interaction: discord.Interaction, name: str):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    try:
        db.remove_class(name)
        await interaction.response.send_message(f"Class **{name}** removed.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="classes", description="Show all classes (debug).")
async def classes(interaction: discord.Interaction):
    rows = db.list_classes()
    if not rows:
        await interaction.response.send_message("No classes in DB.", ephemeral=True); return
    msg = "\n".join(f"{i}. **{name}** (order {ordx})" for i, (cid, name, ordx) in enumerate(rows, 1))
    await interaction.response.send_message(msg, ephemeral=True)

@bot.tree.command(name="add_member", description="(Officers) Add a member to a class.")
@app_commands.describe(class_name="Class name", first_name="First", last_name="Last", nickname="Nickname", bio="Optional bio")
async def add_member(interaction: discord.Interaction, class_name: str, first_name: str, last_name: str, nickname: str, bio: str | None = None):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    try:
        rn = db.add_member(class_name, first_name, last_name, nickname, bio=bio)
        await interaction.response.send_message(f"Added **#{rn} Mr. {first_name} “{nickname}” {last_name}** to {class_name}.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="remove_member", description="(Officers) Remove a member by nickname.")
@app_commands.describe(nickname="Nickname")
async def remove_member(interaction: discord.Interaction, nickname: str):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    db.remove_member(nickname)
    await interaction.response.send_message(f"Removed **{nickname}**.", ephemeral=True)

@bot.tree.command(name="roster", description="Show the roster (optionally for a single class).")
@app_commands.describe(class_name="If provided, shows only this class.")
async def roster(interaction: discord.Interaction, class_name: str | None = None):
    await interaction.response.defer(thinking=True)
    try:
        if class_name:
            rows = db.get_class_roster(class_name)  # (first, nick, last, roll, honor)
            if not rows:
                await interaction.followup.send(f"No members found for **{class_name}**.", ephemeral=True); return
            lines = [format_member_line_colored(f, n, l, r, h) for (f, n, l, r, h) in rows]
            desc = "```ansi\n" + "\n".join(lines) + "\n```"
            await interaction.followup.send(embed=discord.Embed(title=class_name, description=desc))
            return

        rows = db.get_roster()  # (class, first, nick, last, roll, honor)
        if not rows:
            await interaction.followup.send("No classes yet. Ask an officer to add some.", ephemeral=True); return

        embeds, cur_class, buf = [], None, []
        def push():
            nonlocal buf, cur_class, embeds
            if cur_class is None: return
            desc = "```ansi\n" + ("\n".join(buf) if buf else "No members yet") + "\n```"
            embeds.append(discord.Embed(title=cur_class, description=desc))
            buf = []

        for cls, first, nick, last, roll, honor in rows:
            if cls != cur_class:
                push(); cur_class = cls
            if first is not None:
                buf.append(format_member_line_colored(first, nick, last, roll, honor))
        push()

        final = []
        for e in embeds:
            d = e.description or ""
            if len(d) <= 5500:
                final.append(e); continue
            lines = d.strip("`ansi\n").strip("`").splitlines()
            chunk = []
            for ln in lines:
                if len("\n".join(chunk + [ln])) > 5400:
                    final.append(discord.Embed(title=e.title, description="```ansi\n" + "\n".join(chunk) + "\n```"))
                    chunk = []
                chunk.append(ln)
            if chunk:
                final.append(discord.Embed(title=e.title, description="```ansi\n" + "\n".join(chunk) + "\n```"))

        for i in range(0, len(final), 10):
            await interaction.followup.send(embeds=final[i:i+10])

    except Exception as err:
        print("Roster failed:", repr(err))
        await interaction.followup.send(f"Roster failed: {err}", ephemeral=True)

# ---------- COMMANDS: Family & Socials ----------
@bot.tree.command(name="set_big", description="(Officers) Set a member's big (nickname).")
@app_commands.describe(nickname="Member", big_nickname="Big (empty to clear)")
async def set_big(interaction: discord.Interaction, nickname: str, big_nickname: str | None = None):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    try:
        db.set_big(nickname, big_nickname)
        msg = f"Set **{nickname}**'s big to **{big_nickname}**." if big_nickname else f"Cleared big for **{nickname}**."
        await interaction.response.send_message(msg, ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="set_social", description="(Officers) Set a social handle.")
@app_commands.describe(nickname="Member nickname", platform="instagram/x/linkedin/other", handle="Handle or URL")
async def set_social(interaction: discord.Interaction, nickname: str, platform: str, handle: str):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    try:
        db.set_social(nickname, platform, handle)
        await interaction.response.send_message(f"Set {platform} for **{nickname}**.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="remove_social", description="(Officers) Remove a social handle.")
@app_commands.describe(nickname="Member nickname", platform="Platform")
async def remove_social(interaction: discord.Interaction, nickname: str, platform: str):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    db.remove_social(nickname, platform)
    await interaction.response.send_message(f"Removed {platform} for **{nickname}**.", ephemeral=True)

# ---------- COMMANDS: Skipped numbers ----------
@bot.tree.command(name="skip_number", description="(Officers) Mark a roll number as skipped (blackballed).")
@app_commands.describe(number="Number to skip")
async def skip_number(interaction: discord.Interaction, number: int):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    db.add_skipped_number(number)
    await interaction.response.send_message(f"Roll number **#{number}** marked as skipped.", ephemeral=True)

@bot.tree.command(name="unskip_number", description="(Officers) Remove a number from skipped list.")
@app_commands.describe(number="Number to unskip")
async def unskip_number(interaction: discord.Interaction, number: int):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    db.remove_skipped_number(number)
    await interaction.response.send_message(f"Roll number **#{number}** unskipped.", ephemeral=True)

# ---------- COMMANDS: Display-only reordering (OFFICERS ONLY) ----------
@bot.tree.command(name="swap_display", description="(Officers) Swap two brothers' display positions (numbers stay the same).")
@app_commands.describe(number_a="Roll number of first brother", number_b="Roll number of second brother")
async def swap_display(interaction: discord.Interaction, number_a: int, number_b: int):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only (PD/President).", ephemeral=True); return
    try:
        db.swap_display_positions(number_a, number_b)
        await interaction.response.send_message(
            f"Swapped display positions of **#{number_a}** and **#{number_b}** (roll numbers unchanged).",
            ephemeral=True
        )
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="move_display", description="(Officers) Move a brother to appear right AFTER another (numbers stay the same).")
@app_commands.describe(number="Brother to move (roll number)", target_after="Place him after this roll number")
async def move_display(interaction: discord.Interaction, number: int, target_after: int):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only (PD/President).", ephemeral=True); return
    try:
        db.move_display_after(number, target_after)
        await interaction.response.send_message(
            f"Moved **#{number}** to appear after **#{target_after}** (roll numbers unchanged).",
            ephemeral=True
        )
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

# ---------- COMMANDS: Profiles / Names (OFFICERS ONLY) ----------
@bot.tree.command(name="edit_name", description="(Officers) Edit a member's name fields.")
@app_commands.describe(
    nickname="Existing nickname to identify the member",
    first_name="New first name (optional)",
    last_name="New last name (optional)",
    new_nickname="New nickname (optional)",
    honorific="Honorific (e.g., Mr., Ms., Mx.) (optional)"
)
async def edit_name(interaction: discord.Interaction,
                    nickname: str,
                    first_name: str | None = None,
                    last_name: str | None = None,
                    new_nickname: str | None = None,
                    honorific: str | None = None):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only (PD/President).", ephemeral=True); return
    try:
        db.update_member_name(nickname, first_name=first_name, last_name=last_name,
                              new_nickname=new_nickname, honorific=honorific)
        new_n = new_nickname if new_nickname else nickname
        await interaction.response.send_message(f"Updated name for **{new_n}**.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="edit_profile", description="(Officers) Edit profile fields (any subset).")
@app_commands.describe(
    nickname="Member nickname",
    major="Major", age="Age", ethnicity="Ethnicity",
    hometown="Hometown", discord_handle="Discord handle (e.g., @user)"
)
async def edit_profile(interaction: discord.Interaction, nickname: str,
                       major: str | None = None, age: int | None = None,
                       ethnicity: str | None = None, hometown: str | None = None,
                       discord_handle: str | None = None):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only (PD/President).", ephemeral=True); return
    try:
        db.update_member_profile(nickname, major=major, age=age, ethnicity=ethnicity,
                                 hometown=hometown, discord_handle=discord_handle)
        await interaction.response.send_message(f"Updated profile for **{nickname}**.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="edit_social", description="(Officers) Add or update a member's social handle.")
@app_commands.describe(
    nickname="Member nickname",
    platform="instagram / x / linkedin / other",
    handle="Handle or URL (e.g., @name or https://...)"
)
async def edit_social(interaction: discord.Interaction, nickname: str, platform: str, handle: str):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only (PD/President).", ephemeral=True); return
    try:
        db.set_social(nickname, platform, handle)  # upsert
        await interaction.response.send_message(f"Updated **{platform}** for **{nickname}**.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

# ---------- COMMANDS: Lookup (PUBLIC) ----------
@bot.tree.command(name="lookup", description="Find a brother by number, name, or nickname.")
@app_commands.describe(number="Roll number", first="First name", nick="Nickname", last="Last name")
async def lookup(interaction: discord.Interaction,
                 number: int | None = None, first: str | None = None,
                 nick: str | None = None, last: str | None = None):
    # PUBLIC (not ephemeral)
    await interaction.response.defer(ephemeral=False)

    matches = db.lookup_members(first=first, last=last, nick=nick, number=number)
    if not matches:
        await interaction.followup.send("No matching brothers found.")
        return

    def build_embed(roll, f, n, l, classname):
        info = db.get_member_card_by({"number": roll})
        title_line = format_member_line_colored(info["first"], info["nick"], info["last"], info["roll"], info["honor"])
        ansi_title = "```ansi\n" + title_line + "\n```"

        lines = [f"**Class:** {info['class']}"]
        if info.get("major"):     lines.append(f"**Major:** {info['major']}")
        if info.get("age"):       lines.append(f"**Age:** {info['age']}")
        if info.get("ethnicity"): lines.append(f"**Ethnicity:** {info['ethnicity']}")
        if info.get("hometown"):  lines.append(f"**Hometown:** {info['hometown']}")
        if info.get("discord"):   lines.append(f"**Discord:** {info['discord']}")
        if info.get("big"):       lines.append(f"**Big:** {info['big']}")
        if info.get("littles"):   lines.append(f"**Littles:** " + ", ".join(info["littles"]))
        if info.get("socials"):   lines.append("**Socials:** " + " | ".join(f"{k.capitalize()}: {v}" for k, v in info["socials"].items()))
        if info.get("bio"):       lines.append(f"**Bio:** {info['bio']}")

        e = discord.Embed(title=f"#{info['roll']} Mr. {info['first']} “{info['nick']}” {info['last']}",
                          description=ansi_title + "\n" + "\n".join(lines))
        return e

    if len(matches) == 1:
        r, f, n, l, classname = matches[0]
        await interaction.followup.send(embed=build_embed(r, f, n, l, classname))
        return

    options = [SelectOption(label=f"#{r} {f} “{n}” {l} — {classname}", value=str(r)) for (r, f, n, l, classname) in matches]

    class PickBrother(Select):
        def __init__(self):
            super().__init__(placeholder="Select a brother", options=options, min_values=1, max_values=1)

        async def callback(self, select_interaction: discord.Interaction):
            chosen_roll = int(self.values[0])
            r, f, n, l, classname = next(t for t in matches if t[0] == chosen_roll)
            await select_interaction.response.edit_message(embed=build_embed(r, f, n, l, classname), view=None)

    view = View()
    view.add_item(PickBrother())
    await interaction.followup.send("Multiple matches found. Please choose:", view=view)


# === IMPORT / EXPORT (Officers) ===
@bot.tree.command(name="import_roster", description="(Officers) Import roster from an Excel/CSV attachment (Contact sheet).")
@app_commands.describe(file="Attach .xlsx or .csv", clear_existing="Erase current DB first", create_missing="Create members that are not found", default_class="Class name for newly created members")
async def import_roster(interaction: discord.Interaction,
                        file: discord.Attachment,
                        clear_existing: bool = False,
                        create_missing: bool = True,
                        default_class: str = "Imported"):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in (".xlsx",".xls",".csv"):
            await interaction.followup.send("Please upload a .xlsx, .xls, or .csv file.", ephemeral=True); return
        temp_path = f"/data/_import{ext}"
        data = await file.read()
        with open(temp_path, "wb") as f:
            f.write(data)

        if ext in (".xlsx",".xls"):
            df = pd.read_excel(temp_path, sheet_name="Contact")
        else:
            df = pd.read_csv(temp_path)

        db.import_roster_dataframe(df, clear_existing=clear_existing, create_missing=create_missing, default_class=default_class)
        await interaction.followup.send("Roster imported successfully ✅", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"Import error: {e}", ephemeral=True)

@bot.tree.command(name="export_roster", description="(Officers) Export the roster as an Excel file.")
async def export_roster(interaction: discord.Interaction):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("Officers only.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        df = db.export_roster_dataframe()
        out_path = "/data/roster_export.xlsx"
        df.to_excel(out_path, index=False)
        await interaction.followup.send(file=discord.File(out_path, filename="roster_export.xlsx"), ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"Export error: {e}", ephemeral=True)

# ---------- MAIN ----------
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN is not set")
    bot.run(TOKEN)
