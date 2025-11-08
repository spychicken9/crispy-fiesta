# bot.py
import os
import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

import db

# ------------- CONFIG -------------
GUILD_ID = None                  # set to your server id (int) to sync faster; or leave None
ALLOWED_ROLES = {"President", "PD", "Technician"}  # who can add/remove/edit

# ------------- ENV / BOT -------------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()  # slash commands only; no message content needed
bot = commands.Bot(command_prefix="!", intents=intents)

# ------------- ANSI COLOR HELPERS -------------
def _ansi(color_256: int, text: str) -> str:
    return f"\x1b[38;5;{color_256}m{text}\x1b[0m"

ORANGE = 208
LIGHT_BLUE = 81

def format_member_line_colored(first: str, nick: str, last: str, roll: int, honor: str = "Mr.") -> str:
    num = _ansi(ORANGE, f"#{roll}")
    nick_col = _ansi(LIGHT_BLUE, f"“{nick}”")
    return f"{num} {honor} {first} {nick_col} {last}"

# ------------- PERMISSIONS -------------
def officer_only(interaction: discord.Interaction) -> bool:
    if interaction.user.guild_permissions.administrator:
        return True
    role_names = {r.name for r in getattr(interaction.user, "roles", [])}
    return bool(ALLOWED_ROLES & role_names)

# ------------- STARTUP -------------
@bot.event
async def on_ready():
    db.init_db()
    try:
        if GUILD_ID:
            await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
        else:
            await bot.tree.sync()
        print(f"Logged in as {bot.user} (synced commands).")
    except Exception as e:
        print("Sync error:", e)

# ------------- COMMANDS -------------

# Roster (full or a specific class)
@bot.tree.command(name="roster", description="List classes & members (optionally for a single class).")
@app_commands.describe(class_name="If provided, shows only this class.")
async def roster(interaction: discord.Interaction, class_name: str | None = None):
    try:
        # instant ack so Discord doesn’t time out
        await interaction.response.defer(thinking=True)

        # helper to send embed(s) after defer
        async def send_embeds(embeds: list[discord.Embed]):
            # followup is required after defer
            await interaction.followup.send(embeds=embeds)

        # single class path
        if class_name:
            rows = db.get_class_roster(class_name)  # should be (first,nick,last,roll,honor)
            if not rows:
                await interaction.followup.send(f"No members found for **{class_name}**.", ephemeral=True)
                return

            lines = [format_member_line_colored(f, n, l, r, h) for (f, n, l, r, h) in rows]
            desc = "```ansi\n" + "\n".join(lines) + "\n```"
            await send_embeds([discord.Embed(title=class_name, description=desc)])
            return

        # full roster path
        rows = db.get_roster()  # (class, first, nick, last, roll, honor)
        if not rows:
            await interaction.followup.send("No classes yet. Ask an officer to add some.")
            return

        embeds: list[discord.Embed] = []
        current_class: str | None = None
        cur_lines: list[str] = []

        def push_embed():
            nonlocal embeds, current_class, cur_lines
            if current_class is None:
                return
            desc = "```ansi\n" + ("\n".join(cur_lines) if cur_lines else "No members yet") + "\n```"
            embeds.append(discord.Embed(title=current_class, description=desc))
            cur_lines = []

        for cls, first, nick, last, roll, honor in rows:
            if cls != current_class:
                push_embed()
                current_class = cls
            if first is not None:
                cur_lines.append(format_member_line_colored(first, nick, last, roll, honor))

        push_embed()

        # Safety: Discord has a 6000-char limit per embed; if anything is huge, split
        if any(len(e.description or "") > 5500 for e in embeds):
            chunks: list[discord.Embed] = []
            for e in embeds:
                desc = e.description or ""
                if len(desc) <= 5500:
                    chunks.append(e)
                else:
                    body = desc.strip("`ansi\n").strip("`")
                    lines = body.splitlines()
                    buf: list[str] = []
                    cur = []
                    for ln in lines:
                        if len("\n".join(cur + [ln])) > 5400:
                            chunks.append(discord.Embed(title=e.title, description="```ansi\n" + "\n".join(cur) + "\n```"))
                            cur = []
                        cur.append(ln)
                    if cur:
                        chunks.append(discord.Embed(title=e.title, description="```ansi\n" + "\n".join(cur) + "\n```"))
            embeds = chunks

        await send_embeds(embeds)

    except Exception as err:
        # This will also be caught by the global handler, but we handle proactively
        print("Roster command failed:", repr(err))
        try:
            if interaction.response.is_done():
                await interaction.followup.send(f"Roster failed: {err}", ephemeral=True)
            else:
                await interaction.response.send_message(f"Roster failed: {err}", ephemeral=True)
        except Exception as e:
            print("Also failed to notify user about roster error:", e)

# Add/remove classes (restricted)
@bot.tree.command(name="add_class", description="Add a new class with a display order (lower appears earlier).")
@app_commands.describe(name="Class name", order_index="0,1,2,...")
async def add_class(interaction: discord.Interaction, name: str, order_index: int):
    if not officer_only(interaction):
        await interaction.response.send_message("Officers only (President/PD).", ephemeral=True); return
    try:
        db.add_class(name, order_index)
        await interaction.response.send_message(f"Added class **{name}** at order {order_index}.")
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="remove_class", description="Remove a class and all its members.")
async def remove_class(interaction: discord.Interaction, name: str):
    if not officer_only(interaction):
        await interaction.response.send_message("Officers only (President/PD).", ephemeral=True); return
    db.remove_class(name)
    await interaction.response.send_message(f"Removed class **{name}**.")

# Add/remove members (restricted)
@bot.tree.command(name="add_member", description="Add a member (auto join order & roll number).")
@app_commands.describe(class_name="Existing class", first_name="First name", last_name="Last name", nickname="Nickname", bio="Optional bio")
async def add_member(interaction: discord.Interaction, class_name: str, first_name: str, last_name: str, nickname: str, bio: str | None = None):
    if not officer_only(interaction):
        await interaction.response.send_message("Officers only (President/PD).", ephemeral=True); return
    try:
        db.add_member(class_name, first_name, last_name, nickname, bio)
        await interaction.response.send_message(f"Added **{first_name} “{nickname}” {last_name}** to **{class_name}**.")
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="remove_member", description="Remove a member by nickname.")
async def remove_member(interaction: discord.Interaction, nickname: str):
    if not officer_only(interaction):
        await interaction.response.send_message("Officers only (President/PD).", ephemeral=True); return
    db.remove_member(nickname)
    await interaction.response.send_message(f"Removed **{nickname}**.")

# Lookups & details
@bot.tree.command(name="lookup", description="Find a member by number, first name, nickname, or last name.")
@app_commands.describe(number="Roll number", first="First name", nick="Nickname", last="Last name")
async def lookup(interaction: discord.Interaction, number: int | None = None, first: str | None = None, nick: str | None = None, last: str | None = None):
    info = db.get_member_card_by({"number": number, "first": first, "nick": nick, "last": last})
    if not info:
        await interaction.response.send_message("No matching member found.", ephemeral=True); return

    # Title uses same colored line as roster
    title = format_member_line_colored(info["first"], info["nick"], info["last"], info["roll"], "Mr.")
    desc = [f"**Class:** {info['class']}"]
    if info["bio"]: desc.append(f"**Bio:** {info['bio']}")
    if info["big"]: desc.append(f"**Big:** {info['big']}")
    if info["littles"]: desc.append(f"**Littles:** " + ", ".join(info["littles"]))
    if info["socials"]:
        desc.append("**Socials:** " + " | ".join(f"{k.capitalize()}: {v}" for k, v in info["socials"].items()))

    # Title needs colors → put entire card into ansi block so colors render
    ansi_title = "```ansi\n" + title + "\n```"
    embed = discord.Embed(title=info["nick"])
    embed.description = ansi_title + "\n" + "\n".join(desc)
    await interaction.response.send_message(embed=embed)

# Family helpers (restricted for editing)
@bot.tree.command(name="set_big", description="Set or change a member's big (empty to clear).")
async def set_big(interaction: discord.Interaction, nickname: str, big_nickname: str | None):
    if not officer_only(interaction):
        await interaction.response.send_message("Officers only (President/PD).", ephemeral=True); return
    try:
        db.set_big(nickname, big_nickname if big_nickname else None)
        await interaction.response.send_message(f"Updated big for **{nickname}**.")
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="family", description="Show a member's immediate family: big and littles.")
async def family(interaction: discord.Interaction, nickname: str):
    big = db.get_big(nickname)
    littles = db.get_littles(nickname)
    lines = []
    if big: lines.append(f"**Big:** {big}")
    lines.append(f"**Member:** {nickname}")
    if littles: lines.append(f"**Littles:** " + ", ".join(littles))
    await interaction.response.send_message("\n".join(lines))

# Socials (restricted)
@bot.tree.command(name="set_social", description="Set a social handle for a member.")
@app_commands.describe(nickname="Member nickname", platform="instagram/x/linkedin/other", handle="@handle or URL")
async def set_social(interaction: discord.Interaction, nickname: str, platform: str, handle: str):
    if not officer_only(interaction):
        await interaction.response.send_message("Officers only (President/PD).", ephemeral=True); return
    try:
        db.set_social(nickname, platform, handle)
        await interaction.response.send_message(f"Saved {platform} for **{nickname}**.")
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="remove_social", description="Remove a social handle for a member.")
async def remove_social(interaction: discord.Interaction, nickname: str, platform: str):
    if not officer_only(interaction):
        await interaction.response.send_message("Officers only (President/PD).", ephemeral=True); return
    db.remove_social(nickname, platform)
    await interaction.response.send_message(f"Removed {platform} for **{nickname}**.")

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: Exception):
    # log to terminal
    print("APP CMD ERROR:", repr(error))
    # try to show something in Discord so it doesn't time out silently
    try:
        if interaction.response.is_done():
            await interaction.followup.send(f"Error: {error}", ephemeral=True)
        else:
            await interaction.response.send_message(f"Error: {error}", ephemeral=True)
    except Exception as e:
        print("Failed to notify user about error:", e)

@bot.tree.command(name="skip_number", description="Mark a roll number as skipped (blackballed).")
@app_commands.describe(number="The roll number to skip.")
async def skip_number(interaction: discord.Interaction, number: int):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("You don't have permission to do that.", ephemeral=True)
        return

    db.add_skipped_number(number)
    await interaction.response.send_message(f"Roll number #{number} has been marked as skipped.", ephemeral=True)


@bot.tree.command(name="unskip_number", description="Remove a roll number from the skipped list.")
@app_commands.describe(number="The roll number to unskip.")
async def unskip_number(interaction: discord.Interaction, number: int):
    if not await is_pd_or_president(interaction):
        await interaction.response.send_message("You don't have permission to do that.", ephemeral=True)
        return

    db.remove_skipped_number(number)
    await interaction.response.send_message(f"Roll number #{number} has been unskipped.", ephemeral=True)


# ------------- RUN -------------
bot.run(TOKEN)
