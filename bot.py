"""
re:TT Checker & Downloader — Discord bot
Fetches TikTok video analytics + real quality data and posts a styled embed.

Data sources / honesty notes:
- Views, Likes, Comments, Shares, Favorites, ID, Region -> REAL, from tikwm.com's
  public TikTok API (no official TikTok API key needed).
- Resolution/fps/codec -> REAL, from a lightweight remote ffprobe (reads only
  the header tikwm's CDN serves, no full download needed).
- File size & bitrate -> REAL, taken directly from tikwm's own metadata
  (size/hd_size/wm_size + duration) instead of a HEAD request or full
  download, since some CDNs don't reliably answer HEAD with Content-Length.
  Falls back to a full download + ffprobe only if tikwm didn't provide a
  size for that tier.
- Shadow ban -> HEURISTIC ONLY. TikTok exposes no official "shadow ban" flag.
  This bot approximates it from the like-count vs. view-count relationship:
  very few likes on a video with real reach is a classic shadowban symptom,
  while healthy like counts read as "not shadowbanned". This is not
  authoritative, and the middle ground is reported as unknown rather than
  guessed.
- VQ Score -> HEURISTIC ONLY (invented). Computed from bitrate + resolution as a
  rough proxy, 0-100. Not an official TikTok metric.
"""

import os
import re
import io
import asyncio
import tempfile
import subprocess
from datetime import datetime, timezone, timedelta

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv(override=True)  # .env always wins over any stray session variable

TIKWM_API = "https://www.tikwm.com/api/"
DISCORD_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")

# Georgia is a fixed UTC+4 offset year-round (no daylight saving), so we
# convert timestamps explicitly here rather than relying on Discord's
# client-side auto-localization, which wasn't matching for this server.
GEORGIA_TZ = timezone(timedelta(hours=4))

intents = discord.Intents.default()
intents.message_content = True  # required to read link text in normal messages
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

TIKTOK_URL_RE = re.compile(
    r"https?://(?:www\.|vt\.|vm\.)?tiktok\.com/\S+", re.IGNORECASE
)


async def fetch_tikwm(url: str) -> dict:
    # hd=1 is required or tikwm leaves "hdplay" empty/duplicate-of-SD, which
    # was silently hiding the real HD (e.g. 1080p) tier.
    async with aiohttp.ClientSession() as session:
        async with session.get(TIKWM_API, params={"url": url, "hd": "1"}, timeout=20) as resp:
            data = await resp.json()
    if data.get("code") != 0:
        raise RuntimeError(data.get("msg", "tikwm lookup failed"))
    return data["data"]


def _parse_fraction(value: str) -> int | None:
    if not value or "/" not in value:
        return None
    num, den = value.split("/")
    if num.isdigit() and den.isdigit() and int(den) != 0:
        return round(int(num) / int(den))
    return None


async def probe_remote(video_url: str) -> dict:
    """Probe width/height/fps/codec/bitrate directly from the URL — ffprobe
    reads only the header it needs over HTTP, no full download. Much faster
    than downloading the whole file, but doesn't give an exact file size.

    r_frame_rate is unreliable on some tikwm CDN sources (moov atom placement
    means ffprobe sometimes can't see enough of the stream over a plain HTTP
    header read), so we ask for avg_frame_rate too and fall back to it, and
    we bump probesize/analyzeduration so ffprobe reads enough of the stream
    to find frame timing info at all."""
    cmd = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-probesize", "10000000", "-analyzeduration", "10000000",
        "-show_entries", "stream=width,height,codec_name,bit_rate,r_frame_rate,avg_frame_rate",
        "-of", "csv=p=0", video_url,
    ]
    out = await asyncio.to_thread(
        subprocess.run, cmd, capture_output=True, text=True, timeout=25
    )
    parts = out.stdout.strip().split(",")
    codec_name = parts[0] if len(parts) > 0 else "unknown"
    width = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    height = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
    bitrate = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

    fps = _parse_fraction(parts[4]) if len(parts) > 4 else None
    if not fps:
        fps = _parse_fraction(parts[5]) if len(parts) > 5 else None

    return {
        "width": width,
        "height": height,
        "codec": codec_name,
        "bitrate_mbps": round(bitrate / 1_000_000, 1) if bitrate else None,
        "size_mb": None,
        "fps": fps,
    }


async def probe_video(video_url: str) -> dict:
    """Download the video to a temp file and run ffprobe on it for real
    resolution / bitrate / codec / size / fps — used once, on the highest
    quality source, so the 'Size' stat is an exact file size."""
    async with aiohttp.ClientSession() as session:
        async with session.get(video_url, timeout=60) as resp:
            raw = await resp.read()

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
        f.write(raw)
        path = f.name

    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height,codec_name,bit_rate,r_frame_rate,avg_frame_rate",
            "-of", "csv=p=0", path,
        ]
        out = await asyncio.to_thread(
            subprocess.run, cmd, capture_output=True, text=True, timeout=30
        )
        parts = out.stdout.strip().split(",")
        codec_name = parts[0] if len(parts) > 0 else "unknown"
        width = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        height = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
        bitrate = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

        fps = _parse_fraction(parts[4]) if len(parts) > 4 else None
        if not fps:
            fps = _parse_fraction(parts[5]) if len(parts) > 5 else None

        size_bytes = len(raw)
        bitrate_mbps = round(bitrate / 1_000_000, 1) if bitrate else None

        if bitrate_mbps is None:
            # Some mp4 muxers don't write a per-stream bit_rate at all, so
            # fall back to computing it from file size / duration instead
            # of just showing "None".
            dur_cmd = [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "csv=p=0", path,
            ]
            dur_out = await asyncio.to_thread(
                subprocess.run, dur_cmd, capture_output=True, text=True, timeout=15
            )
            try:
                duration = float(dur_out.stdout.strip())
                if duration > 0:
                    bitrate_mbps = round((size_bytes * 8 / duration) / 1_000_000, 1)
            except ValueError:
                pass

        return {
            "width": width,
            "height": height,
            "codec": codec_name,
            "bitrate_mbps": bitrate_mbps,
            "size_mb": round(size_bytes / (1024 * 1024), 1),
            "fps": fps,
        }
    finally:
        os.remove(path)


def short_quality_label(width: int | None, height: int | None, fps: int | None) -> str:
    """Build a compact label like '4K60' or '1080p30' from raw stream info.
    Uses the SHORTER of width/height so portrait TikTok videos (e.g.
    1080x1920) are classified by their conventional resolution (1080p),
    not the taller pixel count."""
    if not width or not height:
        return "უცნობი"
    short_side = min(width, height)
    if short_side >= 2160:
        res = "4K"
    elif short_side >= 1440:
        res = "2K"
    elif short_side >= 1080:
        res = "1080p"
    elif short_side >= 720:
        res = "720p"
    else:
        res = f"{short_side}p"
    fps_label = f"{fps}fps" if fps else "fps: უცნობია"
    return f"{res} • {fps_label}"


def resolution_only_label(width: int | None, height: int | None) -> str:
    """Same short-side bucketing as short_quality_label but without the fps
    suffix — used where fps is already shown elsewhere on screen and
    repeating it would just add length that causes mobile line-wrapping."""
    if not width or not height:
        return "უცნობი"
    short_side = min(width, height)
    if short_side >= 2160:
        return "4K"
    elif short_side >= 1440:
        return "2K"
    elif short_side >= 1080:
        return "1080p"
    elif short_side >= 720:
        return "720p"
    return f"{short_side}p"


def vq_score(bitrate_mbps: float | None, height: int | None) -> int | None:
    """Invented 0-100 proxy score. Not an official TikTok metric.
    Returns None (rendered as '?') when we don't actually have bitrate/height
    data, instead of silently showing a 0 that looks like a real bad score."""
    if bitrate_mbps is None or height is None:
        return None
    score = min(100, round((bitrate_mbps / 15) * 60 + (height / 2880) * 40))
    return max(0, score)


# tikwm can give up to 3 distinct video URLs for the same post: a
# no-watermark "standard" one, a no-watermark "HD" one, and a watermarked
# one. They don't always differ in resolution (often 2 of the 3 point to
# the same file), but sometimes all 3 really are different renditions
# (e.g. 540p / 720p / 1080p) — so we probe every *distinct* URL we're
# given instead of assuming there are always exactly two tiers.
QUALITY_SOURCES = [
    ("🌐 SD", "play"),
    ("📱 HD", "hdplay"),
    ("💧 WM", "wmplay"),
]

# tikwm returns an exact byte size directly in its JSON for each quality
# tier — no need to HEAD or download the file just to learn its size, and
# unlike a HEAD request this always works, even on CDNs that don't answer
# HEAD with a Content-Length header.
SIZE_KEYS = {
    "play": "size",
    "hdplay": "hd_size",
    "wmplay": "wm_size",
}


async def probe_all_sources(meta: dict) -> list[tuple[str, str, dict]]:
    """Probe every distinct quality URL tikwm returned. If two labels point
    at the same URL (common — e.g. hdplay falls back to play) they're
    merged into one combined label so we don't probe or display duplicates.

    Resolution/fps/codec come from a lightweight remote ffprobe (header read
    only). Size and bitrate come straight from tikwm's own metadata, which
    is what actually fixes the "0.0Mbps / 0.0MB" bug — a HEAD-based Content-
    Length lookup was unreliable, and computing bitrate off it (or off a
    tikwm size of 0) could previously round down to a misleading 0.0."""
    labels_by_url: dict[str, list[str]] = {}
    keys_by_url: dict[str, list[str]] = {}
    for label, key in QUALITY_SOURCES:
        url = meta.get(key)
        if url:
            labels_by_url.setdefault(url, []).append(label)
            keys_by_url.setdefault(url, []).append(key)

    urls = list(labels_by_url.keys())
    if not urls:
        return []

    qualities = await asyncio.gather(*(probe_remote(u) for u in urls))

    duration = meta.get("duration") or 0
    results = []
    for url, quality in zip(urls, qualities):
        size_bytes = None
        for key in keys_by_url[url]:
            candidate = meta.get(SIZE_KEYS.get(key, ""))
            if candidate:
                size_bytes = candidate
                break
        if size_bytes and duration:
            quality["size_mb"] = round(size_bytes / (1024 * 1024), 1)
            quality["bitrate_mbps"] = round((size_bytes * 8 / duration) / 1_000_000, 1)
        results.append((" / ".join(labels_by_url[url]), url, quality))
    return results


# Tunable thresholds for the shadow-ban heuristic below.
SHADOW_BAN_HIGH_LIKES = 50      # this many likes (or more) reads as healthy engagement -> NO
SHADOW_BAN_LOW_LIKES = 5        # this many likes (or fewer) is suspicious -> possible YES
SHADOW_BAN_MIN_VIEWS = 1000     # ...but only counts as suspicious once there's real reach to begin with


def estimate_shadow_ban(meta: dict) -> str:
    """HEURISTIC ONLY. TikTok exposes no official "shadow ban" flag.
    A video with real reach (views) but almost no likes is a classic
    shadowban symptom — the algorithm is technically showing it but
    engagement/discovery is being suppressed. A video with a healthy like
    count is almost certainly not shadowbanned. Anything in between is
    genuinely ambiguous, so this reports "unknown" there rather than
    guessing."""
    views = meta.get("play_count", 0) or 0
    likes = meta.get("digg_count", 0) or 0

    if likes >= SHADOW_BAN_HIGH_LIKES:
        return "NO"
    if likes <= SHADOW_BAN_LOW_LIKES and views >= SHADOW_BAN_MIN_VIEWS:
        return "YES"
    return "უცნობია"


def _fmt(value, suffix: str = "") -> str:
    """Render a possibly-missing numeric value without ever printing a bare
    'None' — shows '?' instead so it's visually obvious the data is missing,
    rather than looking like a real (bad) measurement."""
    return f"{value}{suffix}" if value is not None else "?"


def build_embed(meta: dict, sources: list[tuple[str, str, dict]], original_quality: dict, shadow: str) -> discord.Embed:
    author = meta.get("author", {})
    title = meta.get("title", "")
    created_utc = datetime.fromtimestamp(meta.get("create_time", 0), tz=timezone.utc)
    created_local = created_utc.astimezone(GEORGIA_TZ)

    embed = discord.Embed(
        title="🎬 ვიდეო ანალიტიკა",
        description=f"📅 {created_local.strftime('%d %B %Y, %H:%M:%S')}\n> {title}\n🎵 {meta.get('music_info', {}).get('title', 'ორიგინალი ხმა')}",
        color=0x1DA1F2,
    )

    author_kwargs = {"name": author.get("nickname", author.get("unique_id", "უცნობი"))}
    avatar = author.get("avatar_medium") or author.get("avatar_thumb") or author.get("avatar_larger")
    if avatar:
        author_kwargs["icon_url"] = avatar
    embed.set_author(**author_kwargs)

    thumb = meta.get("origin_cover") or meta.get("cover") or meta.get("ai_dynamic_cover")
    if thumb:
        embed.set_thumbnail(url=thumb)

    stats = (
        f"• 👁 **{meta.get('play_count', 0):,}** ნახვა\n"
        f"• ♡ **{meta.get('digg_count', 0):,}** მოწონება\n"
        f"• 💬 **{meta.get('comment_count', 0):,}** კომენტარი\n"
        f"• 🔖 **{meta.get('collect_count', 0):,}** ფავორიტი\n"
        f"• ↗ **{meta.get('share_count', 0):,}** გაზიარება\n"
        f"• ⬇ **{meta.get('download_count', 0):,}** ჩამოტვირთვა"
    )
    embed.add_field(name="📊 სტატისტიკა", value=stats, inline=False)

    info = (
        f"• 🆔 | `{meta.get('id')}`\n"
        f"• 📍 **რეგიონი** | {meta.get('region', '??')}\n"
        f"• 👻 | {shadow}"
    )
    embed.add_field(name="ℹ️ ინფორმაცია", value=info, inline=False)

    tier_lines = "\n".join(
        f"• {label} | {short_quality_label(q.get('width'), q.get('height'), q.get('fps'))}"
        f" • {_fmt(q.get('bitrate_mbps'), 'Mbps')} • {_fmt(q.get('size_mb'), 'MB')}"
        for label, _url, q in sources
    )
    best_res = resolution_only_label(original_quality.get("width"), original_quality.get("height"))
    quote_block = (
        f"> {best_res} • {_fmt(original_quality.get('bitrate_mbps'), 'Mbps')} • "
        f"{original_quality.get('codec')} • {_fmt(original_quality.get('size_mb'), 'MB')}"
    )
    vq = vq_score(original_quality.get("bitrate_mbps"), original_quality.get("height"))
    q = (
        f"{tier_lines}\n\n"
        f"{quote_block}\n\n"
        f"**Original** | {original_quality.get('width')}x{original_quality.get('height')}\n"
        f"**VQ Score** | {_fmt(vq)}"
    )
    embed.add_field(name="✨ ხარისხი", value=q, inline=False)

    embed.set_footer(text="⚡ MOON TIKTOK VIDEO CHECKER")
    return embed


async def analyze_tiktok_url(url: str) -> discord.Embed:
    """Runs the full analytics + quality pipeline for one TikTok URL and
    returns the finished embed. Shared by both the /check command and the
    auto-reply on_message listener, so behavior stays identical either way."""
    meta = await fetch_tikwm(url)

    sources = await probe_all_sources(meta)
    shadow = estimate_shadow_ban(meta)

    if not sources:
        raise RuntimeError("tikwm-მ ვერცერთი ვიდეო ლინკი ვერ დააბრუნა")

    # "Original" = whichever detected source has the tallest short-side
    # resolution. Its size_mb/bitrate_mbps already came straight from
    # tikwm's own metadata inside probe_all_sources — no extra network
    # round-trip needed here.
    best_label, best_url, best_quality = max(
        sources,
        key=lambda s: min(s[2].get("width") or 0, s[2].get("height") or 0),
    )

    original_quality = best_quality
    if original_quality.get("size_mb") is None or original_quality.get("bitrate_mbps") is None:
        # Rare fallback: tikwm didn't give us a usable size/duration for this
        # tier, so fall back to a real download + ffprobe rather than
        # showing a blank/zero value.
        original_quality = await probe_video(best_url)

    return build_embed(meta, sources, original_quality, shadow)


@tree.command(name="check", description="TikTok ვიდეოს ანალიტიკის შემოწმება")
@app_commands.describe(url="TikTok ვიდეოს ლინკი")
async def check(interaction: discord.Interaction, url: str):
    await interaction.response.defer()

    if not TIKTOK_URL_RE.search(url):
        await interaction.followup.send("ეს არ ჰგავს TikTok-ის ლინკს.")
        return

    try:
        embed = await analyze_tiktok_url(url)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"შეცდომა: `{e}`")


@bot.event
async def on_message(message: discord.Message):
    # Ignore the bot's own messages / other bots to avoid loops.
    if message.author.bot:
        return

    match = TIKTOK_URL_RE.search(message.content)
    if match:
        url = match.group(0)
        async with message.channel.typing():
            try:
                embed = await analyze_tiktok_url(url)
                await message.reply(embed=embed, mention_author=False)
            except Exception as e:
                await message.reply(f"შეცდომა: `{e}`", mention_author=False)

    # Keep prefix commands (if any get added later) working.
    await bot.process_commands(message)


@bot.event
async def on_ready():
    await tree.sync()
    print(f"Logged in as {bot.user}")


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("Set DISCORD_BOT_TOKEN environment variable before running.")
    bot.run(DISCORD_TOKEN)