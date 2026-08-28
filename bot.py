"""
re:TT Checker & Downloader — Discord bot
Fetches TikTok video analytics + real quality data and posts a styled embed.

Data sources / honesty notes:
- Views, Likes, Comments, Shares, Favorites, ID, Region -> REAL, from tikwm.com's
  public TikTok API (no official TikTok API key needed).
- Quality (resolution, bitrate, codec, file size) -> REAL, obtained by downloading
  the video via tikwm's direct link and probing it with ffprobe.
- Shadow ban -> HEURISTIC ONLY. TikTok exposes no official "shadow ban" flag.
  This bot approximates it by checking whether the video is discoverable via
  TikTok's public hashtag/keyword search for one of its own hashtags. This is
  not authoritative.
- VQ Score -> HEURISTIC ONLY (invented). Computed from bitrate + resolution as a
  rough proxy, 0-100. Not an official TikTok metric.
- Categories -> HEURISTIC ONLY. Guessed from hashtags/caption keywords via a
  small local keyword map. Not TikTok's own classification.
"""

import os
import re
import io
import asyncio
import tempfile
import subprocess
from datetime import datetime, timezone

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv(override=True)  # .env always wins over any stray session variable

TIKWM_API = "https://www.tikwm.com/api/"
DISCORD_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")

intents = discord.Intents.default()
intents.message_content = True  # required to read link text in normal messages
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

TIKTOK_URL_RE = re.compile(
    r"https?://(?:www\.|vt\.|vm\.)?tiktok\.com/\S+", re.IGNORECASE
)

# --- crude keyword -> category map (heuristic, not TikTok's real classifier) ---
CATEGORY_KEYWORDS = {
    "Movies & TV works": ["movie", "film", "series", "tvshow", "goldberg", "you", "netflix"],
    "Entertainment Culture": ["edit", "viral", "fyp", "trend", "meme"],
    "Entertainment": [],  # fallback bucket, always included if nothing else matches strongly
    "Music": ["song", "music", "sound", "remix", "dj"],
    "Sports": ["football", "basketball", "soccer", "nba", "sport"],
    "Comedy": ["funny", "comedy", "lol", "joke"],
}


def guess_categories(caption: str) -> list[str]:
    caption_l = caption.lower()
    hits = []
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in caption_l for kw in kws):
            hits.append(cat)
    if not hits:
        hits = ["Entertainment"]
    elif "Entertainment" not in hits and len(hits) < 2:
        hits.append("Entertainment")
    return hits[:3]


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
        return "Unknown"
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
    fps_label = f"{fps}fps" if fps else "fps: N/A"
    return f"{res} • {fps_label}"


def vq_score(bitrate_mbps: float | None, height: int | None) -> int:
    """Invented 0-100 proxy score. Not an official TikTok metric."""
    if not bitrate_mbps or not height:
        return 0
    score = min(100, round((bitrate_mbps / 15) * 60 + (height / 2880) * 40))
    return max(0, score)


# tikwm can give up to 3 distinct video URLs for the same post: a
# no-watermark "standard" one, a no-watermark "HD" one, and a watermarked
# one. They don't always differ in resolution (often 2 of the 3 point to
# the same file), but sometimes all 3 really are different renditions
# (e.g. 540p / 720p / 1080p) — so we probe every *distinct* URL we're
# given instead of assuming there are always exactly two tiers.
QUALITY_SOURCES = [
    ("🌐 Browser (SD)", "play"),
    ("📱 Phone (HD)", "hdplay"),
    ("💧 Watermarked", "wmplay"),
]


async def probe_all_sources(meta: dict) -> list[tuple[str, str, dict]]:
    """Probe every distinct quality URL tikwm returned. If two labels point
    at the same URL (common — e.g. hdplay falls back to play) they're
    merged into one combined label so we don't probe or display duplicates."""
    labels_by_url: dict[str, list[str]] = {}
    for label, key in QUALITY_SOURCES:
        url = meta.get(key)
        if url:
            labels_by_url.setdefault(url, []).append(label)

    urls = list(labels_by_url.keys())
    if not urls:
        return []

    qualities = await asyncio.gather(*(probe_remote(u) for u in urls))

    return [
        (" / ".join(labels_by_url[url]), url, quality)
        for url, quality in zip(urls, qualities)
    ]


async def check_shadow_ban(hashtag: str, video_id: str) -> str:
    """Best-effort heuristic: search TikTok's public hashtag feed via tikwm and
    see if this video id shows up. NOT authoritative — many legit videos won't
    appear for unrelated reasons (recency, ranking, region)."""
    if not hashtag:
        return "Unknown"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://www.tikwm.com/api/challenge/videos",
                params={"challenge_name": hashtag, "count": 30},
                timeout=15,
            ) as resp:
                data = await resp.json()
        videos = data.get("data", {}).get("videos", [])
        found = any(str(v.get("video_id")) == str(video_id) for v in videos)
        return "No" if found else "Possibly (not found in hashtag feed)"
    except Exception:
        return "Unknown"


def build_embed(meta: dict, sources: list[tuple[str, str, dict]], original_quality: dict, shadow: str, categories: list[str]) -> discord.Embed:
    author = meta.get("author", {})
    title = meta.get("title", "")
    created = datetime.fromtimestamp(meta.get("create_time", 0), tz=timezone.utc)

    embed = discord.Embed(
        title="🎬 VIDEO • ANALYTICS",
        color=0x1DA1F2,
    )
    embed.set_author(name=f"👤 {author.get('nickname', author.get('unique_id', 'unknown'))}")

    embed.add_field(
        name="\u200b",
        value=f"📅 {created.strftime('%d %B %Y, %H:%M:%S')}\n> {title}\n🎵 {meta.get('music_info', {}).get('title', 'original sound')}",
        inline=False,
    )

    stats = (
        f"👁 **{meta.get('play_count', 0):,} Views**\n"
        f"♡ **{meta.get('digg_count', 0):,} Likes**\n"
        f"💬 **{meta.get('comment_count', 0):,} Comments**\n"
        f"⭐ **{meta.get('collect_count', 0):,} Favorites**\n"
        f"↗ **{meta.get('share_count', 0):,} Shares**\n"
        f"⬇ **{meta.get('download_count', 0):,} Downloads**"
    )
    embed.add_field(name="📊 Statistics", value=stats, inline=False)

    info = (
        f"🆔 **ID** | `{meta.get('id')}`\n"
        f"📡 **Source** | Browser\n"
        f"📍 **Region** | {meta.get('region', '??')}\n"
        f"🚫 **Shadow ban** | {shadow}"
    )
    embed.add_field(name="ℹ️ Information", value=info, inline=False)

    tier_lines = "\n".join(
        f"{label} | {short_quality_label(q.get('width'), q.get('height'), q.get('fps'))}"
        for label, _url, q in sources
    )
    q = (
        f"{tier_lines}\n"
        f"🌐 **Resolution** | {original_quality.get('width')}x{original_quality.get('height')}\n"
        f"📦 **Codec** | {original_quality.get('codec')}\n"
        f"📶 **Bitrate** | {original_quality.get('bitrate_mbps')} Mbps\n"
        f"💾 **Size** | {original_quality.get('size_mb')} MB\n"
        f"🏆 **VQ Score (approx.)** | {vq_score(original_quality.get('bitrate_mbps'), original_quality.get('height'))}"
    )
    embed.add_field(name="✨ Quality", value=q, inline=False)

    embed.add_field(name="📂 Categories (guessed)", value="\n".join(f"| {c}" for c in categories), inline=False)

    embed.set_footer(text="MOON TIKTOK VIDEO CHECKER")
    return embed


async def analyze_tiktok_url(url: str) -> discord.Embed:
    """Runs the full analytics + quality pipeline for one TikTok URL and
    returns the finished embed. Shared by both the /check command and the
    auto-reply on_message listener, so behavior stays identical either way."""
    meta = await fetch_tikwm(url)

    caption = meta.get("title", "")
    hashtag_match = re.findall(r"#(\w+)", caption)

    sources, shadow = await asyncio.gather(
        probe_all_sources(meta),
        check_shadow_ban(hashtag_match[0] if hashtag_match else "", meta.get("id")),
    )

    if not sources:
        raise RuntimeError("tikwm-მ ვერცერთი ვიდეო ლინკი ვერ დააბრუნა")

    # Full download (for exact size/bitrate) only on whichever detected
    # source has the tallest short-side resolution — skips redundant
    # downloads of the lower tiers.
    _best_label, best_url, _best_q = max(
        sources,
        key=lambda s: min(s[2].get("width") or 0, s[2].get("height") or 0),
    )

    original_quality = await probe_video(best_url)
    categories = guess_categories(caption)

    return build_embed(meta, sources, original_quality, shadow, categories)


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
