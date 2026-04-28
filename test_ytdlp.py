import yt_dlp
import logging
logging.basicConfig(level=logging.DEBUG)
ydl_opts = {
    "format": "bestvideo[height<=720]+bestaudio/best",
    "quiet": False,
    "verbose": True,
    "extractor_args": {"youtube": {"player_client": ["web", "mweb", "android", "ios"]}},
}
with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    try:
        info = ydl.extract_info("https://www.youtube.com/watch?v=7LBc-RGorQw", download=False)
        print("SUCCESS title:", info.get("title"))
    except Exception as e:
        print("ERROR:", e)
