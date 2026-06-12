#!/usr/bin/env python3
"""VideoMasjid daily pipeline report -> EmailIt SMTP.

Reads pipeline truth from Postgres (Supabase) and renders a plain-language
HTML email. Designed to run once a day (09:00 WIB).

Environment variables:
  DATABASE_URL        postgres connection string (read-only role is fine)
  EMAILIT_API_KEY     EmailIt API key (used as the SMTP password)   [required to send]
  SMTP_HOST           default smtp.emailit.com
  SMTP_PORT           default 587 (STARTTLS/TLS)
  SMTP_USER           default emailit
  REPORT_FROM         e.g. "VideoMasjid <alerts@your-verified-domain.com>"  [required to send]
  REPORT_TO           default kantorjualsfp@gmail.com
  DASHBOARD_URL       link shown in the footer

Usage:
  python daily_report.py --dry-run   # writes report.html, does not send
  python daily_report.py --send      # renders and emails
"""
import os, sys, json, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

SQL = """
with d as (select now()-interval '24 hours' c)
select json_build_object(
 'vids_24h',(select count(distinct video_id) from youtube.media_files,d where created_at>c),
 'gb_24h',(select round(coalesce(sum(file_size_bytes),0)/1e9,1) from youtube.media_files,d where created_at>c),
 'subs_24h',(select count(*) from youtube.subtitles,d where created_at>c),
 'fail_dl_24h',(select count(*) from youtube.videos where media_status='failed' and updated_at>(select c from d)),
 'fail_sub_24h',(select count(*) from youtube.videos where subtitle_status='failed' and updated_at>(select c from d)),
 'blocked_24h',(select count(*) from youtube.videos where extractor_blocked and updated_at>(select c from d)),
 'vids_pct',(select round(100.0*count(*) filter(where media_status='completed')::numeric/nullif(count(*),0),1) from youtube.videos),
 'subs_pct',(select round(100.0*count(*) filter(where subtitle_status='completed')::numeric/nullif(count(*),0),1) from youtube.videos),
 'vids_done',(select count(*) filter(where media_status='completed') from youtube.videos),
 'vids_total',(select count(*) from youtube.videos),
 'subs_done',(select count(*) filter(where subtitle_status='completed') from youtube.videos),
 'waiting',(select count(*) from youtube.videos where media_status in ('queued','ready_for_download','pending','processing')),
 'storage_gb',(select round(sum(file_size_bytes)/1e9) from youtube.media_files),
 'quota',(select remaining from youtube.api_quota where service='rapidapi'),
 'top_sub_fail',(select json_agg(row_to_json(t)) from (
    select coalesce(nullif(substring(subtitle_last_error from 1 for 70),''),'(unknown)') reason, count(*) n
    from youtube.videos where subtitle_status='failed' group by 1 order by 2 desc limit 3) t),
 'daily',(select json_agg(row_to_json(t)) from (
    select to_char(date_trunc('day',created_at),'Mon DD') d, count(distinct video_id) n
    from youtube.media_files where created_at>now()-interval '14 days' group by date_trunc('day',created_at) order by 1) t)
) j;
"""

def fetch_db():
    dsn = os.environ.get("DATABASE_URL") or os.environ["SUPABASE_DB_URL"]
    try:
        import psycopg2 as pg
    except ImportError:
        import psycopg as pg
    with pg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(SQL)
        row = cur.fetchone()[0]
        return row if isinstance(row, dict) else json.loads(row)

def _n(x):
    try: return f"{int(round(float(x))):,}"
    except Exception: return str(x)

def render_html(j):
    days_to_clear = ""
    try:
        rate = sum(x["n"] for x in (j.get("daily") or [])[-7:]) / max(1, len((j.get("daily") or [])[-7:]))
        if rate > 0:
            days_to_clear = f"~{int(round(j['waiting']/rate))} days to clear at current pace"
    except Exception:
        pass
    daily = j.get("daily") or []
    mx = max([x["n"] for x in daily] or [1])
    bars = ""
    for i, x in enumerate(daily):
        h = max(4, int(round(56 * x["n"] / mx)))
        c = "#185FA5" if i == len(daily)-1 else "#B5D4F4"
        bars += f'<td style="vertical-align:bottom;padding:0 2px;"><div style="height:{h}px;background:{c};border-radius:2px 2px 0 0;"></div><div style="font-size:9px;color:#888;text-align:center;padding-top:3px;">{x["d"][-2:]}</div></td>'
    fail_sub = j.get("fail_sub_24h", 0) or 0
    sub_bg, sub_fg = ("#EAF3DE","#3B6D11") if fail_sub == 0 else ("#FAEEDA","#854F0B")
    dl_bg, dl_fg = ("#EAF3DE","#3B6D11") if (j.get("fail_dl_24h",0) or 0) == 0 else ("#FAEEDA","#854F0B")
    top = j.get("top_sub_fail") or []
    top_line = ("Top reason: “%s” (%s)" % (top[0]["reason"], _n(top[0]["n"]))) if top else ""
    healthy = (j.get("fail_dl_24h",0) or 0) < 50
    status_bg, status_fg = ("#EAF3DE","#3B6D11") if healthy else ("#FCEBEB","#A32D2D")
    status_txt = "Healthy — everything is running" if healthy else "Needs attention"
    dash = os.environ.get("DASHBOARD_URL", "https://grafana.app7.kelana5.com/d/videomasjid-simple")
    today = datetime.now().strftime("%A %d %B %Y")
    def card(label, val, bg="#F7F6F2", fg="#2C2C2A"):
        return f'<td style="padding:6px;"><div style="background:{bg};border-radius:8px;padding:12px;"><div style="font-size:12px;color:#5F5E5A;">{label}</div><div style="font-size:22px;font-weight:600;color:{fg};padding-top:2px;">{val}</div></div></td>'
    def bar(label, pct, color, sub):
        return f'''<div style="margin-bottom:14px;">
          <div style="font-size:13px;color:#2C2C2A;padding-bottom:5px;"><span>{label}</span>
            <span style="float:right;font-weight:600;">{pct}% &middot; {sub}</span></div>
          <div style="height:9px;background:#ECEAE2;border-radius:99px;">
            <div style="height:9px;width:{pct}%;background:{color};border-radius:99px;"></div></div></div>'''
    return f'''<!DOCTYPE html><html><body style="margin:0;background:#F1EFE8;font-family:Arial,Helvetica,sans-serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F1EFE8;padding:20px 0;"><tr><td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background:#fff;border:1px solid #E2E0D8;border-radius:10px;overflow:hidden;">
  <tr><td style="padding:18px 20px;background:{status_bg};">
    <div style="font-size:13px;color:{status_fg};">System status</div>
    <div style="font-size:20px;font-weight:600;color:{status_fg};">{status_txt}</div>
    <div style="font-size:12px;color:#5F5E5A;padding-top:4px;">{today} &middot; covers the last 24 hours</div>
  </td></tr>
  <tr><td style="padding:16px 14px 4px;"><div style="font-size:13px;color:#5F5E5A;padding:0 6px 6px;">Yesterday</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
      {card("Videos downloaded", _n(j["vids_24h"]))}
      {card("Subtitles downloaded", _n(j["subs_24h"]))}
      {card("Storage added", str(j["gb_24h"])+" GB")}
    </tr></table></td></tr>
  <tr><td style="padding:12px 20px 4px;"><div style="font-size:13px;color:#5F5E5A;padding-bottom:10px;">Overall progress</div>
    {bar("Videos downloaded", j["vids_pct"], "#3B6D11", _n(j["vids_done"])+" of "+_n(j["vids_total"]))}
    {bar("Subtitles downloaded", j["subs_pct"], "#BA7517", _n(j["subs_done"])+" of "+_n(j["vids_total"]))}
    <div style="font-size:13px;color:#5F5E5A;padding-top:4px;">{_n(j["waiting"])} waiting in line &nbsp;&middot;&nbsp; {days_to_clear}</div>
  </td></tr>
  <tr><td style="padding:14px 14px 4px;"><div style="font-size:13px;color:#5F5E5A;padding:0 6px 6px;">Anything wrong?</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
      {card("Failed downloads (24h)", _n(j.get("fail_dl_24h",0)), dl_bg, dl_fg)}
      {card("Failed subtitles (24h)", _n(fail_sub), sub_bg, sub_fg)}
      {card("Newly blocked (24h)", _n(j.get("blocked_24h",0)))}
    </tr></table>
    <div style="font-size:12px;color:#5F5E5A;padding:6px 8px 0;">{top_line}</div>
  </td></tr>
  <tr><td style="padding:14px 20px 6px;"><div style="font-size:13px;color:#5F5E5A;padding-bottom:8px;">Last 14 days &middot; videos downloaded per day</div>
    <table role="presentation" cellpadding="0" cellspacing="0" style="width:100%;height:64px;"><tr>{bars}</tr></table>
  </td></tr>
  <tr><td style="padding:14px 20px;border-top:1px solid #E2E0D8;">
    <span style="font-size:12px;color:#5F5E5A;">API quota: {_n(j["quota"])} remaining</span>
    <a href="{dash}" style="float:right;font-size:13px;color:#185FA5;font-weight:600;text-decoration:none;">Open dashboard &rarr;</a>
  </td></tr>
</table></td></tr></table></body></html>'''

def send(html, subject):
    host = os.environ.get("SMTP_HOST", "smtp.emailit.com")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER", "emailit")
    pw   = os.environ["EMAILIT_API_KEY"]
    frm  = os.environ["REPORT_FROM"]
    to   = os.environ.get("REPORT_TO", "kantorjualsfp@gmail.com")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject; msg["From"] = frm; msg["To"] = to
    msg.attach(MIMEText("Open in an HTML-capable mail client for the full report.", "plain"))
    msg.attach(MIMEText(html, "html"))
    ctx = ssl.create_default_context()
    with smtplib.SMTP(host, port, timeout=30) as s:
        s.starttls(context=ctx); s.login(user, pw)
        s.sendmail(frm.split("<")[-1].strip(">"), [to], msg.as_string())
    print("sent to", to)

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "--dry-run"
    if mode == "--loop":
        import time
        from datetime import datetime, timedelta
        print("daily-report loop started; sends at 09:00 local (TZ env).", flush=True)
        while True:
            now = datetime.now()
            target = now.replace(hour=9, minute=0, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            time.sleep(max(1, (target - now).total_seconds()))
            try:
                jj = fetch_db()
                ic = "OK" if (jj.get("fail_dl_24h", 0) or 0) < 50 else "ATTENTION"
                sub = f"VideoMasjid daily — {ic} · {_n(jj['vids_24h'])} videos added · {jj['vids_pct']}% complete"
                send(render_html(jj), sub)
                print("sent daily report", flush=True)
            except Exception as e:
                print("report run failed:", e, flush=True)
            time.sleep(60)
    if "--data" in sys.argv:
        j = json.load(open(sys.argv[sys.argv.index("--data")+1]))
    else:
        j = fetch_db()
    icon = "OK" if (j.get("fail_dl_24h",0) or 0) < 50 else "ATTENTION"
    subject = f"VideoMasjid daily — {icon} · {_n(j['vids_24h'])} videos added · {j['vids_pct']}% complete"
    html = render_html(j)
    if mode == "--send":
        send(html, subject)
    else:
        open("report.html", "w").write(html)
        print("wrote report.html  |  subject:", subject)

if __name__ == "__main__":
    main()
