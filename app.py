from flask import (Flask, render_template, request, redirect,
                   url_for, flash, session, jsonify, abort)
import json, os, re, uuid, smtplib, random, time, hashlib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import psycopg2, psycopg2.extras
from contextlib import contextmanager
import urllib.request, urllib.parse, base64

# ════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════
def load_env():
    path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip().strip('"\'')
load_env()

_base = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__, template_folder=_base, static_folder=os.path.join(_base, "static"))
app.secret_key = os.environ.get("SECRET_KEY", "zedcanvas2_secret_" + uuid.uuid4().hex)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=bool(os.environ.get("RAILWAY_ENVIRONMENT")),
    MAX_CONTENT_LENGTH=8 * 1024 * 1024,
    PERMANENT_SESSION_LIFETIME=timedelta(days=30),
)

# ════════════════════════════════════════
# EMAIL
# ════════════════════════════════════════
EMAIL_ADDRESS  = os.environ.get("EMAIL_ADDRESS",  "zedcanvas4all@gmail.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "jodehbhwzrozyfuc")

# ════════════════════════════════════════
# CLOUDINARY
# ════════════════════════════════════════
CLD_CLOUD  = os.environ.get("CLOUDINARY_CLOUD_NAME", "dbo7y3jo5")
CLD_KEY    = os.environ.get("CLOUDINARY_API_KEY",    "191442868351399")
CLD_SECRET = os.environ.get("CLOUDINARY_API_SECRET", "D76kBZ0SJ9KxJQsn1QVWJ1LNLOg")

def cloudinary_upload(file_obj, folder="zedcanvas2"):
    try:
        ts  = str(int(time.time()))
        sig = hashlib.sha1(f"folder={folder}&timestamp={ts}{CLD_SECRET}".encode()).hexdigest()
        file_obj.seek(0)
        data = file_obj.read()
        file_obj.seek(0)
        bnd  = uuid.uuid4().hex
        body = []
        for name, val in [("api_key", CLD_KEY), ("timestamp", ts),
                          ("folder", folder), ("signature", sig)]:
            body += [f"--{bnd}".encode(),
                     f'Content-Disposition: form-data; name="{name}"'.encode(),
                     b"", val.encode()]
        body += [f"--{bnd}".encode(),
                 b'Content-Disposition: form-data; name="file"; filename="upload"',
                 b"Content-Type: application/octet-stream", b"", data,
                 f"--{bnd}--".encode()]
        body_bytes = b"\r\n".join(body)
        req = urllib.request.Request(
            f"https://api.cloudinary.com/v1_1/{CLD_CLOUD}/image/upload",
            data=body_bytes)
        req.add_header("Content-Type", f"multipart/form-data; boundary={bnd}")
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode()).get("secure_url", "")
    except Exception as e:
        print(f"[CLOUDINARY] {e}")
        return ""

# ════════════════════════════════════════
# DATABASE
# ════════════════════════════════════════
DATABASE_URL = os.environ.get("DATABASE_URL", "")

@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS zc_users (
            id TEXT PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS zc_posts (
            id TEXT PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS zc_messages (
            id TEXT PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS zc_notifications (
            id TEXT PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS zc_stories (
            id TEXT PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS zc_challenges (
            id TEXT PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS zc_listings (
            id TEXT PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS zc_bookmarks (
            id TEXT PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS zc_views (
            id TEXT PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS zc_resets (
            id TEXT PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT NOW());
        """)
    print("[DB] Ready")

def db_all(table):
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"SELECT data FROM {table} ORDER BY created_at ASC")
            return [r["data"] for r in cur.fetchall()]
    except Exception as e:
        print(f"[DB] load {table}: {e}")
        return []

def db_set(table, items):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute(f"DELETE FROM {table}")
            for item in items:
                cur.execute(
                    f"INSERT INTO {table} (id,data) VALUES (%s,%s) "
                    f"ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data",
                    (item.get("id", str(uuid.uuid4())), json.dumps(item)))
    except Exception as e:
        print(f"[DB] save {table}: {e}")

if DATABASE_URL:
    try: init_db()
    except Exception as e: print(f"[DB INIT] {e}")

# ════════════════════════════════════════
# DATA HELPERS
# ════════════════════════════════════════
def load_users():     return db_all("zc_users")
def save_users(d):    db_set("zc_users", d)
def load_posts():     return db_all("zc_posts")
def save_posts(d):    db_set("zc_posts", d)
def load_msgs():      return db_all("zc_messages")
def save_msgs(d):     db_set("zc_messages", d)
def load_notifs():    return db_all("zc_notifications")
def save_notifs(d):   db_set("zc_notifications", d)
def load_stories():   return db_all("zc_stories")
def save_stories(d):  db_set("zc_stories", d)
def load_challenges():  return db_all("zc_challenges")
def save_challenges(d): db_set("zc_challenges", d)
def load_listings():  return db_all("zc_listings")
def save_listings(d): db_set("zc_listings", d)
def load_bookmarks(): return db_all("zc_bookmarks")
def save_bookmarks(d):db_set("zc_bookmarks", d)
def load_views():     return db_all("zc_views")
def save_views(d):    db_set("zc_views", d)
def load_resets():    return db_all("zc_resets")
def save_resets(d):   db_set("zc_resets", d)

ALLOWED = {"png","jpg","jpeg","gif","webp"}
def allowed(fn): return "." in fn and fn.rsplit(".",1)[1].lower() in ALLOWED

ART_STYLES = ["Painting","Drawing","Sculpture","Photography","Digital Art",
              "Illustration","Watercolour","Pencil / Charcoal","Mixed Media",
              "Printmaking","Textile / Fabric","Ceramics","Other"]
ART_CATS   = ART_STYLES

ZAMBIAN_CITIES = ["Lusaka","Ndola","Kitwe","Livingstone","Kabwe",
                  "Chipata","Solwezi","Mansa","Kasama","Other"]

# ════════════════════════════════════════
# UTILITIES
# ════════════════════════════════════════
def now_str():  return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def today_str():return datetime.now().strftime("%Y-%m-%d")

def time_ago(dt):
    try:
        diff = (datetime.now() - datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")).total_seconds()
        if diff < 60:    return "just now"
        if diff < 3600:  return f"{int(diff//60)}m ago"
        if diff < 86400: return f"{int(diff//3600)}h ago"
        return f"{int(diff//86400)}d ago"
    except: return ""

def sanitize(t, n=500):
    if not t: return ""
    return re.sub(r"[<>]", "", str(t)).strip()[:n]

def sanitize_un(u):
    return re.sub(r"[^a-z0-9_]", "", u.lower())[:20]

def get_ip():
    return request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()

# Rate limiter
_rl = {}
def rate_ok(key, limit, window):
    now = time.time()
    _rl[key] = [t for t in _rl.get(key,[]) if now-t < window]
    if len(_rl[key]) >= limit: return False
    _rl[key].append(now)
    return True

# User helpers
def get_user(uid):
    return next((u for u in load_users() if u["id"]==uid), None)

def get_by_username(un):
    return next((u for u in load_users() if u["username"].lower()==un.lower()), None)

def get_by_email(em):
    return next((u for u in load_users() if u["email"].lower()==em.lower()), None)

def current_user():
    uid = session.get("user_id")
    return get_user(uid) if uid else None

def add_notif(to_uid, from_uid, ntype, ref=""):
    if to_uid == from_uid: return
    notifs = load_notifs()
    notifs.append({"id":str(uuid.uuid4()),"to_uid":to_uid,"from_uid":from_uid,
                   "type":ntype,"ref":ref,"read":False,"created":now_str()})
    save_notifs(notifs)

# ════════════════════════════════════════
# AUTH DECORATOR
# ════════════════════════════════════════
def login_required(f):
    @wraps(f)
    def dec(*a,**kw):
        if not session.get("user_id"):
            flash("Please log in first.", "error")
            return redirect(url_for("login"))
        # Update last seen
        try:
            me = current_user()
            if me:
                users = load_users()
                u = next((x for x in users if x["id"]==me["id"]), None)
                if u:
                    u["last_seen"] = now_str()
                    save_users(users)
        except: pass
        return f(*a,**kw)
    return dec

def owner_required(f):
    @wraps(f)
    def dec(*a,**kw):
        me = current_user()
        users = load_users()
        if not me or not users or users[0]["id"] != me["id"]:
            abort(403)
        return f(*a,**kw)
    return dec

# ════════════════════════════════════════
# ONLINE STATUS
# ════════════════════════════════════════
def online_status(user, viewer=None):
    if not user: return None
    priv = user.get("status_privacy","everyone")
    if priv == "nobody": return None
    if priv == "followers" and viewer:
        if viewer["id"] not in user.get("followers",[]) and viewer["id"] != user["id"]:
            return None
    ls = user.get("last_seen")
    if not ls: return None
    try:
        diff = (datetime.now() - datetime.strptime(ls, "%Y-%m-%d %H:%M:%S")).total_seconds()
        if diff < 300:   return "online"
        if diff < 3600:  return f"{int(diff//60)}m ago"
        if diff < 86400: return f"{int(diff//3600)}h ago"
        return f"{int(diff//86400)}d ago"
    except: return None

# ════════════════════════════════════════
# ACHIEVEMENTS
# ════════════════════════════════════════
BADGES = {
    "first_post":     ("🎨","First Post"),
    "ten_posts":      ("🖼️","Prolific"),
    "fifty_posts":    ("✨","Dedicated"),
    "first_like":     ("🖤","Appreciated"),
    "hundred_likes":  ("💫","Fan Favourite"),
    "first_follower": ("👥","Connected"),
    "hundred_follows":("⭐","Rising Star"),
    "first_sale":     ("🛒","Art Seller"),
    "challenger":     ("🎭","Challenger"),
    "trophy":         ("🏆","Champion"),
}

def check_badges(uid):
    try:
        users  = load_users()
        user   = next((u for u in users if u["id"]==uid), None)
        if not user: return
        posts  = load_posts()
        badges = user.get("badges",[])
        changed= False
        my_p   = [p for p in posts if p["user_id"]==uid and not p.get("is_poll")]
        likes  = sum(len(p.get("likes",[])) for p in my_p)
        checks = [
            ("first_post",     len(my_p)>=1),
            ("ten_posts",      len(my_p)>=10),
            ("fifty_posts",    len(my_p)>=50),
            ("first_like",     likes>=1),
            ("hundred_likes",  likes>=100),
            ("first_follower", len(user.get("followers",[]))>=1),
            ("hundred_follows",len(user.get("followers",[]))>=100),
            ("first_sale",     any(l["user_id"]==uid for l in load_listings())),
        ]
        for bid, cond in checks:
            if cond and bid not in badges:
                badges.append(bid)
                add_notif(uid, uid, "badge", bid)
                changed = True
        if changed:
            user["badges"] = badges
            save_users(users)
    except Exception as e:
        print(f"[BADGE] {e}")

# ════════════════════════════════════════
# MENTIONS
# ════════════════════════════════════════
def parse_mentions(text, post_id, author_id):
    if not text: return
    um = {u["username"].lower(): u for u in load_users()}
    for word in text.split():
        if word.startswith("@"):
            un = re.sub(r"[^a-z0-9_]","",word[1:].lower())
            if un in um and um[un]["id"] != author_id:
                add_notif(um[un]["id"], author_id, "mention", post_id)

# ════════════════════════════════════════
# EMAIL
# ════════════════════════════════════════
def send_email(to, subject, html):
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_ADDRESS
        msg["To"]      = to
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as s:
            s.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            s.sendmail(EMAIL_ADDRESS, to, msg.as_string())
        return True
    except Exception as e:
        print(f"[EMAIL] {e}")
        return False

# ════════════════════════════════════════
# PWA
# ════════════════════════════════════════
@app.route("/static/sw.js")
def sw(): return app.send_static_file("sw.js")

@app.route("/manifest.json")
def manifest(): return app.send_static_file("manifest.json")

# ════════════════════════════════════════
# AUTH ROUTES
# ════════════════════════════════════════
@app.route("/")
def index():
    return redirect(url_for("feed") if session.get("user_id") else url_for("landing"))

@app.route("/landing")
def landing():
    if session.get("user_id"): return redirect(url_for("feed"))
    return render_template("landing.html")

@app.route("/signup", methods=["GET","POST"])
def signup():
    if session.get("user_id"): return redirect(url_for("feed"))
    if request.method == "POST":
        if not rate_ok(f"signup:{get_ip()}", 5, 3600):
            flash("Too many attempts. Try later.", "error")
            return render_template("signup.html", styles=ART_STYLES)
        full_name = sanitize(request.form.get("full_name",""), 60)
        username  = sanitize_un(request.form.get("username",""))
        email     = sanitize(request.form.get("email",""), 120).lower()
        password  = request.form.get("password","").strip()
        bio       = sanitize(request.form.get("bio",""), 300)
        art_style = sanitize(request.form.get("art_style",""), 50)
        city      = sanitize(request.form.get("city",""), 50)
        users     = load_users()
        errors    = []
        if not full_name:   errors.append("Full name required.")
        if not re.match(r"^[a-z0-9_]{3,20}$", username):
            errors.append("Username: 3-20 chars, letters/numbers/underscores only.")
        if not re.match(r"^[\w.\-+]+@[\w.\-]+\.\w{2,}$", email):
            errors.append("Valid email required.")
        if len(password) < 6: errors.append("Password min 6 characters.")
        if any(u["username"].lower()==username for u in users):
            errors.append("Username taken.")
        if any(u["email"].lower()==email for u in users):
            errors.append("Email already registered.")
        if errors:
            for e in errors: flash(e,"error")
            return render_template("signup.html", styles=ART_STYLES, form=request.form)
        # Avatar upload
        avatar = ""
        photo  = request.files.get("photo")
        if photo and photo.filename and allowed(photo.filename):
            avatar = cloudinary_upload(photo, "zedcanvas2/avatars")
        new_user = {
            "id":             str(uuid.uuid4()),
            "full_name":      full_name,
            "username":       username,
            "email":          email,
            "password":       generate_password_hash(password),
            "bio":            bio,
            "art_style":      art_style,
            "city":           city,
            "avatar":         avatar,
            "followers":      [],
            "following":      [],
            "verified":       False,
            "badges":         [],
            "status_privacy": "everyone",
            "last_seen":      now_str(),
            "joined":         now_str(),
        }
        users.append(new_user)
        save_users(users)
        session["user_id"] = new_user["id"]
        session.permanent  = True
        # Welcome email (silent fail)
        send_email(email, "Welcome to ZedCanvas! 🎨",
            f"<h2>Welcome @{username}!</h2><p>ZedCanvas — Zambia's art community is waiting for you.</p>")
        flash(f"Welcome to ZedCanvas, @{username}! 🎨🇿🇲", "success")
        return redirect(url_for("feed"))
    return render_template("signup.html", styles=ART_STYLES, form={})

@app.route("/login", methods=["GET","POST"])
def login():
    if session.get("user_id"): return redirect(url_for("feed"))
    if request.method == "POST":
        if not rate_ok(f"login:{get_ip()}", 10, 60):
            flash("Too many attempts. Wait a minute.", "error")
            return render_template("login.html")
        ident    = sanitize(request.form.get("login_id","")).lower()
        password = request.form.get("password","").strip()
        user     = get_by_email(ident) or get_by_username(ident)
        if user and check_password_hash(user["password"], password):
            if user.get("banned"):
                flash("Your account has been suspended.", "error")
                return render_template("login.html")
            session["user_id"] = user["id"]
            session.permanent  = True
            flash(f"Welcome back, @{user['username']}! 🎨", "success")
            return redirect(url_for("feed"))
        time.sleep(0.5)
        flash("Invalid credentials.", "error")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("landing"))

@app.route("/forgot-password", methods=["GET","POST"])
def forgot_password():
    if request.method == "POST":
        email = sanitize(request.form.get("email","")).lower()
        user  = get_by_email(email)
        if user:
            token   = uuid.uuid4().hex
            expires = (datetime.now()+timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
            resets  = [r for r in load_resets() if r.get("email")!=email]
            resets.append({"id":str(uuid.uuid4()),"email":email,
                           "token":token,"expires":expires,"used":False})
            save_resets(resets)
            link = url_for("reset_password", token=token, _external=True)
            send_email(email, "Reset your ZedCanvas password",
                f"<p>Hi @{user['username']},</p>"
                f"<p><a href='{link}'>Click here to reset your password</a></p>"
                f"<p>Link expires in 30 minutes.</p>")
        flash("If that email exists, a reset link was sent.", "success")
        return redirect(url_for("login"))
    return render_template("forgot_password.html")

@app.route("/reset-password/<token>", methods=["GET","POST"])
def reset_password(token):
    resets = load_resets()
    entry  = next((r for r in resets if r["token"]==token and not r.get("used")), None)
    if not entry or entry["expires"] < now_str():
        flash("Invalid or expired link.", "error")
        return redirect(url_for("forgot_password"))
    if request.method == "POST":
        pw = request.form.get("password","").strip()
        if len(pw) < 6:
            flash("Password min 6 characters.", "error")
            return render_template("reset_password.html", token=token)
        users = load_users()
        user  = get_by_email(entry["email"])
        if user:
            u = next((x for x in users if x["id"]==user["id"]), None)
            if u:
                u["password"] = generate_password_hash(pw)
                save_users(users)
        entry["used"] = True
        save_resets(resets)
        flash("Password updated! Please log in.", "success")
        return redirect(url_for("login"))
    return render_template("reset_password.html", token=token)

# ════════════════════════════════════════
# FEED
# ════════════════════════════════════════
@app.route("/feed")
@login_required
def feed():
    me     = current_user()
    posts  = load_posts()
    users  = load_users()
    um     = {u["id"]:u for u in users}
    # Stories bar
    cutoff = (datetime.now()-timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    active_stories = [s for s in load_stories() if s["created"]>=cutoff]
    seen   = set()
    stories_bar = []
    for s in sorted(active_stories, key=lambda x:x["created"], reverse=True):
        if s["user_id"] not in seen:
            seen.add(s["user_id"])
            s["author"] = um.get(s["user_id"],{})
            s["seen"]   = me["id"] in s.get("views",[])
            stories_bar.append(s)
    my_story = next((s for s in stories_bar if s["user_id"]==me["id"]), None)
    if my_story:
        stories_bar.remove(my_story)
        stories_bar.insert(0, my_story)
    # Posts
    all_posts = load_posts()
    pm = {p["id"]:p for p in all_posts}
    listed = {l["post_id"] for l in load_listings() if l["status"]=="available"}
    listing_map = {l["post_id"]:l["id"] for l in load_listings() if l["status"]=="available"}
    visible = [p for p in posts
               if (p["user_id"] in me.get("following",[]) or p["user_id"]==me["id"])]
    visible = sorted(visible, key=lambda p:p["created"], reverse=True)
    for p in visible:
        p["time_ago"]     = time_ago(p["created"])
        p["author"]       = um.get(p["user_id"],{})
        p["liked"]        = me["id"] in p.get("likes",[])
        p["bookmarked"]   = any(b for b in load_bookmarks()
                                if b["user_id"]==me["id"] and b["post_id"]==p["id"])
        p["repost_count"] = sum(1 for r in all_posts if r.get("repost_of")==p["id"])
        p["reposted"]     = any(r for r in all_posts
                                if r.get("repost_of")==p["id"] and r["user_id"]==me["id"])
        p["for_sale"]     = p["id"] in listed
        p["listing_id"]   = listing_map.get(p["id"],"")
        if p.get("repost_of"):
            orig = pm.get(p["repost_of"])
            p["orig_post"]   = orig
            p["orig_author"] = um.get(orig["user_id"],{}) if orig else {}
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("feed.html", posts=visible, me=me, stories=stories_bar,
                           notif_count=len(notifs), now=now_str())

# ════════════════════════════════════════
# DISCOVER
# ════════════════════════════════════════
@app.route("/discover")
@login_required
def discover():
    me    = current_user()
    users = load_users()
    posts = load_posts()
    um    = {u["id"]:u for u in users}
    cat   = request.args.get("cat","")
    # Suggested users (not following)
    suggested = [u for u in users
                 if u["id"] not in me.get("following",[]) and u["id"]!=me["id"]]
    suggested = sorted(suggested, key=lambda u:len(u.get("followers",[])), reverse=True)[:10]
    # Explore posts
    explore = [p for p in posts if p.get("image") and not p.get("is_poll")]
    if cat: explore = [p for p in explore if p.get("category","").lower()==cat.lower()]
    explore = sorted(explore, key=lambda p:len(p.get("likes",[])), reverse=True)[:30]
    for p in explore:
        p["author"] = um.get(p["user_id"],{})
        p["liked"]  = me["id"] in p.get("likes",[])
        p["time_ago"] = time_ago(p["created"])
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("discover.html", suggested=suggested, posts=explore,
                           me=me, cats=ART_CATS, active_cat=cat, notif_count=len(notifs))

# ════════════════════════════════════════
# POSTS
# ════════════════════════════════════════
@app.route("/post/create", methods=["GET","POST"])
@login_required
def create_post():
    me = current_user()
    if request.method == "POST":
        caption   = sanitize(request.form.get("caption",""), 2200)
        category  = sanitize(request.form.get("category",""), 50)
        tags_raw  = sanitize(request.form.get("tags",""), 200)
        tags      = [t.strip().lower().lstrip("#") for t in tags_raw.split(",") if t.strip()][:10]
        image     = request.files.get("image")
        img_url   = ""
        if image and image.filename and allowed(image.filename):
            img_url = cloudinary_upload(image, "zedcanvas2/posts")
        posts = load_posts()
        pid   = str(uuid.uuid4())
        posts.append({
            "id":       pid,
            "user_id":  me["id"],
            "caption":  caption,
            "image":    img_url,
            "category": category,
            "tags":     tags,
            "likes":    [],
            "comments": [],
            "pinned":   False,
            "created":  now_str(),
        })
        save_posts(posts)
        parse_mentions(caption, pid, me["id"])
        check_badges(me["id"])
        flash("Post shared! 🎨", "success")
        return redirect(url_for("feed"))
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("create_post.html", me=me, cats=ART_CATS, notif_count=len(notifs))

@app.route("/post/<pid>/like", methods=["POST"])
@login_required
def like_post(pid):
    me = current_user()
    if not rate_ok(f"like:{me['id']}", 60, 60):
        return jsonify({"error":"slow down"}), 429
    posts = load_posts()
    post  = next((p for p in posts if p["id"]==pid), None)
    if not post: return jsonify({"error":"not found"}), 404
    liked = me["id"] in post.get("likes",[])
    if liked: post["likes"].remove(me["id"])
    else:
        post["likes"].append(me["id"])
        add_notif(post["user_id"], me["id"], "like", pid)
    save_posts(posts)
    check_badges(post["user_id"])
    return jsonify({"liked":not liked,"count":len(post["likes"])})

@app.route("/post/<pid>/comment", methods=["POST"])
@login_required
def comment_post(pid):
    me      = current_user()
    text    = sanitize(request.form.get("comment",""), 500)
    reply_to= request.form.get("reply_to","").strip()
    if not text: return redirect(request.referrer or url_for("feed"))
    if not rate_ok(f"comment:{me['id']}", 20, 60):
        flash("Slow down on comments!", "error")
        return redirect(request.referrer or url_for("feed"))
    posts = load_posts()
    post  = next((p for p in posts if p["id"]==pid), None)
    if not post: return redirect(url_for("feed"))
    comment = {"id":str(uuid.uuid4()),"user_id":me["id"],"username":me["username"],
               "avatar":me.get("avatar",""),"text":text,"reply_to":reply_to,
               "replies":[],"created":now_str()}
    if reply_to:
        parent = next((c for c in post["comments"] if c["id"]==reply_to), None)
        if parent:
            parent.setdefault("replies",[]).append(comment)
            add_notif(parent["user_id"], me["id"], "reply", pid)
        else:
            post["comments"].append(comment)
    else:
        post["comments"].append(comment)
        if post["user_id"] != me["id"]:
            add_notif(post["user_id"], me["id"], "comment", pid)
    save_posts(posts)
    return redirect(request.referrer or url_for("feed"))

@app.route("/post/<pid>/comment/<cid>/delete", methods=["POST"])
@login_required
def delete_comment(pid, cid):
    me = current_user()
    posts = load_posts()
    post  = next((p for p in posts if p["id"]==pid), None)
    if post:
        post["comments"] = [c for c in post["comments"]
                            if not (c["id"]==cid and c["user_id"]==me["id"])]
        for c in post["comments"]:
            c["replies"] = [r for r in c.get("replies",[])
                            if not (r["id"]==cid and r["user_id"]==me["id"])]
        save_posts(posts)
    return redirect(request.referrer or url_for("feed"))

@app.route("/post/<pid>/delete", methods=["POST"])
@login_required
def delete_post(pid):
    me = current_user()
    posts = load_posts()
    posts = [p for p in posts if not (p["id"]==pid and p["user_id"]==me["id"])]
    save_posts(posts)
    flash("Post deleted.", "success")
    return redirect(request.referrer or url_for("profile", username=me["username"]))

@app.route("/post/<pid>/pin", methods=["POST"])
@login_required
def pin_post(pid):
    me = current_user()
    posts = load_posts()
    for p in posts:
        if p["user_id"]==me["id"]: p["pinned"]=False
    post = next((p for p in posts if p["id"]==pid and p["user_id"]==me["id"]), None)
    if post: post["pinned"]=True
    save_posts(posts)
    return redirect(request.referrer or url_for("profile", username=me["username"]))

@app.route("/post/<pid>/repost", methods=["POST"])
@login_required
def repost(pid):
    me = current_user()
    if not rate_ok(f"repost:{me['id']}", 20, 60):
        return jsonify({"error":"slow down"}), 429
    posts = load_posts()
    orig  = next((p for p in posts if p["id"]==pid), None)
    if not orig: return jsonify({"error":"not found"}), 404
    if orig["user_id"]==me["id"]: return jsonify({"error":"own post"}), 400
    existing = next((p for p in posts if p.get("repost_of")==pid and p["user_id"]==me["id"]), None)
    if existing:
        posts = [p for p in posts if not (p.get("repost_of")==pid and p["user_id"]==me["id"])]
        save_posts(posts)
        return jsonify({"reposted":False})
    posts.append({"id":str(uuid.uuid4()),"user_id":me["id"],"repost_of":pid,
                  "caption":"","image":orig.get("image",""),"category":orig.get("category",""),
                  "tags":orig.get("tags",[]),"likes":[],"comments":[],"pinned":False,"created":now_str()})
    save_posts(posts)
    add_notif(orig["user_id"], me["id"], "repost", pid)
    return jsonify({"reposted":True})

@app.route("/post/<pid>/bookmark", methods=["POST"])
@login_required
def bookmark(pid):
    me = current_user()
    bms = load_bookmarks()
    ex  = next((b for b in bms if b["user_id"]==me["id"] and b["post_id"]==pid), None)
    if ex:
        bms = [b for b in bms if not (b["user_id"]==me["id"] and b["post_id"]==pid)]
        save_bookmarks(bms)
        return jsonify({"bookmarked":False})
    bms.append({"id":str(uuid.uuid4()),"user_id":me["id"],"post_id":pid,"created":now_str()})
    save_bookmarks(bms)
    return jsonify({"bookmarked":True})

# ════════════════════════════════════════
# PROFILES
# ════════════════════════════════════════
@app.route("/u/<username>")
@login_required
def profile(username):
    me   = current_user()
    user = get_by_username(username)
    if not user: return "User not found", 404
    posts = load_posts()
    users = load_users()
    um    = {u["id"]:u for u in users}
    my_posts = [p for p in posts if p["user_id"]==user["id"]]
    pinned   = next((p for p in my_posts if p.get("pinned")), None)
    rest     = sorted([p for p in my_posts if not p.get("pinned")],
                      key=lambda p:p["created"], reverse=True)
    if pinned: rest.insert(0, pinned)
    for p in rest:
        p["time_ago"] = time_ago(p["created"])
        p["liked"]    = me["id"] in p.get("likes",[])
        if p.get("repost_of"):
            p["orig_author"] = um.get((next((x for x in posts if x["id"]==p["repost_of"]),{}) or {}).get("user_id",""),{})
    is_following = user["id"] in me.get("following",[])
    is_owner     = bool(users and users[0]["id"]==me["id"])
    status       = online_status(user, me)
    notifs       = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    # Track view
    if user["id"] != me["id"]:
        views = load_views()
        views.append({"id":str(uuid.uuid4()),"profile_uid":user["id"],
                      "viewer_uid":me["id"],"created":now_str()})
        save_views(views)
    return render_template("profile.html", user=user, posts=rest, me=me,
                           is_following=is_following, is_owner=is_owner,
                           status=status, badges=BADGES, notif_count=len(notifs))

@app.route("/settings", methods=["GET","POST"])
@login_required
def settings():
    me    = current_user()
    users = load_users()
    user  = next((u for u in users if u["id"]==me["id"]), None)
    if not user: return redirect(url_for("logout"))
    if request.method == "POST":
        try:
            user["full_name"]     = sanitize(request.form.get("full_name",""), 60)
            user["bio"]           = sanitize(request.form.get("bio",""), 300)
            user["art_style"]     = sanitize(request.form.get("art_style",""), 50)
            user["city"]          = sanitize(request.form.get("city",""), 50)
            user["status_privacy"]= request.form.get("status_privacy","everyone")
            photo = request.files.get("photo")
            if photo and photo.filename and allowed(photo.filename):
                url = cloudinary_upload(photo, "zedcanvas2/avatars")
                if url: user["avatar"] = url
            save_users(users)
            flash("Profile updated! ✅", "success")
        except Exception as e:
            print(f"[SETTINGS] {e}")
            flash("Something went wrong.", "error")
        return redirect(url_for("profile", username=user["username"]))
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("settings.html", me=me, styles=ART_STYLES,
                           cities=ZAMBIAN_CITIES, notif_count=len(notifs))

@app.route("/follow/<uid>", methods=["POST"])
@login_required
def follow(uid):
    me = current_user()
    if not rate_ok(f"follow:{me['id']}", 30, 60):
        return jsonify({"error":"slow down"}), 429
    users  = load_users()
    me_obj = next((u for u in users if u["id"]==me["id"]), None)
    target = next((u for u in users if u["id"]==uid), None)
    if not me_obj or not target or uid==me["id"]:
        return jsonify({"error":"invalid"}), 400
    if uid in me_obj.get("following",[]):
        me_obj["following"].remove(uid)
        if me["id"] in target.get("followers",[]): target["followers"].remove(me["id"])
        following = False
    else:
        me_obj.setdefault("following",[]).append(uid)
        target.setdefault("followers",[]).append(me["id"])
        add_notif(uid, me["id"], "follow", "")
        following = True
    save_users(users)
    check_badges(uid)
    return jsonify({"following":following,"count":len(target.get("followers",[]))})

# ════════════════════════════════════════
# MESSAGES
# ════════════════════════════════════════
@app.route("/messages")
@login_required
def messages():
    me   = current_user()
    msgs = load_msgs()
    users= load_users()
    um   = {u["id"]:u for u in users}
    # Build conversation list
    partners = set()
    for m in msgs:
        if m["from_id"]==me["id"]: partners.add(m["to_id"])
        if m["to_id"]==me["id"]:   partners.add(m["from_id"])
    convos = []
    for pid in partners:
        thread = sorted([m for m in msgs if
                         {m["from_id"],m["to_id"]}=={me["id"],pid}],
                        key=lambda x:x["created"])
        if not thread: continue
        last   = thread[-1]
        unread = sum(1 for m in thread if m["to_id"]==me["id"] and not m.get("read"))
        other  = um.get(pid,{})
        convos.append({"other":other,"last":last,"unread":unread,
                       "time_ago":time_ago(last["created"]),
                       "status":online_status(other, me)})
    convos = sorted(convos, key=lambda c:c["last"]["created"], reverse=True)
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("messages.html", convos=convos, me=me, notif_count=len(notifs))

@app.route("/messages/<uid>", methods=["GET","POST"])
@login_required
def conversation(uid):
    me   = current_user()
    them = get_user(uid)
    if not them: return "User not found", 404
    msgs = load_msgs()
    if request.method == "POST":
        text = sanitize(request.form.get("message",""), 1000)
        if text:
            msgs.append({"id":str(uuid.uuid4()),"from_id":me["id"],"to_id":uid,
                         "text":text,"read":False,"created":now_str()})
            save_msgs(msgs)
            add_notif(uid, me["id"], "message", "")
        return redirect(url_for("conversation", uid=uid))
    # Mark as read
    changed = False
    for m in msgs:
        if m["to_id"]==me["id"] and m["from_id"]==uid and not m.get("read"):
            m["read"]=True; changed=True
    if changed: save_msgs(msgs)
    thread = sorted([m for m in msgs if {m["from_id"],m["to_id"]}=={me["id"],uid}],
                    key=lambda x:x["created"])
    for m in thread: m["time_ago"]=time_ago(m["created"])
    status = online_status(them, me)
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("conversation.html", them=them, thread=thread,
                           me=me, status=status, notif_count=len(notifs))

@app.route("/messages/<uid>/delete/<mid>", methods=["POST"])
@login_required
def delete_message(uid, mid):
    me   = current_user()
    msgs = load_msgs()
    msgs = [m for m in msgs if not (m["id"]==mid and m["from_id"]==me["id"])]
    save_msgs(msgs)
    return redirect(url_for("conversation", uid=uid))

# ════════════════════════════════════════
# NOTIFICATIONS
# ════════════════════════════════════════
@app.route("/notifications")
@login_required
def notifications():
    me     = current_user()
    users  = load_users()
    um     = {u["id"]:u for u in users}
    notifs = load_notifs()
    mine   = sorted([n for n in notifs if n["to_uid"]==me["id"]],
                    key=lambda n:n["created"], reverse=True)
    for n in mine:
        n["from_user"] = um.get(n["from_uid"],{})
        n["time_ago"]  = time_ago(n["created"])
    # Mark all read
    for n in notifs:
        if n["to_uid"]==me["id"]: n["read"]=True
    save_notifs(notifs)
    return render_template("notifications.html", notifs=mine, me=me, notif_count=0)

# ════════════════════════════════════════
# SEARCH
# ════════════════════════════════════════
@app.route("/search")
@login_required
def search():
    me    = current_user()
    q     = request.args.get("q","").strip().lower()
    style = request.args.get("style","").strip()
    users = load_users()
    posts = load_posts()
    um    = {u["id"]:u for u in users}
    r_users, r_posts = [], []
    if q or style:
        r_users = [u for u in users if
                   (not q or q in u["username"].lower() or q in u["full_name"].lower()) and
                   (not style or style.lower() in (u.get("art_style","") or "").lower())]
        r_posts = [p for p in posts if not p.get("is_poll") and
                   (not q or q in (p.get("caption","") or "").lower() or
                    any(q in t for t in p.get("tags",[])))]
        r_posts = sorted(r_posts, key=lambda p:p["created"], reverse=True)
        for p in r_posts:
            p["author"]   = um.get(p["user_id"],{})
            p["liked"]    = me["id"] in p.get("likes",[])
            p["time_ago"] = time_ago(p["created"])
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("search.html", q=q, style=style, users=r_users,
                           posts=r_posts, me=me, styles=ART_STYLES, notif_count=len(notifs))

# ════════════════════════════════════════
# BOOKMARKS
# ════════════════════════════════════════
@app.route("/bookmarks")
@login_required
def bookmarks():
    me    = current_user()
    bms   = load_bookmarks()
    posts = load_posts()
    users = load_users()
    um    = {u["id"]:u for u in users}
    pm    = {p["id"]:p for p in posts}
    saved = []
    for b in [x for x in bms if x["user_id"]==me["id"]]:
        post = pm.get(b["post_id"])
        if post:
            post["author"]   = um.get(post["user_id"],{})
            post["liked"]    = me["id"] in post.get("likes",[])
            post["time_ago"] = time_ago(post["created"])
            saved.append(post)
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("bookmarks.html", posts=saved, me=me, notif_count=len(notifs))

# ════════════════════════════════════════
# STORIES
# ════════════════════════════════════════
@app.route("/stories/create", methods=["GET","POST"])
@login_required
def create_story():
    me = current_user()
    if request.method == "POST":
        if not rate_ok(f"story:{me['id']}", 10, 3600):
            flash("Max 10 stories per hour.", "error")
            return redirect(url_for("feed"))
        image   = request.files.get("image")
        caption = sanitize(request.form.get("caption",""), 200)
        if not image or not image.filename:
            flash("Please upload an image.", "error")
            return redirect(url_for("feed"))
        img_url = cloudinary_upload(image, "zedcanvas2/stories")
        if not img_url:
            flash("Image upload failed.", "error")
            return redirect(url_for("feed"))
        stories = load_stories()
        stories.append({"id":str(uuid.uuid4()),"user_id":me["id"],
                        "username":me["username"],"avatar":me.get("avatar",""),
                        "image":img_url,"caption":caption,"views":[],"created":now_str()})
        save_stories(stories)
        flash("Story posted! Disappears in 24h 📷", "success")
        return redirect(url_for("feed"))
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("create_story.html", me=me, notif_count=len(notifs))

@app.route("/stories/<sid>")
@login_required
def view_story(sid):
    me      = current_user()
    cutoff  = (datetime.now()-timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    stories = [s for s in load_stories() if s["created"]>=cutoff]
    story   = next((s for s in stories if s["id"]==sid), None)
    if not story:
        flash("Story expired or not found.", "error")
        return redirect(url_for("feed"))
    if me["id"] not in story["views"]:
        story["views"].append(me["id"])
        all_s = load_stories()
        for s in all_s:
            if s["id"]==sid: s["views"]=story["views"]
        save_stories(all_s)
    users   = load_users()
    um      = {u["id"]:u for u in users}
    story["author"] = um.get(story["user_id"],{})
    ids     = [s["id"] for s in stories]
    idx     = ids.index(sid) if sid in ids else 0
    prev_id = ids[idx-1] if idx>0 else None
    next_id = ids[idx+1] if idx<len(ids)-1 else None
    viewers = [um.get(v,{}) for v in story["views"] if v!=me["id"]] if story["user_id"]==me["id"] else []
    notifs  = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("view_story.html", story=story, me=me,
                           prev_id=prev_id, next_id=next_id,
                           viewers=viewers, notif_count=len(notifs))

@app.route("/stories/<sid>/delete", methods=["POST"])
@login_required
def delete_story(sid):
    me = current_user()
    stories = [s for s in load_stories() if not (s["id"]==sid and s["user_id"]==me["id"])]
    save_stories(stories)
    flash("Story deleted.", "success")
    return redirect(url_for("feed"))

# ════════════════════════════════════════
# ANALYTICS
# ════════════════════════════════════════
@app.route("/analytics")
@login_required
def analytics():
    me    = current_user()
    posts = load_posts()
    views = load_views()
    users = load_users()
    um    = {u["id"]:u for u in users}
    my_posts = sorted([p for p in posts if p["user_id"]==me["id"] and not p.get("is_poll")],
                      key=lambda p:len(p.get("likes",[])), reverse=True)
    for p in my_posts: p["time_ago"]=time_ago(p["created"])
    my_views = [v for v in views if v["profile_uid"]==me["id"]]
    cutoff   = (datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    recent_v = [v for v in my_views if v["created"]>=cutoff]
    # 7-day chart
    chart = {}
    for i in range(6,-1,-1):
        d = (datetime.now()-timedelta(days=i)).strftime("%Y-%m-%d")
        chart[d] = sum(1 for v in my_views if v["created"][:10]==d)
    recent_viewers = []
    seen = set()
    for v in sorted(recent_v, key=lambda x:x["created"], reverse=True):
        if v["viewer_uid"] not in seen:
            seen.add(v["viewer_uid"])
            u = um.get(v["viewer_uid"],{})
            if u: recent_viewers.append(u)
        if len(recent_viewers)>=5: break
    total_likes = sum(len(p.get("likes",[])) for p in my_posts)
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("analytics.html", me=me, posts=my_posts[:5],
                           total_views=len(recent_v), chart=chart,
                           recent_viewers=recent_viewers, total_likes=total_likes,
                           notif_count=len(notifs))

# ════════════════════════════════════════
# CHALLENGES
# ════════════════════════════════════════
def challenge_status(c):
    n = today_str()
    if n < c.get("start_date",""): return "upcoming"
    if n <= c.get("end_date",""):  return "active"
    return "ended"

def time_left(end):
    try:
        diff = (datetime.strptime(end,"%Y-%m-%d")-datetime.now()).days
        if diff < 0:  return "Ended"
        if diff == 0: return "Last day!"
        return f"{diff} day{'s' if diff!=1 else ''} left"
    except: return ""

@app.route("/challenges")
@login_required
def challenges():
    me  = current_user()
    all_c = load_challenges()
    for c in all_c:
        c["status"]    = challenge_status(c)
        c["time_left"] = time_left(c.get("end_date",""))
    active   = [c for c in all_c if c["status"]=="active"]
    upcoming = [c for c in all_c if c["status"]=="upcoming"]
    ended    = [c for c in all_c if c["status"]=="ended"]
    users  = load_users()
    is_owner = bool(users and users[0]["id"]==me["id"])
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("challenges.html", active=active, upcoming=upcoming,
                           ended=ended, me=me, is_owner=is_owner, notif_count=len(notifs))

@app.route("/challenges/create", methods=["GET","POST"])
@login_required
def create_challenge():
    me    = current_user()
    users = load_users()
    if not users or users[0]["id"]!=me["id"]:
        flash("Owner only.", "error")
        return redirect(url_for("challenges"))
    if request.method == "POST":
        title  = sanitize(request.form.get("title",""), 80)
        desc   = sanitize(request.form.get("description",""), 500)
        cat    = sanitize(request.form.get("category",""), 50)
        prize  = sanitize(request.form.get("prize",""), 200)
        start  = request.form.get("start_date","")
        end    = request.form.get("end_date","")
        if not title or not start or not end:
            flash("Title, start and end date required.", "error")
            return redirect(request.url)
        cs = load_challenges()
        cs.append({"id":str(uuid.uuid4()),"title":title,"description":desc,
                   "category":cat,"prize":prize,"start_date":start,"end_date":end,
                   "created_by":me["id"],"created":now_str()})
        save_challenges(cs)
        flash(f"Challenge '{title}' created! 🎭", "success")
        return redirect(url_for("challenges"))
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("create_challenge.html", me=me, cats=ART_CATS, notif_count=len(notifs))

@app.route("/challenges/<cid>")
@login_required
def challenge_detail(cid):
    me = current_user()
    cs = load_challenges()
    c  = next((x for x in cs if x["id"]==cid), None)
    if not c: return "Not found", 404
    c["status"]    = challenge_status(c)
    c["time_left"] = time_left(c.get("end_date",""))
    posts  = load_posts()
    users  = load_users()
    um     = {u["id"]:u for u in users}
    entries= sorted([p for p in posts if p.get("challenge_id")==cid],
                    key=lambda p:len(p.get("likes",[])), reverse=True)
    for i,p in enumerate(entries):
        p["rank"]      = i+1
        p["time_ago"]  = time_ago(p["created"])
        p["author"]    = um.get(p["user_id"],{})
        p["liked"]     = me["id"] in p.get("likes",[])
    my_entry = next((p for p in entries if p["user_id"]==me["id"]), None)
    is_owner = bool(users and users[0]["id"]==me["id"])
    notifs   = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("challenge_detail.html", challenge=c, entries=entries,
                           my_entry=my_entry, me=me, is_owner=is_owner, notif_count=len(notifs))

@app.route("/challenges/<cid>/enter", methods=["GET","POST"])
@login_required
def enter_challenge(cid):
    me = current_user()
    cs = load_challenges()
    c  = next((x for x in cs if x["id"]==cid), None)
    if not c or challenge_status(c)!="active":
        flash("Challenge not active.", "error")
        return redirect(url_for("challenges"))
    posts = load_posts()
    if any(p for p in posts if p.get("challenge_id")==cid and p["user_id"]==me["id"]):
        flash("Already entered!", "error")
        return redirect(url_for("challenge_detail", cid=cid))
    if request.method == "POST":
        caption = sanitize(request.form.get("caption",""), 2200)
        image   = request.files.get("image")
        if not image or not image.filename:
            flash("Please upload your artwork.", "error")
            return redirect(request.url)
        img_url = cloudinary_upload(image, "zedcanvas2/challenges")
        if not img_url:
            flash("Upload failed.", "error")
            return redirect(request.url)
        posts.append({"id":str(uuid.uuid4()),"user_id":me["id"],"caption":caption,
                      "image":img_url,"category":c.get("category",""),"tags":[],
                      "likes":[],"comments":[],"pinned":False,"challenge_id":cid,"created":now_str()})
        save_posts(posts)
        # Challenger badge
        users = load_users()
        u = next((x for x in users if x["id"]==me["id"]), None)
        if u and "challenger" not in u.get("badges",[]):
            u.setdefault("badges",[]).append("challenger")
            save_users(users)
        flash("Entry submitted! Good luck 🎨", "success")
        return redirect(url_for("challenge_detail", cid=cid))
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("enter_challenge.html", challenge=c, me=me, notif_count=len(notifs))

@app.route("/challenges/<cid>/end", methods=["POST"])
@login_required
def end_challenge(cid):
    me    = current_user()
    users = load_users()
    if not users or users[0]["id"]!=me["id"]: abort(403)
    cs    = load_challenges()
    c     = next((x for x in cs if x["id"]==cid), None)
    if c:
        c["end_date"] = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
        posts   = load_posts()
        entries = [p for p in posts if p.get("challenge_id")==cid]
        if entries:
            winner = max(entries, key=lambda p:len(p.get("likes",[])))
            c["winner_uid"] = winner["user_id"]
            wu = next((u for u in users if u["id"]==winner["user_id"]), None)
            if wu and "trophy" not in wu.get("badges",[]):
                wu.setdefault("badges",[]).append("trophy")
                save_users(users)
            add_notif(winner["user_id"], winner["user_id"], "won_challenge", cid)
        save_challenges(cs)
        flash("Challenge ended! Winner crowned 🏆", "success")
    return redirect(url_for("challenge_detail", cid=cid))

# ════════════════════════════════════════
# MARKETPLACE
# ════════════════════════════════════════
@app.route("/marketplace")
@login_required
def marketplace():
    me    = current_user()
    ls    = load_listings()
    users = load_users()
    posts = load_posts()
    um    = {u["id"]:u for u in users}
    pm    = {p["id"]:p for p in posts}
    cat   = request.args.get("cat","")
    sort  = request.args.get("sort","newest")
    active= [l for l in ls if l["status"]=="available"]
    if cat: active = [l for l in active if l.get("category","").lower()==cat.lower()]
    if sort=="price_low":  active=sorted(active,key=lambda l:float(l.get("price",0)))
    elif sort=="price_high":active=sorted(active,key=lambda l:float(l.get("price",0)),reverse=True)
    else: active=sorted(active,key=lambda l:l["created"],reverse=True)
    for l in active:
        l["seller"] = um.get(l["user_id"],{})
        l["post"]   = pm.get(l.get("post_id",""),{})
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("marketplace.html", listings=active, me=me,
                           cats=ART_CATS, active_cat=cat, sort=sort, notif_count=len(notifs))

@app.route("/marketplace/sell", methods=["GET","POST"])
@login_required
def sell_artwork():
    me   = current_user()
    posts= load_posts()
    ls   = load_listings()
    listed_ids = {l["post_id"] for l in ls if l["user_id"]==me["id"] and l["status"]=="available"}
    my_posts   = [p for p in posts if p["user_id"]==me["id"] and p.get("image") and p["id"] not in listed_ids]
    if request.method == "POST":
        post_id = request.form.get("post_id","").strip()
        price   = request.form.get("price","").strip()
        currency= request.form.get("currency","ZMW")
        desc    = sanitize(request.form.get("description",""), 500)
        cat     = sanitize(request.form.get("category",""), 50)
        title   = sanitize(request.form.get("title",""), 80)
        if not post_id or not price:
            flash("Please select a post and set a price.", "error")
            return redirect(request.url)
        try: price=float(price); assert price>0
        except: flash("Valid price required.", "error"); return redirect(request.url)
        post = next((p for p in posts if p["id"]==post_id and p["user_id"]==me["id"]), None)
        if not post: flash("Invalid post.", "error"); return redirect(request.url)
        ls.append({"id":str(uuid.uuid4()),"user_id":me["id"],"post_id":post_id,
                   "title":title or post.get("caption","Artwork")[:50],
                   "description":desc,"price":price,"currency":currency,
                   "category":cat,"status":"available","created":now_str()})
        save_listings(ls)
        check_badges(me["id"])
        flash("Artwork listed! 🛒", "success")
        return redirect(url_for("marketplace"))
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("sell_artwork.html", me=me, my_posts=my_posts,
                           cats=ART_CATS, notif_count=len(notifs))

@app.route("/marketplace/<lid>")
@login_required
def listing_detail(lid):
    me   = current_user()
    ls   = load_listings()
    l    = next((x for x in ls if x["id"]==lid), None)
    if not l: return "Not found", 404
    users= load_users()
    posts= load_posts()
    um   = {u["id"]:u for u in users}
    pm   = {p["id"]:p for p in posts}
    l["seller"] = um.get(l["user_id"],{})
    l["post"]   = pm.get(l.get("post_id",""),{})
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("listing_detail.html", listing=l, me=me, notif_count=len(notifs))

@app.route("/marketplace/<lid>/sold", methods=["POST"])
@login_required
def mark_sold(lid):
    me = current_user()
    ls = load_listings()
    l  = next((x for x in ls if x["id"]==lid and x["user_id"]==me["id"]), None)
    if l: l["status"]="sold"; save_listings(ls)
    flash("Marked as sold! 🎉", "success")
    return redirect(url_for("marketplace"))

@app.route("/marketplace/<lid>/delete", methods=["POST"])
@login_required
def delete_listing(lid):
    me = current_user()
    ls = [x for x in load_listings() if not (x["id"]==lid and x["user_id"]==me["id"])]
    save_listings(ls)
    flash("Listing removed.", "success")
    return redirect(url_for("marketplace"))

# ════════════════════════════════════════
# POLLS
# ════════════════════════════════════════
@app.route("/polls/create", methods=["GET","POST"])
@login_required
def create_poll():
    me = current_user()
    if request.method == "POST":
        q    = sanitize(request.form.get("question",""), 200)
        opts = [sanitize(request.form.get(f"option{i}",""),100) for i in range(1,5)]
        opts = [o for o in opts if o]
        if not q or len(opts)<2:
            flash("Question and at least 2 options required.", "error")
            return redirect(request.url)
        dur  = int(request.form.get("duration",24))
        exp  = (datetime.now()+timedelta(hours=dur)).strftime("%Y-%m-%d %H:%M:%S")
        pid  = str(uuid.uuid4())
        posts= load_posts()
        posts.append({"id":pid,"user_id":me["id"],"caption":q,"image":"",
                      "category":"","tags":[],"likes":[],"comments":[],"pinned":False,
                      "is_poll":True,
                      "poll":{"question":q,"options":[{"text":o,"votes":[]} for o in opts],
                              "expires":exp},
                      "created":now_str()})
        save_posts(posts)
        parse_mentions(q, pid, me["id"])
        flash("Poll posted! 🗳️", "success")
        return redirect(url_for("feed"))
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("create_poll.html", me=me, notif_count=len(notifs))

@app.route("/polls/<pid>/vote", methods=["POST"])
@login_required
def vote_poll(pid):
    me = current_user()
    oi = int(request.form.get("option",0))
    posts = load_posts()
    post  = next((p for p in posts if p["id"]==pid and p.get("is_poll")), None)
    if not post: return jsonify({"error":"not found"}), 404
    exp = post["poll"].get("expires","")
    if exp and datetime.strptime(exp,"%Y-%m-%d %H:%M:%S") < datetime.now():
        return jsonify({"error":"poll ended"}), 400
    for o in post["poll"]["options"]:
        if me["id"] in o["votes"]: o["votes"].remove(me["id"])
    if 0<=oi<len(post["poll"]["options"]):
        post["poll"]["options"][oi]["votes"].append(me["id"])
    save_posts(posts)
    total   = sum(len(o["votes"]) for o in post["poll"]["options"])
    results = [{"text":o["text"],"votes":len(o["votes"]),
                "pct":round(len(o["votes"])/total*100) if total else 0,
                "voted":me["id"] in o["votes"]} for o in post["poll"]["options"]]
    return jsonify({"results":results,"total":total})

# ════════════════════════════════════════
# ADMIN
# ════════════════════════════════════════
@app.route("/admin/verify/<uid>", methods=["POST"])
@login_required
def toggle_verified(uid):
    me    = current_user()
    users = load_users()
    if not users or users[0]["id"]!=me["id"]: abort(403)
    user  = next((u for u in users if u["id"]==uid), None)
    if user:
        user["verified"] = not user.get("verified",False)
        save_users(users)
    return redirect(request.referrer or url_for("feed"))

# ════════════════════════════════════════
# TAG & CATEGORY
# ════════════════════════════════════════
@app.route("/tag/<tag>")
@login_required
def tag_posts(tag):
    me    = current_user()
    posts = load_posts()
    users = load_users()
    um    = {u["id"]:u for u in users}
    tagged= [p for p in posts if tag.lower() in [t.lower() for t in p.get("tags",[])]
             and not p.get("is_poll")]
    tagged= sorted(tagged, key=lambda p:p["created"], reverse=True)
    for p in tagged:
        p["author"]   = um.get(p["user_id"],{})
        p["liked"]    = me["id"] in p.get("likes",[])
        p["time_ago"] = time_ago(p["created"])
    notifs = [n for n in load_notifs() if n["to_uid"]==me["id"] and not n["read"]]
    return render_template("tag_posts.html", tag=tag, posts=tagged, me=me, notif_count=len(notifs))

if __name__ == "__main__":
    app.run(debug=False, host="127.0.0.1", port=8080)
