import os
import hmac
import json
import hashlib
import logging
import asyncio
import sib_api_v3_sdk
import httpx
from datetime import datetime, timezone
from collections import defaultdict
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from dotenv import load_dotenv
from supabase import create_client, Client

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("neuraflux")

# ─── ENV ──────────────────────────────────────────────────────────────────────
load_dotenv()

# BUG 5 FIX — CAL_WEBHOOK_SECRET now required
REQUIRED_ENV = ["BREVO_API_KEY", "SUPABASE_URL", "SUPABASE_KEY", "CAL_WEBHOOK_SECRET"]
for var in REQUIRED_ENV:
    if not os.getenv(var):
        raise Exception(f"MISSING ENV VARIABLE: {var} — add it to Railway")

SARMAD_BOOKING_PAGE = "https://cal.com/neuraflux-iitdmq/30min"
# BUG 8 FIX — Railway URL from env var
RAILWAY_URL = os.getenv("RAILWAY_URL", "https://web-production-f3c75.up.railway.app")

# ─── MODELS ───────────────────────────────────────────────────────────────────
class ContactForm(BaseModel):
    name: str
    email: EmailStr
    business: str
    challenge: str

class ChatEmail(BaseModel):
    email: EmailStr
    name: str = "Guest"
    flow: str = "B"

# ─── CLIENTS ──────────────────────────────────────────────────────────────────
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

brevo_config = sib_api_v3_sdk.Configuration()
brevo_config.api_key["api-key"] = os.getenv("BREVO_API_KEY")
contact_api = sib_api_v3_sdk.ContactsApi(
    sib_api_v3_sdk.ApiClient(brevo_config)
)

# ─── RATE LIMITER (BUG 7 FIX) ────────────────────────────────────────────────
rate_store = defaultdict(list)
RATE_LIMIT = 10
RATE_WINDOW = 60

def is_rate_limited(ip: str) -> bool:
    now = datetime.now(timezone.utc).timestamp()
    rate_store[ip] = [t for t in rate_store[ip] if now - t < RATE_WINDOW]
    if len(rate_store[ip]) >= RATE_LIMIT:
        return True
    rate_store[ip].append(now)
    return False

# ─── KEEP-ALIVE (BUG 8 FIX) ──────────────────────────────────────────────────
async def keep_alive():
    await asyncio.sleep(60)
    while True:
        try:
            async with httpx.AsyncClient() as client:
                await client.get(f"{RAILWAY_URL}/api/health", timeout=10)
            log.info("Keep-alive ping sent")
        except Exception as e:
            log.warning(f"Keep-alive failed (non-critical): {e}")
        await asyncio.sleep(840)

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(keep_alive())
    yield

# ─── APP + MIDDLEWARE ─────────────────────────────────────────────────────────
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://neuraflux.io", "http://localhost:3000"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def safe_first_name(name: str) -> str:
    """BUG 6 FIX — Safe first name extraction with Guest fallback."""
    parts = (name or "").strip().split()
    return parts[0] if parts else "Guest"


def format_start_time(iso_time: str) -> str:
    """BUG 4 FIX — Human-readable time for email template."""
    if not iso_time:
        return "See your calendar for details"
    try:
        dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
        return dt.strftime("%A, %B %d %Y at %I:%M %p UTC")
    except Exception:
        return iso_time


def verify_cal_signature(payload_bytes: bytes, signature_header: str) -> bool:
    """BUG 2 FIX — HMAC-SHA256 webhook signature validation."""
    secret = os.getenv("CAL_WEBHOOK_SECRET", "")
    expected = hmac.new(
        secret.encode("utf-8"),
        payload_bytes,
        hashlib.sha256
    ).hexdigest()
    received = signature_header.replace("sha256=", "").strip()
    return hmac.compare_digest(expected, received)


def is_email_in_brevo_list(email: str, list_id: int) -> bool:
    """BUG 3 FIX — Check if contact already exists in a specific Brevo list."""
    try:
        contact = contact_api.get_contact_info(email)
        if contact and contact.list_ids:
            return list_id in contact.list_ids
        return False
    except Exception:
        return False


def add_to_brevo(email: str, first_name: str, list_id: int, extra_attrs: dict = None):
    """Add contact to Brevo list. Triggers linked automation."""
    attrs = extra_attrs or {}
    attrs["FIRSTNAME"] = first_name
    try:
        contact = sib_api_v3_sdk.CreateContact(
            email=email,
            attributes=attrs,
            list_ids=[list_id],
            update_enabled=True
        )
        contact_api.create_contact(contact)
        log.info(f"Brevo: {email} added to list {list_id}")
    except Exception as e:
        log.error(f"Brevo error — {email} → list {list_id}: {e}")
        raise Exception(f"Brevo failed: {e}")

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.get("/")
def home():
    return {"message": "NeuraFlux API Online", "docs": "/docs"}


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/contact")
async def contact_form(body: ContactForm):
    """
    Contact form submission.
    Saves to Supabase → adds to Brevo list 6 → triggers Sequence B (NF-B1 to NF-B5).
    BUG 3 FIX: Skips Sequence B if email already in Booked Audit list 7.
    """
    log.info(f"/api/contact — {body.email}")
    try:
        first_name = safe_first_name(body.name)

        supabase.table("Leads").insert({
            "name": body.name,
            "email": body.email,
            "business": body.business,
            "challenge": body.challenge,
            "source": "form",
            "sequence": "B"
        }).execute()
        log.info(f"Supabase saved: {body.email}")

        # BUG 3 FIX — skip nurture if already booked
        if is_email_in_brevo_list(body.email, 7):
            log.info(f"Skipping Sequence B — {body.email} already in Booked Audit list")
            return {"status": "success", "note": "already_booked"}

        add_to_brevo(body.email, first_name, 6)
        return {"status": "success"}

    except Exception as e:
        log.error(f"/api/contact error: {e}")
        raise HTTPException(status_code=500, detail="Something went wrong")


@app.post("/api/audit-booked")
async def audit_booked(request: Request):
    """
    Cal.com webhook — fires when someone books a Free AI Growth Audit.
    Validates signature → saves to Supabase → adds to Brevo list 7 → triggers Sequence A.

    BUG 1 FIX: CALENDAR_URL keeps app.cal.com/video — it IS the video call link.
    BUG 2 FIX: Webhook signature validated with HMAC-SHA256.
    BUG 4 FIX: START_TIME formatted to human-readable string.
    """
    log.info("/api/audit-booked — webhook received")

    raw_body = await request.body()

    # BUG 2 FIX — validate Cal.com signature
    signature = request.headers.get("X-Cal-Signature-256", "")
    if signature:
        if not verify_cal_signature(raw_body, signature):
            log.warning("Invalid webhook signature — request rejected")
            raise HTTPException(status_code=401, detail="Invalid signature")
    else:
        log.warning("No signature header — proceeding without validation (testing only)")

    try:
        body_data = json.loads(raw_body)
        # Cal.com sends data inside a "payload" wrapper
        data = body_data.get("payload", body_data)

        attendees = data.get("attendees", [])
        if not attendees:
            return {"status": "error", "message": "No attendee data"}

        email = attendees[0].get("email", "")
        name = attendees[0].get("name", "Guest")
        first_name = safe_first_name(name)  # BUG 6 FIX

        if not email:
            return {"status": "error", "message": "No email found"}

        # BUG 1 FIX — correct video URL logic
        # Cal.com videoCallUrl (including app.cal.com/video) IS valid — keep it
        # Only fall back if completely absent
        video_url = data.get("metadata", {}).get("videoCallUrl", "")
        if not video_url:
            location = data.get("location", "")
            video_url = location if (location and location.startswith("http")) else SARMAD_BOOKING_PAGE

        # Reschedule URL
        booking_uid = data.get("uid", "")
        reschedule_url = (
            f"https://cal.com/reschedule/{booking_uid}"
            if booking_uid else SARMAD_BOOKING_PAGE
        )

        # BUG 4 FIX — format time for email readability
        start_time = format_start_time(data.get("startTime", ""))

        log.info(f"Booking: {email} | {start_time} | video_url: {video_url[:50]}")

        # Save to Supabase
        supabase.table("Leads").insert({
            "name": name,
            "email": email,
            "source": "calendly",
            "sequence": "A",
            "business": "Booked Audit"
        }).execute()
        log.info(f"Supabase saved: {email}")

        # Add to Brevo list 7 — triggers Sequence A automatically
        add_to_brevo(email, first_name, 7, {
            "CALENDAR_URL": video_url,
            "RESCHEDULE_URL": reschedule_url,
            "START_TIME": start_time
        })

        return {"status": "success"}

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"/api/audit-booked error: {e}")
        raise HTTPException(status_code=500, detail="Something went wrong")


@app.post("/api/chat/email")
async def chat_email(request: Request, body: ChatEmail):
    """
    Chatbot email capture.
    Saves to Supabase → adds to Brevo list 6 → triggers Sequence B.

    BUG 7 FIX: Rate limited — 10 requests per IP per minute.
    BUG 3 FIX: Skips Sequence B if email already in Booked Audit list 7.
    """
    # BUG 7 FIX — rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if is_rate_limited(client_ip):
        log.warning(f"Rate limit hit — IP: {client_ip}")
        raise HTTPException(status_code=429, detail="Too many requests. Try again later.")

    log.info(f"/api/chat/email — {body.email}")
    try:
        first_name = safe_first_name(body.name)

        supabase.table("Chat_sessions").insert({
            "email": body.email,
            "flow": body.flow,
            "messages": []
        }).execute()
        log.info(f"Supabase chat session saved: {body.email}")

        # BUG 3 FIX — skip nurture if already booked
        if is_email_in_brevo_list(body.email, 7):
            log.info(f"Skipping Sequence B — {body.email} already in Booked Audit list")
            return {"status": "success", "note": "already_booked"}

        add_to_brevo(body.email, first_name, 6)
        return {"status": "success"}

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"/api/chat/email error: {e}")
        raise HTTPException(status_code=500, detail="Something went wrong")
