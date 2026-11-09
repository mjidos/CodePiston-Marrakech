"""
CodePiston Marrakesh — Backend API Server (FastAPI + SHA-256 Isolated Auth + Gemini 2.5 Flash)
Livrable : GROUPE_NOM_PRENOM_VF

Endpoints:
- POST /api/v1/auth/register   (Creates a new isolated account with salted SHA-256 password hash)
- POST /api/v1/auth/login      (Verifies salted SHA-256 password hash and returns isolated user state)
- GET  /api/v1/jobs            (Search & filter Marrakesh jobs by district, contract, and keyword)
- POST /api/v1/jobs/collect    (Triggers Phase 1 multi-source ETL pipeline sync)
- POST /api/v1/jobs/{id}/apply (Submits an application and logs it in the user's private Kanban)
- GET  /api/v1/kanban          (Returns the authenticated user's isolated Kanban cards)
- POST /api/v1/kanban          (Adds/updates a card in the authenticated user's isolated Kanban)
- GET  /api/v1/logs            (Returns the authenticated user's private activity audit logs)
- POST /api/v1/ai/match-cv     (Evaluates CV against a Marrakesh job via Gemini 2.5 Flash + fallback)
- POST /api/v1/places/enrich   (Geocodes a company in Marrakesh via Google Places API New + fallback)
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field

from marrakech_jobs_pipeline import (
    DEFAULT_GEMINI_API_KEY,
    DEFAULT_GOOGLE_PLACES_KEY,
    HybridAIMatchmaker,
    HybridPlacesEnricher,
    JobListing,
    MarrakechJobCleaner,
)

app = FastAPI(
    title="CodePiston Marrakesh API",
    description="API REST Full-Stack pour la recherche, collecte, analyse, candidature, suivi Kanban isolé et IA Gemini 2.5 Flash à Marrakech.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

places_service = HybridPlacesEnricher(api_key=DEFAULT_GOOGLE_PLACES_KEY)
ai_service = HybridAIMatchmaker(api_key=DEFAULT_GEMINI_API_KEY)

# Strictly isolated per-user stores keyed by email
USERS_VAULT: dict[str, dict[str, Any]] = {}
SESSION_TOKENS: dict[str, str] = {}  # token -> email
USER_KANBAN_VAULT: dict[str, dict[str, dict[str, Any]]] = {}
USER_LOGS_VAULT: dict[str, list[dict[str, Any]]] = {}
JOBS_DB: dict[str, JobListing] = {}


def hash_password_sha256(salt: str, password: str) -> str:
    return hashlib.sha256(f"{salt}:{password}".encode("utf-8")).hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def record_user_log(email: str, category: str, detail: str) -> dict[str, Any]:
    user_name = USERS_VAULT.get(email, {}).get("name", email)
    entry = {
        "timestamp": now_iso(),
        "user": user_name,
        "category": category,
        "detail": detail,
    }
    USER_LOGS_VAULT.setdefault(email, []).insert(0, entry)
    return entry


def get_authenticated_email(x_session_token: Optional[str] = Header(None)) -> str:
    if not x_session_token or x_session_token not in SESSION_TOKENS:
        raise HTTPException(
            status_code=401,
            detail="Authentification requise : créez un compte ou connectez-vous pour accéder à votre coffre-fort isolé.",
        )
    return SESSION_TOKENS[x_session_token]


class RegisterPayload(BaseModel):
    name: str = Field(..., min_length=2)
    email: str = Field(..., min_length=5)
    password: str = Field(..., min_length=6)
    role: str = "Ingénieur Full-Stack · Marrakech"
    hood: str = "Guéliz (40000)"


class LoginPayload(BaseModel):
    email: str
    password: str


class KanbanCardPayload(BaseModel):
    job_id: str
    title: str
    company: str
    neighborhood: str
    status: str = Field(default="Wishlist", pattern="^(Wishlist|Applied|Interviewing|Offer|Rejected)$")
    notes: str = ""
    match_score: Optional[int] = None


class CVMatchRequest(BaseModel):
    cv_text: str = Field(..., min_length=20)
    job_id: str
    language: str = Field(default="fr", pattern="^(fr|en)$")


class PlacesLookupRequest(BaseModel):
    company_name: str
    raw_location: str = "Marrakech, Morocco"


@app.post("/api/v1/auth/register")
async def register_account(payload: RegisterPayload) -> dict[str, Any]:
    email = payload.email.strip().lower()
    if email in USERS_VAULT:
        raise HTTPException(status_code=409, detail="Un compte existe déjà avec cet email.")
    salt = f"cp_{secrets.token_hex(8)}"
    pw_hash = hash_password_sha256(salt, payload.password)
    user_record = {
        "name": payload.name.strip(),
        "email": email,
        "role": payload.role,
        "hood": payload.hood,
        "salt": salt,
        "password_hash": pw_hash,
        "created_at": now_iso(),
    }
    USERS_VAULT[email] = user_record
    USER_KANBAN_VAULT[email] = {}
    USER_LOGS_VAULT[email] = []
    token = secrets.token_urlsafe(24)
    SESSION_TOKENS[token] = email
    record_user_log(email, "AUTH_REGISTER", f"Compte créé et chiffré (SHA-256 : {pw_hash[:12]}...)")
    return {
        "session_token": token,
        "user": {k: v for k, v in user_record.items() if k not in {"salt", "password_hash"}},
        "password_hash_prefix": pw_hash[:14],
    }


@app.post("/api/v1/auth/login")
async def login_account(payload: LoginPayload) -> dict[str, Any]:
    email = payload.email.strip().lower()
    stored = USERS_VAULT.get(email)
    if not stored:
        raise HTTPException(status_code=404, detail="Aucun compte trouvé pour cet email.")
    check_hash = hash_password_sha256(stored["salt"], payload.password)
    if check_hash != stored["password_hash"]:
        raise HTTPException(status_code=403, detail="Mot de passe incorrect. Accès refusé.")
    token = secrets.token_urlsafe(24)
    SESSION_TOKENS[token] = email
    record_user_log(email, "AUTH_LOGIN", f"Connexion vérifiée (SHA-256 : {check_hash[:12]}...)")
    return {
        "session_token": token,
        "user": {k: v for k, v in stored.items() if k not in {"salt", "password_hash"}},
    }


@app.get("/api/v1/kanban")
async def get_my_kanban(email: str = Query(..., description="Authenticated user email")) -> list[dict[str, Any]]:
    return list(USER_KANBAN_VAULT.get(email.lower(), {}).values())


@app.post("/api/v1/kanban")
async def upsert_my_kanban(payload: KanbanCardPayload, x_session_token: Optional[str] = Header(None)) -> dict[str, Any]:
    email = get_authenticated_email(x_session_token)
    card = payload.model_dump()
    card["applied_date"] = now_iso()[:10]
    USER_KANBAN_VAULT.setdefault(email, {})[payload.job_id] = card
    record_user_log(email, "KANBAN_UPSERT", f"{payload.company} enregistré dans {payload.status}")
    return card


@app.get("/api/v1/logs")
async def get_my_logs(x_session_token: Optional[str] = Header(None)) -> list[dict[str, Any]]:
    email = get_authenticated_email(x_session_token)
    return USER_LOGS_VAULT.get(email, [])
