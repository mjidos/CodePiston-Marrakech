# CodePiston Marrakech — Plateforme Full-Stack Stages & Emplois (Livrable `GROUPE_NOM_PRENOM_VF`)

**CodePiston** est une plateforme web complète permettant de **rechercher, collecter, analyser, visualiser, postuler et suivre** les opportunités d'emploi et de stage à **Marrakech (Marrakech-Safi, 40000)**.

## 1. Architecture Globale de la Solution (VS Code Mermaid & Draw.io)

```mermaid
flowchart TB
    subgraph Sources["1. Sources de Données Marrakech"]
        S1["Stagiaires.ma\n(258 offres Marrakech)"]
        S2["Trabajo.org Maroc\n(418 offres Marrakech)"]
        S3["Indeed MA & LinkedIn\n(354 offres IT Guéliz)"]
        S4["Google Cloud Talent API\n(Recherche v4)"]
        S5["Google Places API (New)\n(Géocodage GPS & Avis ★)"]
    end

    subgraph Pipeline["2. Pipeline ETL Python (marrakech_jobs_pipeline.py)"]
        P1["Collecteurs Asynchrones\n(httpx + BeautifulSoup4 + Playwright)"]
        P2["Géocodeur Hybride\n(Google Places -> OSM Nominatim -> Centroïdes Quartiers)"]
        P3["MarrakechJobCleaner\n(Nettoyage HTML, Déduplication SHA-256, Regex Salaire MAD, NLP 9 Catégories)"]
        P4[("Livrable Excel Audit\nGROUPE_NOM_PRENOM_VF_Dataset_Raw_vs_Cleaned.xlsx\n(20 Brutes -> 16 Nettoyées)")]
    end

    subgraph Backend["3. Backend FastAPI & Sécurité Isolée (kechtalent_backend_api.py)"]
        B1["Serveur REST FastAPI\n(/auth/register, /auth/login, /jobs, /kanban, /logs)"]
        B2[("Coffre-Fort Utilisateur Isolé\n(Mots de passe salés SHA-256, Kanban privé, Logs CSV)")]
        B3["Moteur IA Hybride\n(Gemini 2.5 Flash + Taxonomie Vectorielle Locale)"]
    end

    subgraph Frontend["4. Interface Utilisateur CodePiston (FR par défaut / EN)"]
        F1["Portail d'Authentification Obligatoire\n(Inscription & Connexion SHA-256 avant accès)"]
        F2["Carte Interactive Marrakech & Postuler 1-Clic\n(Guéliz, Sidi Ghanem, Hivernage, Semlalia, Targa)"]
        F3["Suivi Kanban Isolé à 5 Colonnes\n(Liste de Souhaits, Postulé, Entretien, Offre, Refusé)"]
        F4["Matchmaker CV IA & Dashboard Chart.js\n(Score 0-100%, Lettre sur mesure & 4 Graphiques)"]
    end

    S1 & S2 & S3 & S4 --> P1
    S5 --> P2
    P1 --> P2 --> P3
    P3 --> P4
    P3 --> B2
    B2 <--> B1
    B3 <--> B1
    B1 <--> F1 & F2 & F3 & F4
```

## 2. Étapes d'Installation, de Configuration et d'Exécution

### Prérequis
- Python 3.11+
- Navigateur Web moderne (Chrome, Edge, Firefox, Safari)
- Docker & Docker Compose (optionnel pour conteneurisation)

### Étape 1 : Configuration des Variables d'Environnement (`.env`)
```bash
cp .env.example .env
export GOOGLE_MAPS_API_KEY="VOTRE_CLE_GOOGLE_MAPS_ICI"
export GEMINI_API_KEY="VOTRE_CLE_GEMINI_ICI"
```

### Étape 2 : Exécution du Pipeline de Collecte et Nettoyage Python (Phase 1)
```bash
pip install -r backend/requirements.txt
python3 backend/marrakech_jobs_pipeline.py
```

### Étape 3 : Lancement du Serveur Backend FastAPI (Phase 2 & 4)
```bash
uvicorn backend.main:app --reload --port 8000
# Documentation interactive Swagger disponible sur : http://127.0.0.1:8000/docs
```

### Étape 4 : Lancement de l'Interface Frontend CodePiston (Phase 3)
Ouvrez `frontend/index.html` dans votre navigateur ou lancez `docker compose up --build` pour accéder à l'application sur `http://localhost:3000`.
