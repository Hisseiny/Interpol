#!/usr/bin/env python3
"""
Scraper Interpol Red Notices - VERSION PARALLÈLE
- Stratégie de collecte de tâches (Phase 0 & 1)
- Exécution parallèle du téléchargement des détails (Phase 2)
- Cible : 100% des notices, plus rapidement.
"""

import os
import sys
import csv
import json
import time
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Iterable, Set, Tuple
from urllib.request import Request, urlopen
import ssl
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

API_URL = "https://ws-public.interpol.int/notices/v1/red"

RESULTS_PER_PAGE = 160  # Stable et testé
DELAY = float(os.getenv("SCRAPER_DELAY") or "0.5")

# Nombre de "travailleurs" pour télécharger les détails en parallèle
# AUGMENTER = plus rapide, mais plus risqué (blocage IP)
# DIMINUER = plus lent, mais plus sûr
MAX_WORKERS = 20

HEADERS = {
    "accept": "*/*",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.interpol.int",
    "priority": "u=1, i",
    "referer": "https://www.interpol.int/",
    "sec-ch-ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
}
if os.getenv("SCRAPER_COOKIE"):
    HEADERS["cookie"] = os.getenv("SCRAPER_COOKIE").strip()

# Dictionnaire ISO → noms complets
COUNTRY_NAMES = {
    "AD": "Andorre", "AE": "Émirats arabes unis", "AF": "Afghanistan", "AG": "Antigua-et-Barbuda",
    "AI": "Anguilla", "AL": "Albanie", "AM": "Arménie", "AO": "Angola", "AQ": "Antarctique",
    "AR": "Argentine", "AS": "Samoa américaines", "AT": "Autriche", "AU": "Australie",
    "AW": "Aruba", "AX": "Åland", "AZ": "Azerbaïdjan", "BA": "Bosnie-Herzégovine",
    "BB": "Barbade", "BD": "Bangladesh", "BE": "Belgique", "BF": "Burkina Faso",
    "BG": "Bulgarie", "BH": "Bahreïn", "BI": "Burundi", "BJ": "Bénin", "BL": "Saint-Barthélemy",
    "BM": "Bermudes", "BN": "Brunei", "BO": "Bolivie", "BQ": "Bonaire", "BR": "Brésil",
    "BS": "Bahamas", "BT": "Bhoutan", "BV": "Bouvet", "BW": "Botswana", "BY": "Biélorussie",
    "BZ": "Belize", "CA": "Canada", "CC": "Îles Cocos", "CD": "Congo (RDC)", "CF": "République centrafricaine",
    "CG": "Congo", "CH": "Suisse", "CI": "Côte d'Ivoire", "CK": "Îles Cook", "CL": "Chili",
    "CM": "Cameroun", "CN": "Chine", "CO": "Colombie", "CR": "Costa Rica", "CU": "Cuba",
    "CV": "Cap-Vert", "CW": "Curaçao", "CX": "Île Christmas", "CY": "Chypre", "CZ": "Tchéquie",
    "DE": "Allemagne", "DJ": "Djibouti", "DK": "Danemark", "DM": "Dominique", "DO": "République dominicaine",
    "DZ": "Algérie", "EC": "Équateur", "EE": "Estonie", "EG": "Égypte", "EH": "Sahara occidental",
    "ER": "Érythrée", "ES": "Espagne", "ET": "Éthiopie", "FI": "Finlande", "FJ": "Fidji",
    "FK": "Îles Malouines", "FM": "Micronésie", "FO": "Îles Féroé", "FR": "France",
    "GA": "Gabon", "GB": "Royaume-Uni", "GD": "Grenade", "GE": "Géorgie", "GF": "Guyane française",
    "GG": "Guernesey", "GH": "Ghana", "GI": "Gibraltar", "GL": "Groenland", "GM": "Gambie",
    "GN": "Guinée", "GP": "Guadeloupe", "GQ": "Guinée équatoriale", "GR": "Grèce",
    "GS": "Géorgie du Sud", "GT": "Guatemala", "GU": "Guam", "GW": "Guinée-Bissau",
    "GY": "Guyana", "HK": "Hong Kong", "HM": "Heard-et-MacDonald", "HN": "Honduras",
    "HR": "Croatie", "HT": "Haïti", "HU": "Hongrie", "ID": "Indonésie", "IE": "Irlande",
    "IL": "Israël", "IM": "Île de Man", "IN": "Inde", "IO": "Territoire britannique de l'océan Indien",
    "IQ": "Irak", "IR": "Iran", "IS": "Islande", "IT": "Italie", "JE": "Jersey",
    "JM": "Jamaïque", "JO": "Jordanie", "JP": "Japon", "KE": "Kenya", "KG": "Kirghizistan",
    "KH": "Cambodge", "KI": "Kiribati", "KM": "Comores", "KN": "Saint-Christophe-et-Niévès",
    "KP": "Corée du Nord", "KR": "Corée du Sud", "KW": "Koweït", "KY": "Îles Caïmans",
    "KZ": "Kazakhstan", "LA": "Laos", "LB": "Liban", "LC": "Sainte-Lucie", "LI": "Liechtenstein",
    "LK": "Sri Lanka", "LR": "Liberia", "LS": "Lesotho", "LT": "Lituanie", "LU": "Luxembourg",
    "LV": "Lettonie", "LY": "Libye", "MA": "Maroc", "MC": "Monaco", "MD": "Moldavie",
    "ME": "Monténégro", "MF": "Saint-Martin", "MG": "Madagascar", "MH": "Îles Marshall",
    "MK": "Macédoine du Nord", "ML": "Mali", "MM": "Myanmar", "MN": "Mongolie", "MO": "Macao",
    "MP": "Îles Mariannes du Nord", "MQ": "Martinique", "MR": "Mauritanie", "MS": "Montserrat",
    "MT": "Malte", "MU": "Maurice", "MV": "Maldives", "MW": "Malawi", "MX": "Mexique",
    "MY": "Malaisie", "MZ": "Mozambique", "NA": "Namibie", "NC": "Nouvelle-Calédonie",
    "NE": "Niger", "NF": "Île Norfolk", "NG": "Nigeria", "NI": "Nicaragua", "NL": "Pays-Bas",
    "NO": "Norvège", "NP": "Népal", "NR": "Nauru", "NU": "Niue", "NZ": "Nouvelle-Zélande",
    "OM": "Oman", "PA": "Panama", "PE": "Pérou", "PF": "Polynésie française", "PG": "Papouasie-Nouvelle-Guinée",
    "PH": "Philippines", "PK": "Pakistan", "PL": "Pologne", "PM": "Saint-Pierre-et-Miquelon",
    "PN": "Pitcairn", "PR": "Porto Rico", "PS": "Palestine", "PT": "Portugal", "PW": "Palaos",
    "PY": "Paraguay", "QA": "Qatar", "RE": "La Réunion", "RO": "Roumanie", "RS": "Serbie",
    "RU": "Russie", "RW": "Rwanda", "SA": "Arabie saoudite", "SB": "Îles Salomon",
    "SC": "Seychelles", "SD": "Soudan", "SE": "Suède", "SG": "Singapour", "SH": "Sainte-Hélène",
    "SI": "Slovénie", "SJ": "Svalbard et Jan Mayen", "SK": "Slovaquie", "SL": "Sierra Leone",
    "SM": "Saint-Marin", "SN": "Sénégal", "SO": "Somalie", "SR": "Suriname", "SS": "Soudan du Sud",
    "ST": "Sao Tomé-et-Principe", "SV": "Salvador", "SX": "Saint-Martin", "SY": "Syrie",
    "SZ": "Eswatini", "TC": "Îles Turques-et-Caïques", "TD": "Tchad", "TF": "Terres australes françaises",
    "TG": "Togo", "TH": "Thaïlande", "TJ": "Tadjikistan", "TK": "Tokelau", "TL": "Timor oriental",
    "TM": "Turkménistan", "TN": "Tunisie", "TO": "Tonga", "TR": "Turquie", "TT": "Trinité-et-Tobago",
    "TV": "Tuvalu", "TW": "Taïwan", "TZ": "Tanzanie", "UA": "Ukraine", "UG": "Ouganda",
    "UM": "Îles mineures éloignées des États-Unis", "US": "États-Unis", "UY": "Uruguay",
    "UZ": "Ouzbékistan", "VA": "Vatican", "VC": "Saint-Vincent-et-les-Grenadines",
    "VE": "Venezuela", "VG": "Îles Vierges britanniques", "VI": "Îles Vierges américaines",
    "VN": "Vietnam", "VU": "Vanuatu", "WF": "Wallis-et-Futuna", "WS": "Samoa", "YE": "Yémen",
    "YT": "Mayotte", "ZA": "Afrique du Sud", "ZM": "Zambie", "ZW": "Zimbabwe"
}

def convert_country_code(code: str) -> str:
    if not code:
        return ""
    code_upper = code.strip().upper()
    return COUNTRY_NAMES.get(code_upper, code)

def http_get_json(url: str, params: Optional[Dict[str, str]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    if params:
        url = f"{url}?{urlencode(params)}"
    req = Request(url, headers=(headers or HEADERS))
    ctx: Optional[ssl.SSLContext] = None
    if os.getenv("SCRAPER_INSECURE") == "1":
        ctx = ssl._create_unverified_context()
    with urlopen(req, timeout=30, context=ctx) as resp:
        data = resp.read().decode("utf-8", errors="replace")
    try:
        return json.loads(data)
    except Exception:
        return {}

def clean_text(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(f"<div>{text}</div>", "html.parser")
    cleaned = soup.get_text(separator=" ")
    return " ".join(cleaned.split())

def classify_infraction(text: str) -> str:
    if not text:
        return ""
    text_lower = text.lower()
    interpol_categories = {
        "Terrorisme et sécurité publique": ["terrorism", "terrorist", "armed formation", "explosive", "bomb", "attack", "recruit", "training", "organization", "wmd", "conspiracy to kill", "armed group", "national defence", "terror plot", "bombing", "mass destruction"],
        "Meurtre, tentative de meurtre et crimes violents": ["murder", "homicide", "attempted murder", "assault", "aggravated assault", "armed robbery", "violence", "robbery", "femicidio", "feminicidio", "asesinato", "agravado", "homicidio", "manslaughter", "kill", "slaying"],
        "Crimes sexuels aggravés et abus": ["rape", "sexual assault", "sexual abuse", "indecent", "sex offence", "sodomy", "abuso", "viol", "violación", "agression sexuelle", "statutory rape", "harcèlement sexuel", "violence sexuelle", "attentat à la pudeur"],
        "Exploitation et pornographie infantile": ["child pornography", "pornography", "mineur", "minor", "child", "indecency with a child", "sexual abuse of minor", "exploitation enfant", "abus sur mineur", "child abuse"],
        "Traite des êtres humains et enlèvements": ["trafficking", "human", "kidnapping", "hostage", "abduction", "slavery", "migrant", "illegal entry", "smuggling", "captivity", "traite", "enlèvement", "séquestration", "aide à l'entrée irrégulière", "migration illégale"],
        "Criminalité organisée et conspiration": ["organized crime", "criminal organization", "association de malfaiteurs", "conspiracy", "participation", "illicit association", "gang", "group", "asociacion ilicita", "membership of a criminal organisation", "union criminelle"],
        "Cybercriminalité et crimes technologiques": ["cybercrime", "hacking", "malware", "phishing", "ransomware", "computer", "digital", "data", "cryptology", "encryption", "forgery digital", "piratage", "usurpation", "intrusion", "cryptographie", "refus de remettre clé de chiffrement"],
        "Trafic de drogues et substances illicites": ["drug", "drugs", "narcotic", "psychotropic", "trafficking", "marijuana", "cocaine", "heroin", "distribution", "stupéfiant", "importation", "transport", "unauthorised", "production", "manufacturing", "cannabis", "substance", "illicit traffic"],
        "Crimes financiers et corruption": ["fraud", "money laundering", "bribery", "forgery", "financial", "corruption", "laundering", "breach of trust", "false declaration", "market manipulation", "escroquerie", "abus de confiance", "blanchiment", "détournement", "pots-de-vin", "fraude fiscale"],
        "Trafic d'armes et explosifs": ["firearms", "weapons", "arms", "ammunition", "explosives", "illegal possession", "transport", "acquisition", "armas", "munitions", "fabrication d'explosifs", "arme de guerre", "arms act", "catégorie b", "catégorie c", "catégorie d"],
        "Crimes environnementaux et patrimoine culturel": ["environment", "wildlife", "pollution", "cultural", "heritage", "artefact", "environmental", "illegal logging", "braconnage", "espèces protégées", "trafic d'ivoire", "musée", "art theft"],
        "Autres infractions": ["evasion", "escape", "absconding", "custody", "unlawful", "detention", "obstruction", "entrave", "fuite", "détention illégale", "désobéissance"]
    }
    severity_rank = {
        "Terrorisme et sécurité publique": 10, "Meurtre, tentative de meurtre et crimes violents": 9,
        "Crimes sexuels aggravés et abus": 8, "Exploitation et pornographie infantile": 8,
        "Traite des êtres humains et enlèvements": 7, "Criminalité organisée et conspiration": 6,
        "Cybercriminalité et crimes technologiques": 5, "Trafic de drogues et substances illicites": 5,
        "Crimes financiers et corruption": 4, "Trafic d'armes et explosifs": 4,
        "Crimes environnementaux et patrimoine culturel": 3, "Autres infractions": 2, "Non classé": 1
    }
    matched = []
    for category, keywords in interpol_categories.items():
        for keyword in keywords:
            if keyword in text_lower:
                matched.append(category)
                break
    if not matched:
        return "Non classé"
    best_category = max(matched, key=lambda cat: severity_rank.get(cat, 0))
    return best_category

def extract_age_from_dob(dob: str) -> str:
    if not dob or len(dob) < 4 or not dob[:4].isdigit():
        return ""
    try:
        year = int(dob[:4])
        age = datetime.now().year - year
        return str(age) if 0 <= age <= 120 else ""
    except Exception:
        return ""

def iter_notices(data: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    emb = data.get("_embedded", {})
    if isinstance(emb, dict):
        arr = emb.get("notices", [])
        if isinstance(arr, list):
            for item in arr:
                if isinstance(item, dict):
                    yield item

def extract_infractions(obj: Optional[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    if not obj:
        return out
    aws = obj.get("arrest_warrants")
    if not isinstance(aws, list):
        return out
    for aw in aws:
        if not isinstance(aw, dict):
            continue
        ch = aw.get("charge")
        if ch:
            classified = classify_infraction(str(ch))
            if classified and classified not in out:
                out.append(classified)
        chs = aw.get("charges")
        if isinstance(chs, list):
            for c in chs:
                classified = classify_infraction(str(c))
                if classified and classified not in out:
                    out.append(classified)
        tr = aw.get("charge_translation")
        if tr:
            classified = classify_infraction(str(tr))
            if classified and classified not in out:
                out.append(classified)
    return out

def extract_list_value(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, list):
        if len(value) > 0:
            return clean_text(str(value[0]))
        return ""
    value_str = str(value).strip()
    if value_str.startswith('[') and value_str.endswith(']'):
        value_str = value_str[1:-1].strip()
        value_str = value_str.strip("'\"")
        if ',' in value_str:
            value_str = value_str.split(',')[0].strip("'\" ")
        return clean_text(value_str)
    return clean_text(value_str)

def extract_distinguishing_marks(detail: Optional[Dict[str, Any]]) -> str:
    if not detail:
        return ""
    marks = detail.get("distinguishing_marks")
    if marks:
        return clean_text(str(marks))
    return ""

def fetch_detail(url: str) -> Optional[Dict[str, Any]]:
    if not url:
        return None
    try:
        # Note : le time.sleep(0.1) est maintenant moins bloquant
        # car il s'exécute dans un thread séparé.
        time.sleep(0.1) 
        data = http_get_json(url, headers=HEADERS)
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def fetch_detail_by_entity_id(entity_id: str) -> Optional[Dict[str, Any]]:
    if not entity_id:
        return None
    detail_url = f"https://ws-public.interpol.int/notices/v1/red/{entity_id}"
    try:
        time.sleep(0.1)
        data = http_get_json(detail_url, headers=HEADERS)
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def normalize_notice(raw: Dict[str, Any], detail: Optional[Dict[str, Any]]) -> Dict[str, str]:
    name = clean_text(str(raw.get("name") or ""))
    forename = clean_text(str(raw.get("forename") or ""))
    
    dob = str(raw.get("date_of_birth") or "").strip()
    if not dob and detail:
        dob = str(detail.get("date_of_birth") or "").strip()
    age = extract_age_from_dob(dob)
    
    sex = clean_text(str(raw.get("sex_id") or raw.get("sex") or ""))
    if not sex and detail:
        sex = clean_text(str(detail.get("sex_id") or detail.get("sex") or ""))

    place_of_birth = clean_text(str(raw.get("place_of_birth") or ""))
    if not place_of_birth and detail:
        place_of_birth = clean_text(str(detail.get("place_of_birth") or ""))

    height = ""
    weight = ""
    hair_color = ""
    eye_color = ""
    
    if detail and detail.get("height"):
        height = clean_text(str(detail.get("height")))
    elif raw.get("height"):
        height = clean_text(str(raw.get("height")))
    
    if detail and detail.get("weight"):
        weight = clean_text(str(detail.get("weight")))
    elif raw.get("weight"):
        weight = clean_text(str(raw.get("weight")))
    
    if detail and detail.get("hairs_id"):
        hair_color = extract_list_value(detail.get("hairs_id"))
    elif detail and detail.get("hair_color"):
        hair_color = extract_list_value(detail.get("hair_color"))
    elif raw.get("hairs_id"):
        hair_color = extract_list_value(raw.get("hairs_id"))
    elif raw.get("hair_color"):
        hair_color = extract_list_value(raw.get("hair_color"))
    
    if detail and detail.get("eyes_colors_id"):
        eye_color = extract_list_value(detail.get("eyes_colors_id"))
    elif detail and detail.get("eye_color"):
        eye_color = extract_list_value(detail.get("eye_color"))
    elif raw.get("eyes_colors_id"):
        eye_color = extract_list_value(raw.get("eyes_colors_id"))
    elif raw.get("eye_color"):
        eye_color = extract_list_value(raw.get("eye_color"))
    
    distinguishing_marks = extract_distinguishing_marks(detail)
    
    languages = ""
    if detail and detail.get("languages_spoken_ids"):
        langs = detail.get("languages_spoken_ids", [])
        if isinstance(langs, list):
            languages = ", ".join([clean_text(str(lang)) for lang in langs])
    elif detail and detail.get("languages_spoken"):
        langs = detail.get("languages_spoken", [])
        if isinstance(langs, list):
            languages = ", ".join([clean_text(str(lang)) for lang in langs])

    nat = ""
    nats = raw.get("nationalities")
    if isinstance(nats, list) and nats:
        nat_code = clean_text(str(nats[0]))
        nat = convert_country_code(nat_code)
    elif nats:
        nat_code = clean_text(str(nats))
        nat = convert_country_code(nat_code)
    
    if not nat and detail:
        nats = detail.get("nationalities")
        if isinstance(nats, list) and nats:
            nat_code = clean_text(str(nats[0]))
            nat = convert_country_code(nat_code)

    entity_id = str(raw.get("entity_id") or raw.get("id") or "").strip()
    notice_id = str(raw.get("notice_id") or "").strip()

    url = ""
    links = raw.get("_links")
    if isinstance(links, dict):
        sl = links.get("self")
        if isinstance(sl, dict):
            url = str(sl.get("href") or "").strip()
        elif isinstance(sl, str):
            url = sl.strip()

    warrant_country = ""
    for source in (raw, detail):
        if isinstance(source, dict):
            aws = source.get("arrest_warrants")
            if isinstance(aws, list):
                for aw in aws:
                    if isinstance(aw, dict):
                        wc = aw.get("issuing_country_id") or aw.get("issuing_country")
                        if wc:
                            wc_code = clean_text(str(wc))
                            warrant_country = convert_country_code(wc_code)
                            break
        if warrant_country:
            break

    infractions: List[str] = []
    infractions += extract_infractions(raw)
    infractions += extract_infractions(detail)
    seen: Set[str] = set()
    infractions = [x for x in infractions if not (x in seen or seen.add(x))]
    infractions_joined = " | ".join(infractions)

    return {
        "name": name, "forename": forename, "date_of_birth": dob, "age": age, "sex": sex,
        "place_of_birth": place_of_birth, "nationality": nat, "height": height, "weight": weight,
        "hair_color": hair_color, "eye_color": eye_color, "distinguishing_marks": distinguishing_marks,
        "languages": languages, "entity_id": entity_id, "notice_id": notice_id,
        "warrant_country": warrant_country, "url": url, "infractions": infractions_joined,
    }

def fetch_page(page: int) -> Dict[str, Any]:
    params = {"page": str(page), "resultPerPage": str(RESULTS_PER_PAGE)}
    return http_get_json(API_URL, params=params, headers=HEADERS)

def fetch_page_with_filters(page: int, nationality: Optional[str] = None, age_min: Optional[int] = None, age_max: Optional[int] = None, sex_id: Optional[str] = None) -> Dict[str, Any]:
    params = {"page": str(page), "resultPerPage": str(RESULTS_PER_PAGE)}
    if nationality:
        params["nationality"] = nationality
    if age_min is not None:
        params["ageMin"] = str(age_min)
    if age_max is not None:
        params["ageMax"] = str(age_max)
    if sex_id:
        params["sexId"] = sex_id
    return http_get_json(API_URL, params=params, headers=HEADERS)

def get_total_with_filters(nationality: Optional[str] = None, age_min: Optional[int] = None, age_max: Optional[int] = None, sex_id: Optional[str] = None) -> int:
    try:
        data = fetch_page_with_filters(1, nationality, age_min, age_max, sex_id)
        if not data:
            return 0
        total = int(data.get("total", 0))
        if total <= 0:
            notices = list(iter_notices(data))
            total = len(notices)
        return total
    except Exception:
        return 0

# --- MODIFIÉ ---
# Renommé et modifié pour retourner une liste de TÂCHES (tuples)
# au lieu de lignes CSV finales.
def fetch_all_pages_for_filters_TASKS(
    nationality: Optional[str], 
    age_min: Optional[int], 
    age_max: Optional[int], 
    sex_id: Optional[str], 
    seen_ids: Set[str], 
    delay: float
) -> List[Tuple[Dict, str, str]]:
    
    tasks: List[Tuple[Dict, str, str]] = []
    total = get_total_with_filters(nationality, age_min, age_max, sex_id)
    if total == 0:
        return tasks
    
    num_pages = math.ceil(total / RESULTS_PER_PAGE)
    
    for page in range(1, num_pages + 1):
        if page > 1:
            time.sleep(delay)
        data = fetch_page_with_filters(page, nationality, age_min, age_max, sex_id)
        notices = list(iter_notices(data))
        
        for item in notices:
            eid = str(item.get("entity_id") or item.get("id") or "").strip()
            nurl = ""
            links = item.get("_links")
            if isinstance(links, dict):
                sl = links.get("self")
                if isinstance(sl, dict):
                    nurl = str(sl.get("href") or "").strip()
                elif isinstance(sl, str):
                    nurl = sl.strip()
            
            key = eid
            if not key:
                notice_id = str(item.get("notice_id") or "").strip()
                key = notice_id
            if not key:
                key = nurl
            if not key:
                name = str(item.get('name', '')).strip()
                forename = str(item.get('forename', '')).strip()
                dob = str(item.get('date_of_birth', '')).strip()
                sex = str(item.get('sex_id', '')).strip()
                key = f"{name}|{forename}|{dob}|{sex}"
            
            if key in seen_ids:
                continue
            seen_ids.add(key)
            
            # --- CHANGEMENT CLÉ ---
            # N'appelle pas fetch_detail ou normalize_notice
            # Ajoute juste la tâche (notice brute, url, eid)
            tasks.append((item, nurl, eid))
            
    return tasks

# --- MODIFIÉ ---
# Renommé et modifié pour retourner une liste de TÂCHES (tuples)
def recursive_age_split_TASKS(
    country: str, 
    sex_id: Optional[str], 
    age_min: int, 
    age_max: int, 
    seen_ids: Set[str], 
    delay: float, 
    depth: int = 0
) -> List[Tuple[Dict, str, str]]:
    
    all_tasks: List[Tuple[Dict, str, str]] = []
    MAX_DEPTH = 10
    if depth > MAX_DEPTH:
        return all_tasks
    
    total = get_total_with_filters(country, age_min, age_max, sex_id)
    if total == 0:
        return all_tasks
    
    if total >= 160 and (age_max - age_min) > 0:
        mid = (age_min + age_max) // 2
        tasks1 = recursive_age_split_TASKS(country, sex_id, age_min, mid, seen_ids, delay, depth + 1)
        tasks2 = recursive_age_split_TASKS(country, sex_id, mid + 1, age_max, seen_ids, delay, depth + 1)
        all_tasks.extend(tasks1)
        all_tasks.extend(tasks2)
        return all_tasks
    
    if (age_max - age_min) <= 1 and total >= 160:
        if sex_id is None:
            for sx in ["M", "F", "U"]:
                all_tasks.extend(recursive_age_split_TASKS(country, sx, age_min, age_max, seen_ids, delay, depth + 1))
            return all_tasks
            
    # Appelle la version _TASKS
    all_tasks.extend(fetch_all_pages_for_filters_TASKS(country, age_min, age_max, sex_id, seen_ids, delay))
    return all_tasks

# --- MODIFIÉ ---
# Renommé et modifié pour retourner une liste de TÂCHES (tuples)
def smart_fetch_country_TASKS(country: str, seen_ids: Set[str], delay: float) -> List[Tuple[Dict, str, str]]:
    all_tasks: List[Tuple[Dict, str, str]] = []
    try:
        total_country = get_total_with_filters(country)
        if total_country == 0:
            return all_tasks
        
        if total_country <= 160:
            # Appelle la version _TASKS
            tasks = fetch_all_pages_for_filters_TASKS(country, None, None, None, seen_ids, delay)
            all_tasks.extend(tasks)
        else:
            sex_ids = ["M", "F", "U"]
            for sex_id in sex_ids:
                total_sex = get_total_with_filters(country, None, None, sex_id)
                if total_sex == 0:
                    continue
                if total_sex <= 160:
                    # Appelle la version _TASKS
                    tasks = fetch_all_pages_for_filters_TASKS(country, None, None, sex_id, seen_ids, delay)
                    all_tasks.extend(tasks)
                else:
                    # Appelle la version _TASKS
                    tasks = recursive_age_split_TASKS(country, sex_id, 0, 120, seen_ids, delay, 0)
                    all_tasks.extend(tasks)
    except Exception as e:
        print(f"[Erreur] {country}: {e}")
    return all_tasks

# --- FONCTION RUN ENTIÈREMENT MODIFIÉE ---
def run(max_pages: Optional[int], output_csv: str, delay: float) -> None:
    start_time = time.time()
    start_datetime = datetime.now()
    
    print("\n" + "="*60)
    print(f"🕐 DÉMARRAGE: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⚡ Mode: Parallèle (Max Workers: {MAX_WORKERS})")
    print("="*60)
    
    # Liste pour stocker les tâches : (notice_brute, url_detail, entity_id)
    tasks_to_fetch: List[Tuple[Dict, str, str]] = []
    seen_ids: Set[str] = set()
    
    # --- PHASE 0: COLLECTE GLOBALE DES TÂCHES ---
    phase0_start = time.time()
    print("\n" + "="*60)
    print("🌐 PHASE 0: COLLECTE GLOBALE DES TÂCHES")
    print("="*60)
    
    try:
        data = http_get_json(API_URL, {"page": "1", "resultPerPage": "1"}, headers=HEADERS)
        total_global = int(data.get("total", 0))
        print(f"[Info] Total global: {total_global} notices")
        
        # Calcule le nombre de pages en fonction de RESULTS_PER_PAGE
        num_pages_total = math.ceil(total_global / RESULTS_PER_PAGE)
        # Limite à 50 pages (limite API) ou max_pages si fourni
        num_pages_global = min(50, num_pages_total)
        if max_pages is not None:
             num_pages_global = min(num_pages_global, max_pages)

        print(f"[Info] Collecte sur {num_pages_global} pages...")
        
        for page in range(1, num_pages_global + 1):
            if page > 1:
                time.sleep(delay)
            
            page_start = time.time()
            data = fetch_page(page)
            notices = list(iter_notices(data))
            
            new_tasks_count = 0
            for item in notices:
                eid = str(item.get("entity_id") or item.get("id") or "").strip()
                nurl = ""
                links = item.get("_links")
                if isinstance(links, dict):
                    sl = links.get("self")
                    if isinstance(sl, dict):
                        nurl = str(sl.get("href") or "").strip()
                    elif isinstance(sl, str):
                        nurl = sl.strip()
                
                key = eid
                if not key:
                    notice_id = str(item.get("notice_id") or "").strip()
                    key = notice_id
                if not key:
                    key = nurl
                if not key:
                    name = str(item.get('name', '')).strip()
                    forename = str(item.get('forename', '')).strip()
                    dob = str(item.get('date_of_birth', '')).strip()
                    sex = str(item.get('sex_id', '')).strip()
                    key = f"{name}|{forename}|{dob}|{sex}"
                
                if key in seen_ids:
                    continue
                seen_ids.add(key)
                
                tasks_to_fetch.append((item, nurl, eid))
                new_tasks_count += 1
            
            elapsed = time.time() - phase0_start
            eta = (elapsed / page) * (num_pages_global - page)
            print(f"[Global] Page {page}/{num_pages_global}: +{new_tasks_count} tâches | Total Tâches: {len(tasks_to_fetch)} | ETA: {timedelta(seconds=int(eta))}")
        
        phase0_duration = time.time() - phase0_start
        print(f"\n✅ Phase 0: {len(tasks_to_fetch)} tâches collectées")
        print(f"⏱️  Durée Phase 0: {timedelta(seconds=int(phase0_duration))}")
    except Exception as e:
        print(f"[Erreur] Phase globale: {e}")
        phase0_duration = time.time() - phase0_start

    # --- PHASE 1: COLLECTE DES TÂCHES PAR PAYS ---
    phase1_start = time.time()
    print("\n" + "="*60)
    print("🌍 PHASE 1: COLLECTE PAR PAYS (compléments)")
    print("="*60)
    
    countries = [
        "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR", "AS", "AT", "AU", "AW", "AX", "AZ",
        "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ", "BR", "BS", "BT", "BV", "BW", "BY", "BZ",
        "CA", "CC", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN", "CO", "CR", "CU", "CV", "CW", "CX", "CY", "CZ",
        "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE", "EG", "EH", "ER", "ES", "ET", "FI", "FJ", "FK", "FM", "FO", "FR",
        "GA", "GB", "GD", "GE", "GF", "GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW", "GY",
        "HK", "HM", "HN", "HR", "HT", "HU", "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT",
        "JE", "JM", "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ",
        "LA", "LB", "LC", "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY",
        "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MK", "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ",
        "NA", "NC", "NE", "NF", "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ",
        "OM", "PA", "PE", "PF", "PG", "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY",
        "QA", "RE", "RO", "RS", "RU", "RW", "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS", "ST", "SV", "SX", "SY", "SZ",
        "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO", "TR", "TT", "TV", "TW", "TZ",
        "UA", "UG", "UM", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI", "VN", "VU", "WF", "WS", "YE", "YT", "ZA", "ZM", "ZW"
    ]
    
    print(f"[Info] Pays à traiter: {len(countries)}")
    
    total_countries = len(countries)
    for i, country in enumerate(countries, 1):
        country_name = convert_country_code(country)
        country_start = time.time()
        
        try:
            # Appelle la version _TASKS
            new_tasks = smart_fetch_country_TASKS(country, seen_ids, delay)
            tasks_to_fetch.extend(new_tasks)
            
            elapsed = time.time() - phase1_start
            eta = (elapsed / i) * (total_countries - i)
            
            print(f"[{i}/{total_countries}] {country_name}: +{len(new_tasks)} tâches | Total Tâches: {len(tasks_to_fetch)} | ETA: {timedelta(seconds=int(eta))}")
            
            if max_pages and len(tasks_to_fetch) >= max_pages * RESULTS_PER_PAGE:
                 print(f"[Info] Limite max_pages ({max_pages}) atteinte pendant la Phase 1.")
                 break
        except Exception as e:
            print(f"[Erreur] {country_name}: {e}")
            continue
        
        if i < total_countries:
            time.sleep(delay)
    
    phase1_duration = time.time() - phase1_start
    print(f"\n✅ Phase 1: {len(tasks_to_fetch)} tâches totales collectées")
    print(f"⏱️  Durée Phase 1: {timedelta(seconds=int(phase1_duration))}")

    # --- NOUVELLE PHASE 2: TÉLÉCHARGEMENT PARALLÈLE ---
    phase2_start = time.time()
    print("\n" + "="*60)
    print(f"🚀 PHASE 2: TÉLÉCHARGEMENT PARALLÈLE ({MAX_WORKERS} workers)")
    print("="*60)
    
    all_rows: List[Dict[str, str]] = []

    # Petite fonction pour traiter une seule tâche dans un thread
    def process_task(task_data: Tuple[Dict, str, str]) -> Optional[Dict[str, str]]:
        item, nurl, eid = task_data
        try:
            detail = None
            if nurl:
                detail = fetch_detail(nurl)
            elif eid:
                detail = fetch_detail_by_entity_id(eid)
            
            row = normalize_notice(item, detail)
            return row
        except Exception as e:
            print(f"[Erreur Tâche] {eid}: {e}")
            return None

    count = 0
    total_tasks = len(tasks_to_fetch)
    
    if total_tasks == 0:
        print("[Alerte] Aucune tâche à traiter. Le script va se terminer.")
    else:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Soumettre toutes les tâches au pool de threads
            future_to_task = {executor.submit(process_task, task): task for task in tasks_to_fetch}
            
            for future in as_completed(future_to_task):
                row = future.result()
                if row:
                    all_rows.append(row)
                
                count += 1
                # Afficher le progrès tous les 100 traités
                if count % 100 == 0 or count == total_tasks:
                    elapsed = time.time() - phase2_start
                    eta_seconds = 0
                    if count > 0:
                        eta_seconds = (elapsed / count) * (total_tasks - count)
                    
                    print(f"[Progrès] {count}/{total_tasks} notices traitées | Total: {len(all_rows)} | ETA: {timedelta(seconds=int(eta_seconds))}")

    phase2_duration = time.time() - phase2_start
    print(f"\n✅ Phase 2: {len(all_rows)} notices normalisées")
    print(f"⏱️  Durée Phase 2: {timedelta(seconds=int(phase2_duration))}")


    # --- ÉCRITURE CSV ---
    csv_start = time.time()
    fieldnames = [
        "name", "forename", "date_of_birth", "age", "sex", "place_of_birth", "nationality",
        "height", "weight", "hair_color", "eye_color", "distinguishing_marks", "languages",
        "entity_id", "notice_id", "warrant_country", "url", "infractions"
    ]
    with open(output_csv, "w", encoding="utf-8-sig", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fieldnames)
        wr.writeheader()
        for r in all_rows:
            wr.writerow({k: r.get(k, "") for k in fieldnames})
    csv_duration = time.time() - csv_start
    
    # --- RAPPORT FINAL ---
    total_duration = time.time() - start_time
    end_datetime = datetime.now()
    
    print("\n" + "="*60)
    print("🎉 SCRAPING TERMINÉ")
    print("="*60)
    print(f"📁 Fichier: {output_csv}")
    print(f"📊 Notices: {len(all_rows):,}")
    print(f"📋 Colonnes: {len(fieldnames)}")
    print()
    print("⏱️  TEMPS D'EXÉCUTION:")
    print(f"   - Phase 0 (Collecte): {timedelta(seconds=int(phase0_duration))}")
    print(f"   - Phase 1 (Collecte): {timedelta(seconds=int(phase1_duration))}")
    print(f"   - Phase 2 (Parallèle): {timedelta(seconds=int(phase2_duration))}")
    print(f"   - Écriture CSV: {csv_duration:.1f}s")
    print(f"   - TOTAL: {timedelta(seconds=int(total_duration))}")
    print()
    print(f"🕐 Début: {start_datetime.strftime('%H:%M:%S')}")
    print(f"🕐 Fin: {end_datetime.strftime('%H:%M:%S')}")
    print()
    if total_duration > 0:
        print(f"⚡ Vitesse: {len(all_rows) / (total_duration / 60):.0f} notices/minute")
        print(f"⚡ Vitesse: {len(all_rows) / (total_duration / 3600):.0f} notices/heure")
    print("="*60)

# --- BLOC MAIN ET IF __NAME__ CORRIGÉS ---

def main(argv: List[str]) -> int:
    # L'import doit être À L'INTÉRIEUR de la fonction main
    import argparse
    
    # Doit être déclaré global AVANT d'être utilisé dans 'default'
    global MAX_WORKERS
    
    p = argparse.ArgumentParser("Interpol Red Notices - VERSION PARALLÈLE")
    p.add_argument("--max-pages", type=int, default=None, help="Limiter le nombre de pages (pour la Phase 0)")
    p.add_argument("--delay", type=float, default=DELAY, help="Délai entre appels de collecte (s)")
    p.add_argument("--output", type=str, default="interpol_parallel.csv", help="CSV de sortie")
    # 'default' lit maintenant la variable globale sans erreur
    p.add_argument("--workers", type=int, default=MAX_WORKERS, help="Nombre de workers parallèles")
    args = p.parse_args(argv[1:])

    # Assigne la valeur de l'argument à la variable globale
    MAX_WORKERS = args.workers

    print("🚀 SCRAPER INTERPOL - VERSION PARALLÈLE")
    print("=" * 60)
    print("✅ Collecte de tâches (Phase 0 & 1)")
    print("✅ Téléchargement parallèle des détails (Phase 2)")
    print("✅ 18 colonnes + conversion ISO → noms")
    print("=" * 60)
    print(f"⏱️  Délai collecte: {args.delay}s")
    print(f"⚡ Workers parallèles: {args.workers}")
    print(f"📄 Sortie: {args.output}")
    print("=" * 60)

    run(max_pages=args.max_pages, output_csv=args.output, delay=args.delay)
    
    # 'return' doit être À L'INTÉRIEUR de la fonction main
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))