"""
data.py — Stock data fetching and storage.

Primary source: yfinance (Thai stocks via .BK suffix, SET index via ^SET.BK)
Future: swap in SET Trade Open API as primary, keep yfinance as fallback.
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import yfinance as yf
import pytz

logger = logging.getLogger(__name__)

BANGKOK_TZ = pytz.timezone("Asia/Bangkok")

# /root/.cache/py-yfinance may exist as a file (not dir) in container images,
# causing TzCache creation to fail. Redirect to /tmp which is always writable.
import os as _os
_tz_dir = "/tmp/yfinance-tz"
_os.makedirs(_tz_dir, exist_ok=True)  # pre-create so yfinance's own makedirs doesn't fail
yf.set_tz_cache_location(_tz_dir)

# ─── BigQuery (optional) ──────────────────────────────────────────────────────
_bq_client = None
_bq_project = ""
_bq_dataset = "signalix"
BQ_AVAILABLE = False

_BQ_SCHEMA = [
    ("symbol", "STRING"),
    ("date",   "DATE"),
    ("open",   "FLOAT64"),
    ("high",   "FLOAT64"),
    ("low",    "FLOAT64"),
    ("close",  "FLOAT64"),
    ("volume", "INT64"),
]

# ─── Symbol list ─────────────────────────────────────────────────────────────
# Major SET stocks covering key sectors + all SET indexes.
# Expand this list or replace with a live fetch from SET Trade API.
SET_STOCKS = [
    "2S", "3BBIF", "88TH", "A", "A5", "AAI", "AAV", "ABM", "ACAP", "ACC",
    "ACE", "ACG", "ADB", "ADD", "ADVANC", "ADVICE", "AE", "AEONTS", "AF", "AFC",
    "AGE", "AH", "AHC", "AI", "AIE", "AIMCG", "AIMIRT", "AIRA", "AIT", "AJ",
    "AJA", "AKP", "AKR", "AKS", "ALLA", "ALLY", "ALPHAX", "ALT", "ALUCON", "AMA",
    "AMANAH", "AMARC", "AMARIN", "AMATA", "AMATAR", "AMATAV", "AMC", "AMR", "ANAN", "ANI",
    "AOT", "AP", "APCO", "APCS", "APO", "APP", "APURE", "AQUA", "ARIN", "ARIP",
    "ARROW", "AS", "ASAP", "ASEFA", "ASIA", "ASIAN", "ASIMAR", "ASK", "ASN", "ASP",
    "ASW", "ATLAS", "ATP30", "AU", "AUCT", "AURA", "AWC", "AXTRART", "AYUD",
    "B", "BA", "BAFS", "BAM", "BANPU", "BAREIT", "BAY", "BBGI", "BBIK", "BBL",
    "BC", "BCH", "BCP", "BCPG", "BCT", "BDMS", "BE8", "BEAUTY", "BEC", "BEM",
    "BEYOND", "BGC", "BGRIM", "BGT", "BH", "BIG", "BIOTEC", "BIS", "BIZ", "BJC",
    "BJCHI", "BKA", "BKD", "BKGI", "BKIH", "BLA", "BLAND", "BLC", "BLESS", "BLISS",
    "BM", "BOFFICE", "BOL", "BPP", "BPS", "BR", "BRI", "BROCK", "BRR", "BRRGIF",
    "BSBM", "BSM", "BTC", "BTG", "BTNC", "BTS", "BTSGIF", "BTW", "BUI", "BVG",
    "BWG", "BYD",
    "CAZ", "CBG", "CCET", "CCP", "CEN", "CENTEL", "CEYE", "CFARM", "CFRESH", "CGD",
    "CGH", "CH", "CHAO", "CHARAN", "CHASE", "CHAYO", "CHEWA", "CHG", "CHIC", "CHO",
    "CHOTI", "CHOW", "CI", "CIG", "CIMBT", "CITY", "CIVIL", "CK", "CKP", "CM",
    "CMAN", "CMC", "CMO", "CMR", "CNT", "COCOCO", "COLOR", "COM7", "COMAN", "CPALL",
    "CPANEL", "CPAXT", "CPF", "CPH", "CPI", "CPL", "CPN", "CPNCG", "CPNREIT", "CPR",
    "CPT", "CPTREIT", "CPW", "CRANE", "CRC", "CRD", "CREDIT", "CSC", "CSP", "CSR",
    "CSS", "CTARAF", "CTW", "CV", "CWT",
    "D", "DCC", "DCON", "DDD", "DELTA", "DEMCO", "DEXON", "DHOUSE", "DIF", "DIMET",
    "DITTO", "DMT", "DOD", "DOHOME", "DPAINT", "DREIT", "DRT", "DTCENT", "DTCI", "DUSIT",
    "DV8",
    "EA", "EASON", "EAST", "EASTW", "ECF", "EFORL", "EGATIF", "EGCO", "EKH", "EMC",
    "EMPIRE", "EP", "EPG", "ERW", "ESTAR", "ETC", "ETE", "ETL", "EURO", "EVER",
    "FANCY", "FE", "FLOYD", "FM", "FMT", "FN", "FNS", "FORTH", "FPI", "FPT",
    "FSMART", "FSX", "FTE", "FTI", "FTREIT", "FUTURERT", "FVC",
    "GABLE", "GAHREIT", "GBX", "GC", "GCAP", "GEL", "GENCO", "GFC", "GFPT", "GGC",
    "GJS", "GLAND", "GLOBAL", "GLORY", "GPI", "GPSC", "GRAMMY", "GRAND", "GREEN", "GROREIT",
    "GSTEEL", "GTB", "GTV", "GULF", "GUNKUL", "GVREIT", "GYT",
    "HANA", "HANN", "HARN", "HEALTH", "HENG", "HFT", "HL", "HMPRO", "HPF", "HPT",
    "HTC", "HTECH", "HUMAN", "HYDRO", "HYDROGEN",
    "I2", "ICC", "ICHI", "ICN", "IDG", "IFS", "IHL", "IIG", "III", "ILINK",
    "ILM", "IMH", "IMPACT", "IND", "INET", "INETREIT", "INGRS", "INOX", "INSET", "INSURE",
    "IP", "IRC", "IRCP", "IROYAL", "IRPC", "ISSARA", "IT", "ITC", "ITD", "ITEL",
    "ITNS", "ITTHI", "IVF", "IVL",
    "J", "JAK", "JAS", "JCK", "JCT", "JDF", "JMART", "JMT", "JPARK", "JR",
    "JSP", "JTS", "JUBILE",
    "K", "KAMART", "KASET", "KBANK", "KBS", "KBSPIF", "KC", "KCAR", "KCC", "KCE",
    "KCG", "KCM", "KDH", "KGEN", "KGI", "KIAT", "KISS", "KJL", "KK", "KKC",
    "KKP", "KLINIQ", "KOOL", "KPNREIT", "KSL", "KTB", "KTBSTMR", "KTC", "KTIS", "KTMS",
    "KUMWEL", "KUN", "KWC", "KWI", "KWM", "KYE",
    "LALIN", "LANNA", "LDC", "LEE", "LEO", "LH", "LHFG", "LHHOTEL", "LHK", "LHRREIT",
    "LHSC", "LIT", "LOXLEY", "LPH", "LPN", "LRH", "LST", "LTMH", "LTS", "LUXF",
    "M", "MADAME", "MAGURO", "MAJOR", "MALEE", "MANRIN", "MASTEC", "MASTER", "MATCH", "MATI",
    "MBAX", "MBK", "MC", "MCA", "MCOT", "MCS", "MDX", "MEB", "MEDEZE", "MEGA",
    "MENA", "META", "METCO", "MFC", "MFEC", "MGC", "MGI", "MGT", "MICRO", "MIDA",
    "MII", "MILL", "MINT", "MIPF", "MITSIB", "MJD", "MJLF", "MK", "ML", "MMM",
    "MNIT", "MNIT2", "MNRF", "MODERN", "MONO", "MOONG", "MORE", "MOSHI", "MOTHER", "MPJ",
    "MRDIYT", "MSC", "MST", "MTC", "MTI", "MTW", "MUD", "MVP",
    "NAM", "NAT", "NATION", "NC", "NCAP", "NCH", "NCL", "NCP", "NDR", "NEO",
    "NEP", "NER", "NETBAY", "NEW", "NEX", "NFC", "NKI", "NKT", "NL", "NNCL",
    "NOBLE", "NOVA", "NPK", "NRF", "NSL", "NTF", "NTSC", "NTV", "NUT", "NV",
    "NVD", "NWR", "NYT",
    "OCC", "OGC", "OHTL", "OKJ", "ONEE", "ONSENS", "OR", "ORI", "ORN", "OSP",
    "PACO", "PAF", "PANEL", "PAP", "PATO", "PB", "PCC", "PCE", "PCSGH", "PDG",
    "PDJ", "PEACE", "PEER", "PERM", "PF", "PG", "PHG", "PHOL", "PICO", "PIMO",
    "PIN", "PIS", "PJW", "PK", "PL", "PLANB", "PLANET", "PLAT", "PLE", "PLT",
    "PLUS", "PM", "PMC", "PMTA", "POLY", "POPF", "PORT", "PPM", "PPP", "PPPM",
    "PPS", "PQS", "PR9", "PRAKIT", "PRAPAT", "PREB", "PRECHA", "PRG", "PRI", "PRIME",
    "PRIN", "PRINC", "PRM", "PROEN", "PROS", "PROSPECT", "PROUD", "PRTR", "PSGC", "PSH",
    "PSL", "PSP", "PSTC", "PT", "PTC", "PTECH", "PTG", "PTL", "PTT", "PTTEP",
    "PTTGC", "PYLON",
    "QDC", "QH", "QHBREIT", "QHHRREIT", "QHOP", "QLT", "QTC", "QTCG",
    "RABBIT", "RAM", "RATCH", "RBF", "RCL", "READY", "RICHY", "RJH", "RML", "ROCK",
    "ROCTEC", "ROH", "ROJNA", "RP", "RPC", "RPH", "RS", "RSP", "RT", "RWI",
    "S", "S11", "SA", "SAAM", "SABINA", "SAF", "SAFE", "SAK", "SALEE", "SAM",
    "SAMART", "SAMCO", "SAMTEL", "SANKO", "SAPPE", "SAT", "SAUCE", "SAV", "SAWAD", "SAWANG",
    "SC", "SCAP", "SCB", "SCC", "SCCC", "SCG", "SCGD", "SCGP", "SCI", "SCL",
    "SCM", "SCN", "SCP", "SDC", "SE", "SEAFCO", "SEAOIL", "SECURE", "SEI", "SELIC",
    "SENA", "SENX", "SFLEX", "SFT", "SGC", "SGF", "SGP", "SHANG", "SHR", "SIAM",
    "SICT", "SIMAT", "SINGER", "SINO", "SIRI", "SIRIPRT", "SIS", "SISB", "SITHAI", "SJWD",
    "SK", "SKE", "SKIN", "SKN", "SKR", "SKY", "SLP", "SMART", "SMD100", "SMIT",
    "SMO", "SMPC", "SMT", "SNC", "SNNP", "SNP", "SNPS", "SO", "SOLAR", "SONIC",
    "SORKON", "SPA", "SPACK", "SPALI", "SPC", "SPCG", "SPG", "SPI", "SPRC", "SPREME",
    "SPRIME", "SPTX", "SPVI", "SQ", "SR", "SRICHA", "SRIPANWA", "SRS", "SSF", "SSP",
    "SSPF", "SSSC", "SST", "SSTRT", "STA", "STANLY", "STARM", "STC", "STECH", "STECON",
    "STELLA", "STGT", "STI", "STOWER", "STP", "STPI", "STX", "SUC", "SUN", "SUPER",
    "SUPEREIF", "SUSCO", "SUTHA", "SVI", "SVOA", "SVR", "SVT", "SWC", "SYMC", "SYNEX",
    "SYNTEC",
    "TACC", "TAE", "TAKUNI", "TAN", "TAPAC", "TASCO", "TATG", "TBN", "TC", "TCAP",
    "TCC", "TCJ", "TCMC", "TCOAT", "TEAM", "TEAMG", "TEGH", "TEKA", "TERA", "TFFIF",
    "TFG", "TFI", "TFM", "TFMAMA", "TGE", "TGH", "TGPRO", "TH", "THAI", "THANA",
    "THANI", "THCOM", "THE", "THG", "THIP", "THMUI", "THRE", "THREL", "TIDLOR", "TIF1",
    "TIGER", "TIPCO", "TIPH", "TISCO", "TITLE", "TK", "TKC", "TKN", "TKS", "TKT",
    "TL", "TLHPF", "TLI", "TM", "TMAN", "TMC", "TMD", "TMI", "TMILL", "TMT",
    "TMW", "TNDT", "TNH", "TNITY", "TNL", "TNP", "TNPC", "TNPF", "TNR", "TOA",
    "TOG", "TOP", "TOPP", "TPA", "TPAC", "TPBI", "TPCH", "TPCS", "TPIPL", "TPIPP",
    "TPL", "TPLAS", "TPOLY", "TPP", "TPRIME", "TPS", "TQM", "TQR", "TR", "TRC",
    "TRITN", "TRP", "TRT", "TRU", "TRUBB", "TRUE", "TRV", "TSC", "TSE", "TSI",
    "TSR", "TSTE", "TSTH", "TTA", "TTB", "TTCL", "TTI", "TTLPF", "TTT", "TTW",
    "TU", "TURBO", "TURTLE", "TVDH", "TVH", "TVO", "TVT", "TWP", "TWPC", "TWZ",
    "TYCN",
    "UAC", "UBA", "UBE", "UBIS", "UEC", "UKEM", "UMI", "UMS", "UNIQ", "UOBKH",
    "UP", "UPF", "UPOIC", "UREKA", "UTP", "UV", "UVAN",
    "VARO", "VCOM", "VGI", "VIBHA", "VIH", "VL", "VNG", "VPO", "VRANDA", "VS",
    "WACOAL", "WARRIX", "WASH", "WAVE", "WELL", "WFX", "WGE", "WHA", "WHABT", "WHAIR",
    "WHART", "WHAUP", "WICE", "WIIK", "WIN", "WINDOW", "WINMED", "WINNER", "WORK", "WP",
    "WPH", "WSOL",
    "XBIO", "XO", "XPG", "XYZ",
    "YGG", "YONG", "YUASA",
    "ZAA", "ZEN", "ZIGA",
]

# Alias map — common brand names → actual SET ticker
SYMBOL_ALIASES: dict[str, str] = {
    "SCG": "SCC",          # Siam Cement Group brand → SCC ticker
    "SIAM CEMENT": "SCC",
    "KASIKORN": "KBANK",
    "KASIKORNBANK": "KBANK",
    "KRUNGTHAI": "KTB",
    "BANGKOK BANK": "BBL",
    "SCB": "SCB",
    "KRUNGSRI": "BAY",
    "CENTRAL PATTANA": "CPN",
    "CENTRAL RETAIL": "CRC",
    "THAI UNION": "TU",
    "CHAROEN POKPHAND": "CPF",
    "TRUE CORP": "TRUE",
    "AIS": "ADVANC",
    "PTG": "PTG",
}

SET_INDEXES = ["^SET.BK"]  # SET Index

# Major SET indexes for yfinance (keyed by display name)
INDEX_SYMBOLS: dict[str, str] = {
    "SET":    "^SET.BK",
    "SET50":  "^SET50.BK",
    "SET100": "^SET100.BK",
    "MAI":    "^MAI.BK",
    "sSET":   "^SSET.BK",
    "SETESG": "^SETESG.BK",
}

# ──────────────────────────────────────────────────────────────────────────
# Sub-index member lists (SET50 / SET100 / MAI)
# ──────────────────────────────────────────────────────────────────────────
# Bootstrap (hardcoded) lists for the core SET sub-indexes. SET rebalances
# every 6 months (effective 1 January and 1 July). Keep these as the
# fallback / seed values; the canonical runtime source is Firestore
# `index_members/{INDEX_NAME}.members`, populated by /admin/refresh_index_members
# (manual trigger or monthly Cloud Scheduler job).
#
# Last hand-verified: 2026 H1 composition (best-effort from public SET data).
# When SET publishes a rebalance, either update these constants OR push the
# fresh list to Firestore via the admin endpoint. The bot prefers Firestore
# at runtime when present.

# SET50 — top 50 by market cap + liquidity. Stable mega-caps.
SET50_MEMBERS_FALLBACK: set[str] = {
    "ADVANC", "AOT", "AWC", "BANPU", "BBL", "BCP", "BDMS", "BEM", "BGRIM", "BH",
    "BJC", "BTS", "CBG", "CENTEL", "CK", "COM7", "CPALL", "CPF", "CPN", "CRC",
    "DELTA", "EA", "EGCO", "GLOBAL", "GPSC", "GULF", "HMPRO", "INTUCH", "IVL",
    "KBANK", "KCE", "KTB", "KTC", "LH", "MINT", "MTC", "OR", "OSP", "PTT",
    "PTTEP", "PTTGC", "RATCH", "SCB", "SCC", "SCGP", "TIDLOR", "TOP", "TRUE",
    "TTB", "VGI",
}

# SET100 — top 100 by market cap + liquidity. Includes all SET50 plus 50 more
# mid-caps. The members below are the ADDITIONAL 50 names beyond SET50.
SET100_EXTRA_FALLBACK: set[str] = {
    "AAV", "AMATA", "ASW", "BA", "BAM", "BBIK", "BCH", "BCPG", "BJCHI", "BLA",
    "BPP", "CHG", "CKP", "DOHOME", "ERW", "GFPT", "GUNKUL", "HANA", "ICHI",
    "ITC", "JMART", "JMT", "KKP", "M", "MAJOR", "MEGA", "OSP", "PR9", "PRM",
    "PSL", "PTG", "QH", "RBF", "S", "SABUY", "SAPPE", "SAWAD", "SINGER",
    "SISB", "SJWD", "SNNP", "SPALI", "SPRC", "STA", "STARK", "STEC", "TLI",
    "TOA", "TQM", "VGI", "WHA",
}
SET100_MEMBERS_FALLBACK: set[str] = SET50_MEMBERS_FALLBACK | SET100_EXTRA_FALLBACK

# MAI — ~200 stocks listed on the MAI exchange (separate from SET).
# Hardcoding is impractical without a verified source. Empty fallback;
# Phase B will populate via Firestore from a scraped or official feed.
MAI_MEMBERS_FALLBACK: set[str] = set()

# Public accessor — runtime in-memory dict, populated at startup from
# Firestore (preferred) or fallback constants. main.py reads this via
# get_index_members() to avoid coupling notifier/analyzer to globals.
_index_members: dict[str, set[str]] = {
    "SET50":  set(SET50_MEMBERS_FALLBACK),
    "SET100": set(SET100_MEMBERS_FALLBACK),
    "MAI":    set(MAI_MEMBERS_FALLBACK),
}


def get_index_members(index_name: str) -> set[str]:
    """Return the member set for a sub-index. Empty set if unknown.

    Special case: 'MARGINABLE' returns the symbol set from the Krungsri
    Marginable Securities List (data_static/margin_securities.json plus
    Firestore overlay). Treats the broker's IM50/60/70/80 universe as a
    first-class index so every scoped helper (`<index> stage`, `<index>
    stages`, `<index> pivot`, `<index> ready`, etc.) automatically
    works for the trader's actual universe without bespoke wiring.
    """
    name = index_name.upper()
    if name == "MARGINABLE":
        if not _margin_securities:
            _load_margin_securities()
        return set(_margin_securities.keys())
    return _index_members.get(name, set())


def set_index_members(index_name: str, members: set[str]) -> None:
    """Replace the in-memory member set. Called by the /admin refresh path
    after writing to Firestore so subsequent scans use the new list
    without restart."""
    _index_members[index_name.upper()] = set(members)


def index_member_counts() -> dict[str, int]:
    """{index_name: member_count} — used by the monthly health check."""
    return {k: len(v) for k, v in _index_members.items()}


# TradingView URLs per index
INDEX_TV_URLS: dict[str, str] = {
    "SET":    "https://www.tradingview.com/chart/?symbol=SET%3ASET",
    "SET50":  "https://www.tradingview.com/chart/?symbol=SET%3ASET50",
    "SET100": "https://www.tradingview.com/chart/?symbol=SET%3ASET100",
    "MAI":    "https://www.tradingview.com/chart/?symbol=SET%3AMAI",
    "sSET":   "https://www.tradingview.com/chart/?symbol=SET%3ASSET",
    "SETESG": "https://www.tradingview.com/chart/?symbol=SET%3ASETESG",
}

# Official SET sector codes
SECTORS: list[str] = ["AGRO", "CONSUMP", "FINCIAL", "INDUS", "PROPCON", "RESOURC", "SERVICE", "TECH"]

# Sector mapping: symbol → SET sector code (unmapped stocks → "OTHER")
SECTOR_MAP: dict[str, str] = {
    # AGRO — Agriculture & Food Industry
    "CPF": "AGRO", "GFPT": "AGRO", "TU": "AGRO", "MINT": "AGRO", "BR": "AGRO",
    "MALEE": "AGRO", "SAPPE": "AGRO", "TFG": "AGRO", "KSL": "AGRO", "KTIS": "AGRO",
    "CFRESH": "AGRO", "CHOTI": "AGRO", "KASET": "AGRO", "TFMAMA": "AGRO", "TIPCO": "AGRO",
    "NRF": "AGRO", "STA": "AGRO", "PPM": "AGRO", "SUSCO": "AGRO", "CGD": "AGRO",

    # CONSUMP — Consumer Products
    "CPALL": "CONSUMP", "CRC": "CONSUMP", "DOHOME": "CONSUMP", "HMPRO": "CONSUMP",
    "COM7": "CONSUMP", "BEAUTY": "CONSUMP", "JUBILE": "CONSUMP", "MC": "CONSUMP",
    "MK": "CONSUMP", "S&P": "CONSUMP", "SABINA": "CONSUMP", "TBSP": "CONSUMP",
    "OISHI": "CONSUMP", "OSP": "CONSUMP", "CBG": "CONSUMP", "MONO": "CONSUMP",
    "SINGER": "CONSUMP", "WARRIX": "CONSUMP", "SAUCE": "CONSUMP", "MOONG": "CONSUMP",
    "ZMICO": "CONSUMP", "IVF": "CONSUMP", "SYNEX": "CONSUMP", "SIS": "CONSUMP",

    # FINCIAL — Financials
    "BBL": "FINCIAL", "KBANK": "FINCIAL", "KTB": "FINCIAL", "SCB": "FINCIAL",
    "BAY": "FINCIAL", "KKP": "FINCIAL", "TISCO": "FINCIAL", "LHFG": "FINCIAL",
    "TCAP": "FINCIAL", "TMB": "FINCIAL", "CIMBT": "FINCIAL", "UOBKH": "FINCIAL",
    "MTC": "FINCIAL", "SAWAD": "FINCIAL", "AEONTS": "FINCIAL", "KTC": "FINCIAL",
    "TIDLOR": "FINCIAL", "MFC": "FINCIAL", "PHATRA": "FINCIAL", "ASK": "FINCIAL",
    "CGH": "FINCIAL", "GGC": "FINCIAL", "BFIT": "FINCIAL", "AYUD": "FINCIAL",
    "BLA": "FINCIAL", "TQM": "FINCIAL", "MTI": "FINCIAL", "MITSIB": "FINCIAL",
    "NKI": "FINCIAL", "THRE": "FINCIAL", "THREL": "FINCIAL", "TLI": "FINCIAL",
    "GPI": "FINCIAL", "MII": "FINCIAL", "KGI": "FINCIAL", "ASP": "FINCIAL",
    "JMART": "FINCIAL", "JMT": "FINCIAL", "NCAP": "FINCIAL", "CHAYO": "FINCIAL",
    "BAM": "FINCIAL", "JTS": "FINCIAL",

    # INDUS — Industrials
    "SCC": "INDUS", "SCGD": "INDUS", "SCGP": "INDUS", "PYLON": "INDUS",
    "TTA": "INDUS", "HANA": "INDUS", "KCE": "INDUS", "DELTA": "INDUS",
    "SVI": "INDUS", "BTNC": "INDUS", "SMT": "INDUS", "TRT": "INDUS",
    "STEC": "INDUS", "ITD": "INDUS", "PREB": "INDUS", "SEAFCO": "INDUS",
    "NWR": "INDUS", "SYNTEC": "INDUS", "STECON": "INDUS", "CK": "INDUS",
    "ALUCON": "INDUS", "STANLY": "INDUS", "TKN": "INDUS", "SNC": "INDUS",
    "IRPC": "INDUS", "TASCO": "INDUS", "TRUBB": "INDUS", "BSM": "INDUS",
    "AJ": "INDUS", "TPI": "INDUS", "TPIPL": "INDUS", "DCON": "INDUS",
    "ASIA": "INDUS", "MILL": "INDUS", "GEL": "INDUS",

    # PROPCON — Property & Construction
    "CPN": "PROPCON", "LH": "PROPCON", "AP": "PROPCON", "SPALI": "PROPCON",
    "ORI": "PROPCON", "SIRI": "PROPCON", "PSH": "PROPCON", "QH": "PROPCON",
    "LPN": "PROPCON", "NOBLE": "PROPCON", "MJD": "PROPCON", "SC": "PROPCON",
    "LALIN": "PROPCON", "GRAND": "PROPCON", "ANAN": "PROPCON", "NWG": "PROPCON",
    "CHEWA": "PROPCON", "PF": "PROPCON", "RICHY": "PROPCON", "SENA": "PROPCON",
    "GLAND": "PROPCON", "PROUD": "PROPCON", "DHOUSE": "PROPCON", "SUTHA": "PROPCON",
    "AMATA": "PROPCON", "WHA": "PROPCON", "ROJNA": "PROPCON", "EASTW": "PROPCON",
    "TTW": "PROPCON", "BWG": "PROPCON", "TWPC": "PROPCON",
    "CK": "PROPCON", "CPNREIT": "PROPCON", "WHART": "PROPCON", "FTREIT": "PROPCON",
    "DREIT": "PROPCON", "DIF": "PROPCON", "BTSGIF": "PROPCON", "LHHOTEL": "PROPCON",

    # RESOURC — Resources
    "PTT": "RESOURC", "PTTEP": "RESOURC", "PTTGC": "RESOURC", "BANPU": "RESOURC",
    "RATCH": "RESOURC", "BCPG": "RESOURC", "BCP": "RESOURC", "EGCO": "RESOURC",
    "GPSC": "RESOURC", "GULF": "RESOURC", "GUNKUL": "RESOURC", "EA": "RESOURC",
    "TOP": "RESOURC", "ESSO": "RESOURC", "SPRC": "RESOURC", "PTG": "RESOURC",
    "OR": "RESOURC", "BBGI": "RESOURC", "TPIPP": "RESOURC", "TPCH": "RESOURC",
    "DEMCO": "RESOURC", "SPCG": "RESOURC", "EPG": "RESOURC", "LANNA": "RESOURC",
    "GC": "RESOURC", "PPP": "RESOURC", "ESSO": "RESOURC",

    # SERVICE — Services (healthcare, media, tourism, transport)
    "BDMS": "SERVICE", "BGH": "SERVICE", "BH": "SERVICE", "CHG": "SERVICE",
    "BCH": "SERVICE", "RJH": "SERVICE", "VIH": "SERVICE", "PRINC": "SERVICE",
    "VIBHA": "SERVICE", "EKH": "SERVICE", "NTV": "SERVICE", "LPH": "SERVICE",
    "AOT": "SERVICE", "AAV": "SERVICE", "BA": "SERVICE", "NOK": "SERVICE",
    "BTS": "SERVICE", "BEM": "SERVICE", "BMCL": "SERVICE", "TMILL": "SERVICE",
    "PSL": "SERVICE", "RCL": "SERVICE", "TTA": "SERVICE", "NCL": "SERVICE",
    "MAJOR": "SERVICE", "MCOT": "SERVICE", "BEC": "SERVICE", "WORK": "SERVICE",
    "RS": "SERVICE", "GRAMMY": "SERVICE", "MONO": "SERVICE",
    "DUSIT": "SERVICE", "MINT": "SERVICE", "ERW": "SERVICE", "AWC": "SERVICE",
    "CEN": "SERVICE", "CENTEL": "SERVICE", "ONEE": "SERVICE", "NATION": "SERVICE",
    "WICE": "SERVICE", "LEO": "SERVICE", "GLOBAL": "SERVICE", "MBK": "SERVICE",

    # TECH — Technology & Communication
    "ADVANC": "TECH", "TRUE": "TECH", "INTUCH": "TECH", "JAS": "TECH",
    "ITEL": "TECH", "THCOM": "TECH", "INSET": "TECH", "NETBAY": "TECH",
    "MFEC": "TECH", "SVOA": "TECH", "SIS": "TECH", "COM7": "TECH",
    "BE8": "TECH", "SYMC": "TECH", "ADVICE": "TECH", "INET": "TECH",
    "DIGIO": "TECH", "DITTO": "TECH", "EFORL": "TECH", "AI": "TECH",
    "FORTH": "TECH", "JCT": "TECH", "CSP": "TECH", "DATA": "TECH",
    "VGI": "TECH", "PLANB": "TECH",
}

_STOCK_SET = set(SET_STOCKS)

# ─── SET Subsector taxonomy ───────────────────────────────────────────────────

# SET's 25 official subsectors mapped to their parent industry group
SUBSECTOR_TO_SECTOR: dict[str, str] = {
    # AGRO
    "AGRI":    "AGRO",    # Agricultural products
    "FOOD":    "AGRO",    # Food & Beverage
    # CONSUMP
    "FASHION": "CONSUMP", # Fashion & Apparel
    "HOME":    "CONSUMP", # Household & Office products
    "PERSON":  "CONSUMP", # Personal products & Pharma
    # FINCIAL
    "BANK":    "FINCIAL", # Banking
    "FIN":     "FINCIAL", # Finance & Securities
    "INSUR":   "FINCIAL", # Insurance
    # INDUS
    "AUTO":    "INDUS",   # Automotive
    "IMM":     "INDUS",   # Industrial Materials & Machinery
    "PAPER":   "INDUS",   # Paper & Printing
    "PETRO":   "INDUS",   # Petrochemical & Chemical
    "PKG":     "INDUS",   # Packaging
    "STEEL":   "INDUS",   # Steel & Metal products
    # PROPCON
    "CONMAT":  "PROPCON", # Construction Materials
    "CONS":    "PROPCON", # Construction Services
    "PF":      "PROPCON", # Property Funds & REITs
    "PROP":    "PROPCON", # Property Development
    # RESOURC
    "ENERG":   "RESOURC", # Energy & Utilities
    "MINE":    "RESOURC", # Mining
    # SERVICE
    "COMM":    "SERVICE", # Commerce (retail/wholesale)
    "HELTH":   "SERVICE", # Healthcare Services
    "MEDIA":   "SERVICE", # Media & Publishing
    "PROF":    "SERVICE", # Professional Services
    "TOURISM": "SERVICE", # Tourism & Leisure
    "TRANS":   "SERVICE", # Transportation & Logistics
    # TECH
    "ETRON":   "TECH",    # Electronic Components
    "ICT":     "TECH",    # Information & Communication Technology
}

# Translation from Yahoo Finance sector/industry strings → SET subsector codes
# Used by fetch_sector_map_from_yfinance()
_YF_INDUSTRY_TO_SUBSECTOR: dict[str, str] = {
    # Banking & Finance
    "Banks—Regional": "BANK", "Banks—Diversified": "BANK",
    "Capital Markets": "FIN", "Financial Conglomerates": "FIN",
    "Credit Services": "FIN", "Asset Management": "FIN",
    "Insurance—Life": "INSUR", "Insurance—Property & Casualty": "INSUR",
    "Insurance—Diversified": "INSUR", "Insurance Brokers": "INSUR",
    # Technology
    "Software—Application": "ICT", "Software—Infrastructure": "ICT",
    "Telecom Services": "ICT", "Communication Equipment": "ICT",
    "Internet Content & Information": "ICT", "Electronic Gaming & Multimedia": "ICT",
    "Information Technology Services": "ICT",
    "Electronic Components": "ETRON", "Semiconductors": "ETRON",
    "Electronics & Computer Distribution": "ETRON",
    # Energy & Resources
    "Oil & Gas E&P": "ENERG", "Oil & Gas Integrated": "ENERG",
    "Oil & Gas Midstream": "ENERG", "Oil & Gas Refining & Marketing": "ENERG",
    "Utilities—Regulated Electric": "ENERG", "Utilities—Renewable": "ENERG",
    "Utilities—Independent Power Producers": "ENERG",
    "Utilities—Diversified": "ENERG", "Solar": "ENERG",
    "Coal": "MINE", "Other Industrial Metals & Mining": "MINE",
    "Copper": "MINE", "Gold": "MINE", "Silver": "MINE",
    "Oil & Gas Equipment & Services": "ENERG",
    # Industrials
    "Auto Manufacturers": "AUTO", "Auto Parts": "AUTO",
    "Specialty Chemicals": "PETRO", "Chemicals": "PETRO",
    "Agricultural Chemicals": "PETRO",
    "Packaging & Containers": "PKG",
    "Paper & Paper Products": "PAPER", "Printing Services": "PAPER",
    "Steel": "STEEL", "Aluminum": "STEEL", "Other Precious Metals & Mining": "STEEL",
    "Industrial Materials": "IMM", "Industrial Machinery": "IMM",
    "Specialty Industrial Machinery": "IMM", "Tools & Accessories": "IMM",
    "Rubber & Plastics": "IMM", "Electrical Equipment & Parts": "IMM",
    # Property & Construction
    "Real Estate—Development": "PROP", "Residential Construction": "PROP",
    "Real Estate—Diversified": "PROP",
    "REIT—Retail": "PF", "REIT—Diversified": "PF", "REIT—Industrial": "PF",
    "REIT—Office": "PF", "REIT—Residential": "PF", "REIT—Hotel & Motel": "PF",
    "Building Materials": "CONMAT", "Building Products & Equipment": "CONMAT",
    "Engineering & Construction": "CONS", "Infrastructure Operations": "CONS",
    # Agriculture & Food
    "Farm Products": "AGRI", "Agricultural Inputs": "AGRI",
    "Farm & Heavy Construction Machinery": "AGRI",
    "Packaged Foods": "FOOD", "Food Distribution": "FOOD",
    "Beverages—Non-Alcoholic": "FOOD", "Beverages—Alcoholic": "FOOD",
    "Beverages—Wineries & Distilleries": "FOOD", "Confectioners": "FOOD",
    "Seafood": "FOOD", "Meat Products": "FOOD",
    # Consumer
    "Apparel Manufacturing": "FASHION", "Apparel Retail": "FASHION",
    "Footwear & Accessories": "FASHION", "Luxury Goods": "FASHION",
    "Furnishings, Fixtures & Appliances": "HOME", "Home Improvement Retail": "HOME",
    "Household & Personal Products": "PERSON", "Drug Manufacturers—Specialty & Generic": "PERSON",
    "Medical Devices": "PERSON", "Pharmaceutical Retailers": "PERSON",
    # Services
    "Grocery Stores": "COMM", "Department Stores": "COMM",
    "Specialty Retail": "COMM", "Discount Stores": "COMM",
    "Electronics Retail": "COMM",
    "Healthcare Facilities": "HELTH", "Medical Care Facilities": "HELTH",
    "Diagnostics & Research": "HELTH", "Health Information Services": "HELTH",
    "Broadcasting": "MEDIA", "Advertising Agencies": "MEDIA",
    "Publishing": "MEDIA", "Entertainment": "MEDIA",
    "Staffing & Employment Services": "PROF", "Consulting Services": "PROF",
    "Security & Protection Services": "PROF", "Waste Management": "PROF",
    "Hotels & Motels": "TOURISM", "Resorts & Casinos": "TOURISM",
    "Travel Services": "TOURISM", "Restaurants": "TOURISM",
    "Leisure": "TOURISM",
    "Airlines": "TRANS", "Trucking": "TRANS", "Marine Shipping": "TRANS",
    "Railroads": "TRANS", "Integrated Freight & Logistics": "TRANS",
    "Airport Operations": "TRANS",
}

# SET sector index tickers for yfinance
SECTOR_INDEX_SYMBOLS: dict[str, str] = {
    "AGRO":    "^AGRO.BK",
    "CONSUMP": "^CONSUMP.BK",
    "FINCIAL": "^FINCIAL.BK",
    "INDUS":   "^INDUS.BK",
    "PROPCON": "^PROPCON.BK",
    "RESOURC": "^RESOURC.BK",
    "SERVICE": "^SERVICE.BK",
    "TECH":    "^TECH.BK",
}

# In-memory sector map — loaded from Firestore on startup via yfinance .info mapping
# Maps symbol → subsector code (e.g. "BANK", "FOOD", "ICT"). 28 codes total.
# Use get_sector(symbol) / get_subsector(symbol) helpers below.
_dynamic_sector_map: dict[str, str] = {}  # symbol → subsector


# ─── Marginable Securities List ────────────────────────────────────────────────
# Sourced from Krungsri Securities' periodic Marginable Securities List PDF.
# Stored as a static JSON in data_static/margin_securities.json (committed to
# the repo). To refresh: `python3 scripts/refresh_margin_list.py --pdf <path>`
# then commit + deploy.
#
# Schema: {symbol: {im_pct: int, short_sell: bool}}
#   im_pct  = Initial Margin requirement % (50 / 60 / 70 / 80).
#             Lower = more leverage. IM50 = 2.0× max, IM80 = 1.25× max.
#   short_sell = True if symbol allows short-sell (was '**' in source PDF).
# Symbols ABSENT from this dict are NOT marginable (broker rejects margin
# orders) — so consumers should treat .get(sym) is None as "no margin".
_margin_securities: dict[str, dict] = {}
_margin_metadata: dict = {}


def _load_margin_securities() -> None:
    """Load data_static/margin_securities.json into module-level dict.
    Called once from init_firestore()/init_bq() startup. Silent no-op if
    the file is missing (service still runs without margin data).
    """
    global _margin_securities, _margin_metadata
    try:
        import json as _json
        from pathlib import Path
        path = Path(__file__).parent / "data_static" / "margin_securities.json"
        if not path.exists():
            logger.warning("margin_securities.json not found at %s", path)
            return
        with path.open("r", encoding="utf-8") as f:
            payload = _json.load(f)
        _margin_securities = payload.get("securities", {}) or {}
        _margin_metadata = {k: v for k, v in payload.items() if k != "securities"}
        logger.info("Loaded %d marginable securities (as of %s)",
                    len(_margin_securities), _margin_metadata.get("as_of", "?"))
    except Exception as exc:
        logger.warning("_load_margin_securities failed: %s", exc)


def get_margin_im_pct(symbol: str) -> int:
    """Return IM% for a symbol (50/60/70/80) or 0 if not marginable.
    0 is the explicit 'non-marginable' marker — broker rejects margin
    orders on these symbols, so trades must be 100% cash."""
    if not _margin_securities:
        return 0
    entry = _margin_securities.get(symbol.upper())
    return int(entry.get("im_pct", 0)) if entry else 0


def get_margin_info(symbol: str) -> dict:
    """Full margin entry for a symbol — empty dict when non-marginable.
    Includes both im_pct and short_sell flag."""
    if not _margin_securities:
        return {}
    return dict(_margin_securities.get(symbol.upper(), {}) or {})


def margin_metadata() -> dict:
    """Return the dataset metadata (as_of date, source URL, notes).
    Used by /test endpoints for transparency."""
    return dict(_margin_metadata)


def load_margin_overlay_from_firestore(db) -> bool:
    """Check Firestore for a more-recent margin overlay (written by the
    /admin/refresh_margin_list endpoint or the monthly Cloud Scheduler
    job) and replace the in-memory dict if it's newer than the static
    JSON. Returns True when an overlay was loaded.

    Doc path: `app_state/margin_securities_live`. Same shape as the
    JSON in data_static/.
    """
    global _margin_securities, _margin_metadata
    if db is None:
        return False
    try:
        doc = db.collection("app_state").document("margin_securities_live").get()
        if not doc.exists:
            return False
        payload = doc.to_dict() or {}
        live = payload.get("securities") or {}
        if not live:
            return False
        # Prefer overlay when it's newer than the static JSON's as_of OR
        # when the static JSON is empty (first-time bootstrap).
        live_as_of = payload.get("as_of", "")
        static_as_of = _margin_metadata.get("as_of", "")
        if live_as_of and static_as_of and live_as_of < static_as_of:
            logger.info("Margin overlay older than static (%s < %s) — keeping static",
                        live_as_of, static_as_of)
            return False
        _margin_securities = live
        _margin_metadata = {k: v for k, v in payload.items() if k != "securities"}
        logger.info("Loaded %d margin securities from Firestore overlay (as_of %s)",
                    len(live), live_as_of or "?")
        return True
    except Exception as exc:
        logger.warning("load_margin_overlay_from_firestore failed: %s", exc)
        return False


def refresh_margin_from_url(url: str, db=None, as_of: str = "") -> dict:
    """Fetch the Krungsri Marginable Securities PDF from `url`, parse,
    persist to Firestore overlay (when db provided), and update the
    in-memory dict atomically.

    Returns a stats dict with before/after counts + symbol-level diff
    so callers can log meaningful change reports.
    """
    global _margin_securities, _margin_metadata
    import re as _re
    import urllib.request as _ur
    from datetime import date as _date

    # Parse PDF — same logic as scripts/refresh_margin_list.py kept
    # local here to avoid import shenanigans inside Cloud Run.
    try:
        import pdfplumber as _pdf
    except ImportError:
        return {"error": "pdfplumber not installed in runtime"}

    try:
        # 30s timeout — Krungsri's site is fast but Cloud Run egress
        # can be slow if the connection cold-starts.
        req = _ur.Request(url, headers={"User-Agent": "signalix-margin-refresh/1.0"})
        with _ur.urlopen(req, timeout=30) as resp:
            pdf_bytes = resp.read()
    except Exception as exc:
        return {"error": f"download failed: {exc}"}

    import io
    try:
        with _pdf.open(io.BytesIO(pdf_bytes)) as pdf:
            text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as exc:
        return {"error": f"pdf parse failed: {exc}"}

    tier_pat = _re.compile(r"^IM(\d+)%")
    sym_pat  = _re.compile(r"(?:^|\s)\d+\s+([A-Z][A-Z0-9\-]*)(\*{0,2})")
    KEEP_TIERS = {50, 60, 70, 80}
    current_tier = None
    new_data: dict = {}
    for line in text.split("\n"):
        m = tier_pat.match(line.strip())
        if m:
            current_tier = int(m.group(1))
            continue
        if current_tier is None:
            continue
        for sm in sym_pat.finditer(line):
            sym = sm.group(1)
            ast = len(sm.group(2))
            if sym == "IM" or current_tier not in KEEP_TIERS:
                continue
            new_data[sym] = {"im_pct": current_tier, "short_sell": ast >= 2}

    if not new_data:
        return {"error": "no securities parsed — PDF format may have changed"}

    # Diff vs current in-memory dict for the response
    before_set = set(_margin_securities.keys())
    after_set  = set(new_data.keys())
    added   = sorted(after_set - before_set)
    removed = sorted(before_set - after_set)
    tier_changes = []
    for sym in sorted(after_set & before_set):
        old_tier = _margin_securities[sym].get("im_pct")
        new_tier = new_data[sym].get("im_pct")
        if old_tier != new_tier:
            tier_changes.append({"symbol": sym, "from": old_tier, "to": new_tier})

    # Build the persisted payload
    payload = {
        "as_of": as_of or str(_date.today()),
        "source": f"Krungsri Securities — {url}",
        "source_url": url,
        "notes": (
            "Refreshed via /admin/refresh_margin_list. "
            "im_pct = Initial Margin %. Lower = more leverage."
        ),
        "securities": new_data,
    }

    # Persist overlay to Firestore (when available) so other Cloud Run
    # instances pick it up on next cold-start without redeploy.
    if db is not None:
        try:
            db.collection("app_state").document("margin_securities_live").set(payload)
        except Exception as exc:
            logger.warning("Could not persist margin overlay to Firestore: %s", exc)

    # Update in-memory atomically
    _margin_securities = new_data
    _margin_metadata = {k: v for k, v in payload.items() if k != "securities"}

    return {
        "as_of": payload["as_of"],
        "source_url": url,
        "before_count": len(before_set),
        "after_count": len(after_set),
        "added": added,
        "removed": removed,
        "tier_changes": tier_changes,
        "tier_breakdown": {
            t: sum(1 for v in new_data.values() if v["im_pct"] == t)
            for t in (50, 60, 70, 80)
        },
    }


# Hand-curated overrides for symbols that yfinance .info doesn't classify
# (Thai REITs / infrastructure funds / property-fund tickers mostly) or
# where the industry string falls outside our _YF_INDUSTRY_TO_SUBSECTOR map.
# These are treated as authoritative: get_subsector checks here first.
# Settrade's Investor SDK doesn't expose sector / industry (confirmed via
# /test/settrade_sectors — only get_candlestick / get_quote_symbol /
# market_url are public) so this is the only reliable path for known
# blue chips + funds.
_MANUAL_SUBSECTOR_OVERRIDES: dict[str, str] = {
    # ── BANK ────────────────────────────────────────────────────────
    "BBL": "BANK", "KBANK": "BANK", "KTB": "BANK", "SCB": "BANK",
    "BAY": "BANK", "KKP": "BANK", "TISCO": "BANK", "LHFG": "BANK",
    "TCAP": "BANK", "TMB": "BANK", "TTB": "BANK", "CIMBT": "BANK",
    "UOBKH": "BANK",

    # ── FIN (Finance & Securities) ─────────────────────────────────
    "MTC": "FIN", "SAWAD": "FIN", "AEONTS": "FIN", "KTC": "FIN",
    "TIDLOR": "FIN", "MFC": "FIN", "PHATRA": "FIN", "ASK": "FIN",
    "CGH": "FIN", "BFIT": "FIN", "GPI": "FIN", "KGI": "FIN",
    "ASP": "FIN", "JMART": "FIN", "JMT": "FIN", "NCAP": "FIN",
    "CHAYO": "FIN", "BAM": "FIN",

    # ── INSUR ───────────────────────────────────────────────────────
    "AYUD": "INSUR", "BLA": "INSUR", "TQM": "INSUR", "MTI": "INSUR",
    "MITSIB": "INSUR", "NKI": "INSUR", "THRE": "INSUR",
    "THREL": "INSUR", "TLI": "INSUR",

    # ── PROP (Property Development) ────────────────────────────────
    "CPN": "PROP", "LH": "PROP", "AP": "PROP", "SPALI": "PROP",
    "ORI": "PROP", "SIRI": "PROP", "PSH": "PROP", "QH": "PROP",
    "LPN": "PROP", "NOBLE": "PROP", "MJD": "PROP", "SC": "PROP",
    "LALIN": "PROP", "ANAN": "PROP", "NWG": "PROP", "CHEWA": "PROP",
    "RICHY": "PROP", "SENA": "PROP", "GLAND": "PROP", "PROUD": "PROP",
    "AMATA": "PROP", "WHA": "PROP", "ROJNA": "PROP", "MBK": "PROP",
    "DHOUSE": "PROP", "SUTHA": "PROP",

    # ── PF (Property Funds / REITs / Infra Trusts) ─────────────────
    "CPNREIT": "PF", "WHART": "PF", "FTREIT": "PF", "DREIT": "PF",
    "DIF": "PF", "BTSGIF": "PF", "LHHOTEL": "PF", "3BBIF": "PF",
    "AIMCG": "PF", "AIMIRT": "PF", "AMATAR": "PF", "AMATAV": "PF",
    "LHSC": "PF", "CPTGF": "PF", "FUTUREPF": "PF", "GVREIT": "PF",
    "TLGF": "PF", "URBNPF": "PF", "HREIT": "PF", "IMPACT": "PF",
    "JASIF": "PF", "TFUND": "PF", "SPF": "PF", "TPRIME": "PF",
    "WHABT": "PF", "B-WORK": "PF", "SIRIP": "PF", "M-STOR": "PF",
    "CTARAF": "PF", "DIGI": "PF", "LUXF": "PF", "POPF": "PF",
    "SHREIT": "PF", "SRIPANWA": "PF", "LHPF": "PF", "CPTREIT": "PF",
    "QHHRREIT": "PF", "QHOP": "PF", "QHPF": "PF", "BKKCP": "PF",
    "GAHREIT": "PF", "INETREIT": "PF", "KTBSTMR": "PF", "SPRIME": "PF",

    # ── CONS (Construction Services) ───────────────────────────────
    "STEC": "CONS", "ITD": "CONS", "PREB": "CONS", "SEAFCO": "CONS",
    "NWR": "CONS", "SYNTEC": "CONS", "STECON": "CONS", "CK": "CONS",
    "DCON": "CONS", "UNIQ": "CONS",

    # ── CONMAT (Construction Materials) ────────────────────────────
    "SCC": "CONMAT", "SCGD": "CONMAT", "SCGP": "CONMAT",
    "TPIPL": "CONMAT", "TPI": "CONMAT", "DRT": "CONMAT",
    "VNG": "CONMAT", "DCC": "CONMAT",

    # ── ENERG (Energy & Utilities) ─────────────────────────────────
    "PTT": "ENERG", "PTTEP": "ENERG", "BANPU": "ENERG",
    "RATCH": "ENERG", "BCPG": "ENERG", "BCP": "ENERG",
    "EGCO": "ENERG", "GPSC": "ENERG", "GULF": "ENERG",
    "GUNKUL": "ENERG", "EA": "ENERG", "TOP": "ENERG",
    "ESSO": "ENERG", "SPRC": "ENERG", "PTG": "ENERG",
    "OR": "ENERG", "BBGI": "ENERG", "TPIPP": "ENERG",
    "TPCH": "ENERG", "DEMCO": "ENERG", "SPCG": "ENERG",
    "EPG": "ENERG", "EASTW": "ENERG", "TTW": "ENERG",

    # ── MINE ────────────────────────────────────────────────────────
    "LANNA": "MINE",

    # ── PETRO ──────────────────────────────────────────────────────
    "PTTGC": "PETRO", "IRPC": "PETRO", "TASCO": "PETRO",
    "GC": "PETRO", "GGC": "PETRO",

    # ── ICT (Information & Communication Tech) ─────────────────────
    "ADVANC": "ICT", "TRUE": "ICT", "INTUCH": "ICT", "JAS": "ICT",
    "ITEL": "ICT", "THCOM": "ICT", "INSET": "ICT", "NETBAY": "ICT",
    "INET": "ICT", "DTAC": "ICT",

    # ── ETRON (Electronics) ────────────────────────────────────────
    "HANA": "ETRON", "KCE": "ETRON", "DELTA": "ETRON",
    "SVI": "ETRON", "SMT": "ETRON", "STARK": "ETRON",

    # ── COMM (Commerce Retail/Wholesale) ───────────────────────────
    "CPALL": "COMM", "CRC": "COMM", "DOHOME": "COMM", "HMPRO": "COMM",
    "COM7": "COMM", "GLOBAL": "COMM", "MAKRO": "COMM", "SPVI": "COMM",
    "SYNEX": "COMM", "SIS": "COMM", "ADVICE": "COMM",

    # ── HELTH (Healthcare) ─────────────────────────────────────────
    "BDMS": "HELTH", "BH": "HELTH", "CHG": "HELTH", "BCH": "HELTH",
    "RJH": "HELTH", "VIH": "HELTH", "PRINC": "HELTH", "VIBHA": "HELTH",
    "EKH": "HELTH", "NTV": "HELTH", "LPH": "HELTH", "THG": "HELTH",
    "M-CHAI": "HELTH", "KDH": "HELTH",

    # ── TRANS (Transportation & Logistics) ─────────────────────────
    "AOT": "TRANS", "AAV": "TRANS", "BA": "TRANS", "NOK": "TRANS",
    "BTS": "TRANS", "BEM": "TRANS", "BMCL": "TRANS", "TMILL": "TRANS",
    "PSL": "TRANS", "RCL": "TRANS", "NCL": "TRANS", "WICE": "TRANS",
    "LEO": "TRANS", "TTA": "TRANS",

    # ── MEDIA ──────────────────────────────────────────────────────
    "MAJOR": "MEDIA", "MCOT": "MEDIA", "BEC": "MEDIA",
    "WORK": "MEDIA", "RS": "MEDIA", "GRAMMY": "MEDIA",
    "MONO": "MEDIA", "ONEE": "MEDIA", "NATION": "MEDIA",
    "VGI": "MEDIA", "PLANB": "MEDIA",

    # ── TOURISM ────────────────────────────────────────────────────
    "MINT": "TOURISM", "DUSIT": "TOURISM", "ERW": "TOURISM",
    "AWC": "TOURISM", "CENTEL": "TOURISM",

    # ── FOOD (incl. beverages) ─────────────────────────────────────
    "CPF": "FOOD", "GFPT": "FOOD", "TU": "FOOD", "BR": "FOOD",
    "MALEE": "FOOD", "SAPPE": "FOOD", "TFG": "FOOD",
    "CFRESH": "FOOD", "CHOTI": "FOOD", "TFMAMA": "FOOD",
    "TIPCO": "FOOD", "NRF": "FOOD", "OISHI": "FOOD",
    "OSP": "FOOD", "CBG": "FOOD", "ICHI": "FOOD",
    "M": "FOOD", "MK": "FOOD", "S&P": "FOOD",

    # ── AGRI (Agricultural products) ───────────────────────────────
    "KSL": "AGRI", "KTIS": "AGRI", "KASET": "AGRI",
    "STA": "AGRI", "PPM": "AGRI", "SUSCO": "AGRI", "CGD": "AGRI",

    # ── AUTO ───────────────────────────────────────────────────────
    "STANLY": "AUTO", "SAT": "AUTO", "IHL": "AUTO",
    "PTECH": "AUTO", "TKN": "AUTO", "SNC": "AUTO",

    # ── FASHION / HOME / PERSON ────────────────────────────────────
    "SABINA": "FASHION", "BEAUTY": "FASHION",
    "JUBILE": "FASHION", "MC": "FASHION", "WARRIX": "FASHION",
}


def get_sector(symbol: str) -> str:
    """Return SET industry group (8 codes) for a symbol. Falls back to SECTOR_MAP then OTHER."""
    subsector = get_subsector(symbol)
    if subsector:
        return SUBSECTOR_TO_SECTOR.get(subsector, "OTHER")
    return SECTOR_MAP.get(symbol, "OTHER")


def get_subsector(symbol: str) -> str:
    """Return SET subsector code (28 codes) for a symbol, empty string if unknown.

    Lookup order: manual override (hand-curated, authoritative) →
    _dynamic_sector_map (yfinance .info derived). Manual wins so a wrong
    yfinance classification never hides our curated choice.
    """
    override = _MANUAL_SUBSECTOR_OVERRIDES.get(symbol)
    if override:
        return override
    return _dynamic_sector_map.get(symbol, "")


def fetch_sector_map_from_yfinance(symbols: list[str], batch_size: int = 50) -> dict[str, str]:
    """Fetch sector/subsector for all symbols using yfinance .info.
    Returns dict[symbol → subsector_code]. Slow (1–2 min for 900 stocks) — run once,
    cache result in Firestore.
    """
    result: dict[str, str] = {}
    total = len(symbols)
    for i in range(0, total, batch_size):
        batch = symbols[i:i + batch_size]
        tickers_str = " ".join(f"{s}.BK" for s in batch)
        try:
            import yfinance as yf
            data = yf.Tickers(tickers_str)
            for sym in batch:
                try:
                    info = data.tickers.get(f"{sym}.BK", None)
                    if info is None:
                        continue
                    industry = (info.info or {}).get("industry", "")
                    subsector = _YF_INDUSTRY_TO_SUBSECTOR.get(industry, "")
                    if subsector:
                        result[sym] = subsector
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("fetch_sector_map_from_yfinance batch %d failed: %s", i, exc)
        logger.info("Sector map fetch: %d/%d symbols processed (%d mapped)",
                    min(i + batch_size, total), total, len(result))
    return result


def save_sector_map_to_firestore(sector_map: dict[str, str], db) -> None:
    """Cache subsector map in Firestore sector_map/latest."""
    if not sector_map or db is None:
        return
    try:
        db.collection("sector_map").document("latest").set({
            "stocks": sector_map,   # {symbol: subsector_code}
            "updated_at": datetime.now(BANGKOK_TZ).isoformat(),
            "total": len(sector_map),
        })
        logger.info("Saved sector map: %d symbols to Firestore", len(sector_map))
    except Exception as exc:
        logger.error("save_sector_map_to_firestore failed: %s", exc)


def load_sector_map_from_firestore(db) -> dict[str, str]:
    """Load cached subsector map from Firestore. Returns {} if missing."""
    if db is None:
        return {}
    try:
        doc = db.collection("sector_map").document("latest").get()
        if doc.exists:
            data = doc.to_dict() or {}
            stocks = data.get("stocks", {})
            logger.info("Loaded sector map from Firestore: %d symbols", len(stocks))
            return stocks
    except Exception as exc:
        logger.error("load_sector_map_from_firestore failed: %s", exc)
    return {}


def fetch_sector_index_prices() -> dict[str, dict]:
    """Fetch current prices for SET industry group indexes via yfinance.
    Returns {sector_code: {close, change_pct, scanned_at}} for indexes that exist.

    Per-ticker fetch — same reasoning as fetch_indexes_with_history: the
    bulk yf.download(..., group_by='ticker') silently dropped 5/6 Thai
    indexes in prod, leaving sector cards with no price data at all
    (live_sector_indexes was {}). yf.Ticker().history() per ticker is
    reliable; parallelised via ThreadPoolExecutor (4 workers) so 8 calls
    complete in ~2-3s instead of ~8-10s sequential.
    """
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor

    now = datetime.now(BANGKOK_TZ).isoformat()

    def _fetch_one(sector: str, ticker: str):
        try:
            df = yf.Ticker(ticker).history(period="5d", interval="1d", auto_adjust=False)
            if df is None or df.empty:
                logger.warning("fetch_sector_index_prices: %s (%s) returned empty", sector, ticker)
                return None
            df = df.dropna(subset=["Close"])
            if df.empty:
                return None
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2]) if len(df) >= 2 else close
            chg_pct = round((close - prev) / prev * 100, 2) if prev else 0.0
            return {"close": close, "change_pct": chg_pct, "scanned_at": now}
        except Exception as exc:
            logger.warning("fetch_sector_index_prices(%s / %s) failed: %s", sector, ticker, exc)
            return None

    result: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {
            ex.submit(_fetch_one, sector, ticker): sector
            for sector, ticker in SECTOR_INDEX_SYMBOLS.items()
        }
        for fut in futures:
            data = fut.result()
            if data is not None:
                result[futures[fut]] = data
    logger.info("fetch_sector_index_prices: %d/%d indexes fetched (%s)",
                len(result), len(SECTOR_INDEX_SYMBOLS), list(result.keys()))
    return result


# ──────────────────────────────────────────────────────────────────────────
# Global watchlist (non-SET). Separate namespace from SET tickers so users
# can tap "SPX" / "BTC" / "GOOG" from the `global` command and get the same
# single-asset card flow. yfinance is the one source we use for everything
# here — Settrade doesn't cover non-Thai assets and the curated list is
# small enough (~28 symbols) that per-ticker fetches are cheap.
# ──────────────────────────────────────────────────────────────────────────

# Each entry has an optional "section" — a stable display group used by
# build_global_snapshot_card to render the bulk view in tabs/headers.
# Six top-level groups, organised by what Thai SET-stock-focused traders
# actually watch in TradingView side-panels.
#
# Class vs Section: 'class' is the asset taxonomy (index/etf/stock/crypto/
# fx/commodity) used for icons + per-class scoring rules. 'section' is the
# UI grouping (e.g. US Indexes vs Asia Indexes are both class=index but
# different sections).
GLOBAL_SYMBOLS: dict[str, dict] = {
    # ── 🇺🇸 US Major Indexes ────────────────────────────────────────
    "SPX":     {"yf": "^GSPC",      "name": "S&P 500",                       "class": "index",     "section": "us_indexes"},
    "NDX":     {"yf": "^NDX",       "name": "Nasdaq 100",                    "class": "index",     "section": "us_indexes"},
    "DJI":     {"yf": "^DJI",       "name": "Dow Jones",                     "class": "index",     "section": "us_indexes"},
    "RUT":     {"yf": "^RUT",       "name": "Russell 2000 (small caps)",     "class": "index",     "section": "us_indexes"},
    "VIX":     {"yf": "^VIX",       "name": "Volatility Index (fear gauge)", "class": "index",     "section": "us_indexes"},

    # ── 🌏 Asia Pacific Indexes ─────────────────────────────────────
    "KOSPI":   {"yf": "^KS11",      "name": "KOSPI (Korea)",                 "class": "index",     "section": "asia_indexes"},
    "NI225":   {"yf": "^N225",      "name": "Nikkei 225 (Japan)",            "class": "index",     "section": "asia_indexes"},
    "HSI":     {"yf": "^HSI",       "name": "Hang Seng (Hong Kong)",         "class": "index",     "section": "asia_indexes"},
    "SSE":     {"yf": "000001.SS",  "name": "SSE Composite (Shanghai)",      "class": "index",     "section": "asia_indexes"},
    "TWII":    {"yf": "^TWII",      "name": "Taiwan Weighted (TWSE)",        "class": "index",     "section": "asia_indexes"},
    "STI":     {"yf": "^STI",       "name": "Straits Times (Singapore)",     "class": "index",     "section": "asia_indexes"},
    "JKSE":    {"yf": "^JKSE",      "name": "Jakarta Composite (Indonesia)", "class": "index",     "section": "asia_indexes"},
    "NIFTY":   {"yf": "^NSEI",      "name": "NIFTY 50 (India)",              "class": "index",     "section": "asia_indexes"},
    # VNINDEX intentionally omitted — yfinance has no reliable coverage for
    # the Vietnamese market (^VNI / ^VNINDEX / VNINDEX.VN / VN30.VN all
    # return 0 rows / 404). Restore in Phase 3 via dedicated SEA feed.

    # ── 💱 FX & Macro (Thailand-relevant currencies + dollar index) ──
    "USDTHB":  {"yf": "THB=X",      "name": "USD / THB",                     "class": "fx",        "section": "fx_macro"},
    "DXY":     {"yf": "DX-Y.NYB",   "name": "US Dollar Index",               "class": "fx",        "section": "fx_macro"},
    "USDJPY":  {"yf": "JPY=X",      "name": "USD / JPY",                     "class": "fx",        "section": "fx_macro"},
    "USDCNY":  {"yf": "CNY=X",      "name": "USD / CNY",                     "class": "fx",        "section": "fx_macro"},

    # ── 🛢 Commodities (futures — drives PTT/PTTEP/IRPC/SET energy) ──
    "GOLD":    {"yf": "GC=F",       "name": "Gold (futures)",                "class": "commodity", "section": "commodities"},
    "OIL":     {"yf": "CL=F",       "name": "WTI Crude Oil (futures)",       "class": "commodity", "section": "commodities"},
    "COPPER":  {"yf": "HG=F",       "name": "Copper (futures)",              "class": "commodity", "section": "commodities"},
    "NATGAS":  {"yf": "NG=F",       "name": "Natural Gas (futures)",         "class": "commodity", "section": "commodities"},

    # ── 📈 ETFs (broad market + thematic + regional) ─────────────────
    "QQQ":     {"yf": "QQQ",        "name": "Invesco QQQ (Nasdaq 100)",      "class": "etf",       "section": "etfs"},
    "SPY":     {"yf": "SPY",        "name": "SPDR S&P 500 ETF",              "class": "etf",       "section": "etfs"},
    "VOO":     {"yf": "VOO",        "name": "Vanguard S&P 500 ETF",          "class": "etf",       "section": "etfs"},
    "SMH":     {"yf": "SMH",        "name": "VanEck Semiconductor ETF",      "class": "etf",       "section": "etfs"},
    "ARKW":    {"yf": "ARKW",       "name": "ARK Next Gen Internet ETF",     "class": "etf",       "section": "etfs"},
    "GLD":     {"yf": "GLD",        "name": "SPDR Gold Shares",              "class": "etf",       "section": "etfs"},
    "FXI":     {"yf": "FXI",        "name": "iShares China Large-Cap",       "class": "etf",       "section": "etfs"},
    "EWY":     {"yf": "EWY",        "name": "iShares MSCI South Korea",      "class": "etf",       "section": "etfs"},
    "INDA":    {"yf": "INDA",       "name": "iShares MSCI India",            "class": "etf",       "section": "etfs"},

    # ── 🏢 US Mega-cap stocks ────────────────────────────────────────
    "AAPL":    {"yf": "AAPL",       "name": "Apple",                         "class": "stock",     "section": "us_megacap"},
    "MSFT":    {"yf": "MSFT",       "name": "Microsoft",                     "class": "stock",     "section": "us_megacap"},
    "NVDA":    {"yf": "NVDA",       "name": "NVIDIA",                        "class": "stock",     "section": "us_megacap"},
    "GOOG":    {"yf": "GOOG",       "name": "Alphabet",                      "class": "stock",     "section": "us_megacap"},
    "META":    {"yf": "META",       "name": "Meta Platforms",                "class": "stock",     "section": "us_megacap"},
    "TSLA":    {"yf": "TSLA",       "name": "Tesla",                         "class": "stock",     "section": "us_megacap"},
    "AMZN":    {"yf": "AMZN",       "name": "Amazon",                        "class": "stock",     "section": "us_megacap"},
    "BRK-B":   {"yf": "BRK-B",      "name": "Berkshire Hathaway B",          "class": "stock",     "section": "us_megacap"},

    # ── 🏢 Theme stocks (semis · banks · payments — Delta/PTT proxies) ──
    "TSM":     {"yf": "TSM",        "name": "Taiwan Semi (Delta proxy)",     "class": "stock",     "section": "us_themes"},
    "AMD":     {"yf": "AMD",        "name": "AMD (semis)",                   "class": "stock",     "section": "us_themes"},
    "AVGO":    {"yf": "AVGO",       "name": "Broadcom (semis)",              "class": "stock",     "section": "us_themes"},
    "JPM":     {"yf": "JPM",        "name": "JPMorgan (banks bellwether)",   "class": "stock",     "section": "us_themes"},
    "V":       {"yf": "V",          "name": "Visa (payments)",               "class": "stock",     "section": "us_themes"},
    "GEV":     {"yf": "GEV",        "name": "GE Vernova (energy/AI)",        "class": "stock",     "section": "us_themes"},
    "NFLX":    {"yf": "NFLX",       "name": "Netflix",                       "class": "stock",     "section": "us_themes"},

    # ── ₿ Crypto (USD pricing — THB pairs deferred to Phase 3 / Bitkub) ──
    "BTC":     {"yf": "BTC-USD",    "name": "Bitcoin",                       "class": "crypto",    "section": "crypto"},
    "ETH":     {"yf": "ETH-USD",    "name": "Ethereum",                      "class": "crypto",    "section": "crypto"},
    "SOL":     {"yf": "SOL-USD",    "name": "Solana",                        "class": "crypto",    "section": "crypto"},
    "BNB":     {"yf": "BNB-USD",    "name": "BNB (Binance)",                 "class": "crypto",    "section": "crypto"},
    "XRP":     {"yf": "XRP-USD",    "name": "XRP (Ripple)",                  "class": "crypto",    "section": "crypto"},
}

# Section display order for the bulk 'global' card. Matches the dict insertion
# order above, but keeping it explicit makes the UI rendering deterministic
# and decouples display from dict ordering.
GLOBAL_SECTION_ORDER: list[tuple[str, str]] = [
    ("us_indexes",   "🇺🇸 US Indexes"),
    ("asia_indexes", "🌏 Asia Pacific Indexes"),
    ("fx_macro",     "💱 FX & Macro"),
    ("commodities",  "🛢 Commodities"),
    ("etfs",         "📈 ETFs"),
    ("us_megacap",   "🏢 US Mega-cap"),
    ("us_themes",    "🏢 Theme Stocks"),
    ("crypto",       "₿ Crypto"),
]


def is_global_code(text: str) -> bool:
    """True if text (case-insensitive, trimmed) matches a GLOBAL_SYMBOLS code."""
    if not text:
        return False
    return text.strip().upper() in GLOBAL_SYMBOLS


def fetch_global_asset(code: str) -> Optional[dict]:
    """Richer single-asset fetch for the tap-to-detail card.

    Returns price + day/52W ranges + volume PLUS Minervini stage and
    pattern detection (stage 1-4, pattern breakout/vcp/etc., SMAs,
    stage_weakening flag). Same analysis the SET-stock card runs, applied
    uniformly to every global asset class. The is_index flag passed to
    detect_pattern depends on class — price-only patterns for asset
    classes with aggregate/nil volume (indexes, ETFs, FX, commodities),
    full volume-confirmed patterns for stocks and crypto.

    Returns None if the code isn't known or yfinance returns no data.
    """
    import yfinance as yf
    from analyzer import (classify_stage, detect_pattern, _sma)
    import numpy as np

    code = (code or "").strip().upper()
    meta = GLOBAL_SYMBOLS.get(code)
    if not meta:
        return None
    try:
        # 1y history gives us 52W range + ~250 bars (enough for SMA200 +
        # 20-bar SMA200-rising lookback that classify_stage needs).
        df = yf.Ticker(meta["yf"]).history(period="1y", auto_adjust=False)
        if df is None or df.empty:
            logger.warning("fetch_global_asset(%s): empty dataframe", code)
            return None
        df = df.dropna(subset=["Close"])
        if df.empty:
            return None

        last_row = df.iloc[-1]
        close = float(last_row["Close"])
        prev = float(df["Close"].iloc[-2]) if len(df) >= 2 else close
        chg = round((close - prev) / prev * 100, 2) if prev else 0.0

        week52_high = float(df["High"].max()) if "High" in df else close
        week52_low = float(df["Low"].min()) if "Low" in df else close

        day_high = float(last_row.get("High", close))
        day_low = float(last_row.get("Low", close))

        vol = float(last_row.get("Volume", 0) or 0)

        # ── Stage + pattern analysis ─────────────────────────────────
        # Price-only patterns for asset classes where volume is
        # aggregate / nil; volume-confirmed patterns for stocks + crypto.
        asset_class = meta["class"]
        price_only = asset_class in ("index", "etf", "fx", "commodity")

        stage: Optional[int] = None
        pattern: Optional[str] = None
        breakout_details: dict = {}
        sma50 = sma150 = sma200 = float("nan")
        stage_weakening = False
        if len(df) >= 60:  # detect_pattern's minimum bar requirement
            try:
                stage = classify_stage(df) if len(df) >= 200 else None
                if stage is not None:
                    pattern, breakout_details = detect_pattern(
                        df, stage, is_index=price_only,
                    )
                if len(df) >= 50:
                    sma50 = float(_sma(df["Close"], 50).iloc[-1])
                if len(df) >= 150:
                    sma150 = float(_sma(df["Close"], 150).iloc[-1])
                if len(df) >= 200:
                    sma200 = float(_sma(df["Close"], 200).iloc[-1])
                if (stage == 2 and not np.isnan(sma50) and close < sma50):
                    stage_weakening = True
            except Exception as exc:
                logger.warning("fetch_global_asset(%s): stage/pattern failed: %s", code, exc)

        return {
            "code": code,
            "yf": meta["yf"],
            "name": meta["name"],
            "class": asset_class,
            "close": close,
            "change_pct": chg,
            "day_high": day_high,
            "day_low": day_low,
            "week52_high": week52_high,
            "week52_low": week52_low,
            "volume": vol,
            # Stage + pattern fields — same vocabulary as StockSignal so
            # the card can render with the existing PATTERN_LABEL /
            # STAGE_LABEL maps. None when history is too short.
            "stage": stage,
            "pattern": pattern,
            "breakout_details": breakout_details,
            "sma50": (round(sma50, 4) if not np.isnan(sma50) else 0.0),
            "sma150": (round(sma150, 4) if not np.isnan(sma150) else 0.0),
            "sma200": (round(sma200, 4) if not np.isnan(sma200) else 0.0),
            "stage_weakening": stage_weakening,
            "scanned_at": datetime.now(BANGKOK_TZ).isoformat(),
        }
    except Exception as exc:
        logger.warning("fetch_global_asset(%s / %s) failed: %s", code, meta["yf"], exc)
        return None


def fetch_global_snapshot() -> dict[str, dict]:
    """Fetch latest price + change% for every asset in GLOBAL_SYMBOLS.

    Two-phase fetch to work around a known yfinance 1.3.x threading race:
    concurrent yf.Ticker().history() calls collide on yfinance's internal
    sqlite cache and raise "database is locked". We do a parallel pass
    first (fast, ~3-5 s), then retry any failures sequentially (reliable,
    adds ~0.5 s per retry). Observed locally: ~1-2 of 25 tickers need the
    retry under typical thread contention.

    Returns:
        {code: {name, class, close, change_pct, scanned_at}} for symbols that
        returned data. Failures surviving the sequential retry are logged at
        WARN and dropped (card just won't show them).
    """
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor

    result: dict[str, dict] = {}
    now = datetime.now(BANGKOK_TZ).isoformat()

    def _fetch_one(code: str, meta: dict):
        try:
            df = yf.Ticker(meta["yf"]).history(period="5d", auto_adjust=False)
            if df is None or df.empty:
                return None
            df = df.dropna(subset=["Close"])
            if df.empty:
                return None
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2]) if len(df) >= 2 else close
            chg = round((close - prev) / prev * 100, 2) if prev else 0.0
            return {
                "name": meta["name"],
                "class": meta["class"],
                "close": close,
                "change_pct": chg,
                "scanned_at": now,
            }
        except Exception as exc:
            logger.debug("fetch_global_snapshot(%s / %s) attempt failed: %s",
                         code, meta["yf"], exc)
            return None

    # Phase 1 — parallel pass. 4 workers (down from 8) to reduce sqlite
    # cache contention without giving up meaningful parallelism.
    with ThreadPoolExecutor(max_workers=4) as ex:
        for code, data in ex.map(
            lambda item: (item[0], _fetch_one(*item)),
            GLOBAL_SYMBOLS.items(),
        ):
            if data:
                result[code] = data

    # Phase 2 — sequential retry for anything the parallel pass missed.
    missing = [c for c in GLOBAL_SYMBOLS if c not in result]
    for code in missing:
        data = _fetch_one(code, GLOBAL_SYMBOLS[code])
        if data:
            result[code] = data
        else:
            logger.warning("fetch_global_snapshot(%s) failed both attempts", code)

    logger.info("fetch_global_snapshot: %d/%d assets fetched (phase1=parallel, phase2 retried %d)",
                len(result), len(GLOBAL_SYMBOLS), len(missing))
    return result


def resolve_symbol(text: str) -> Optional[str]:
    """
    Resolve user input to a valid SET symbol.
    Handles aliases (SCG→SCC), case, and whitespace.
    Returns the symbol string or None if not found.
    """
    upper = text.upper().strip().replace("SET:", "")
    if upper in _STOCK_SET:
        return upper
    alias = SYMBOL_ALIASES.get(upper)
    if alias and alias in _STOCK_SET:
        return alias
    return None


# Map clean symbol → yfinance ticker
def _to_yf_ticker(symbol: str) -> str:
    if symbol.startswith("^"):
        return symbol  # already a yfinance index ticker
    return f"{symbol}.BK"


# ─── Fetch functions ──────────────────────────────────────────────────────────

def get_stock_list() -> list[str]:
    """Return the full list of tracked SET stock symbols (without .BK suffix)."""
    return SET_STOCKS.copy()




def fetch_ohlcv_settrade(symbol: str, period: str = "1Y") -> Optional[pd.DataFrame]:
    """Fetch OHLCV via Settrade Open API only — no yfinance fallback."""
    try:
        from settrade_client import get_ohlcv, is_api_available
        if not is_api_available():
            return None
        return get_ohlcv(symbol, period=period)
    except Exception as exc:
        logger.error("fetch_ohlcv_settrade(%s) failed: %s", symbol, exc)
        return None


def fetch_ohlcv(symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV for a single symbol.
    Tries SET Trade API first, falls back to yfinance.
    """
    # ── Primary: SET Trade Open API ──
    try:
        from settrade_client import get_ohlcv, is_api_available
        if is_api_available():
            # Map period: yfinance "1y" → settrade "1Y"
            period_map = {"1y": "1Y", "2y": "3Y", "5y": "5Y", "6mo": "6M", "3mo": "3M"}
            st_period = period_map.get(period, "1Y")
            df = get_ohlcv(symbol, period=st_period)
            if df is not None and not df.empty:
                logger.debug("Fetched %s from SET Trade API (%d rows)", symbol, len(df))
                return df
    except Exception as exc:
        logger.debug("SET Trade API failed for %s, falling back: %s", symbol, exc)

    # ── Fallback: yfinance ──
    ticker = "^SET.BK" if symbol == "SET" else _to_yf_ticker(symbol)
    try:
        df = yf.download(ticker, period=period, progress=False, auto_adjust=False)
        if df.empty:
            logger.warning("No data returned for %s", ticker)
            return None
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df.index.name = "Date"
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.columns = [c.replace(" ", "_") for c in df.columns]
        return df
    except Exception as exc:
        logger.error("yfinance also failed for %s: %s", ticker, exc)
        return None


# ─── BigQuery helpers ────────────────────────────────────────────────────────

_SCAN_RESULTS_SCHEMA = [
    ("scanned_at",        "TIMESTAMP"),
    ("symbol",            "STRING"),
    ("name",              "STRING"),
    ("stage",             "INT64"),
    ("pattern",           "STRING"),
    ("close",             "FLOAT64"),
    ("change_pct",        "FLOAT64"),
    ("volume",            "INT64"),
    ("volume_ratio",      "FLOAT64"),
    ("sma50",             "FLOAT64"),
    ("sma150",            "FLOAT64"),
    ("sma200",            "FLOAT64"),
    ("high_52w",          "FLOAT64"),
    ("low_52w",           "FLOAT64"),
    ("strength_score",    "FLOAT64"),
    ("atr",               "FLOAT64"),
    ("trade_value_m",     "FLOAT64"),
    ("pct_from_52w_high", "FLOAT64"),
    ("stop_loss",         "FLOAT64"),
    ("target_price",      "FLOAT64"),
    ("breakout_count_1y", "INT64"),
    ("tradingview_url",   "STRING"),
    ("breakout_details",  "STRING"),
]


def init_bq(project_id: str, dataset: str = "signalix") -> None:
    """Initialize BigQuery client and ensure ohlcv + scan_results tables exist."""
    global _bq_client, _bq_project, _bq_dataset, BQ_AVAILABLE
    # Load static margin securities map (cheap, no IO beyond file read)
    # — happens once per cold start, before BQ work so even if BQ init
    # fails the margin lookup still works.
    _load_margin_securities()
    try:
        from google.cloud import bigquery
        _bq_client = bigquery.Client(project=project_id)
        _bq_project = project_id
        _bq_dataset = dataset
        _ensure_bq_table()
        _ensure_scan_results_table()
        # Idempotent FSM-iteration migration: adds columns to scan_results
        # for sub_stage / sma10 / sma20 / sma200_roc20 / pivot_price /
        # pivot_stop / stage_weakening, and creates the breadth_snapshots
        # table for per-scan dashboard time series. Auto-applies on first
        # cold start after deploy; safe to re-run on subsequent starts
        # because it uses ADD COLUMN IF NOT EXISTS + get-or-create.
        try:
            _migrate_bq_schema()
        except Exception as exc:
            # Don't fail BQ init if migration trips a permission edge —
            # service can keep running without the new columns; old
            # save_signals_to_bq path stays compatible.
            logger.warning("BQ schema migration deferred: %s", exc)
        BQ_AVAILABLE = True
        logger.info("BigQuery initialized: %s.%s", project_id, dataset)
    except Exception as exc:
        logger.warning("BigQuery init failed (continuing without BQ): %s", exc)
        BQ_AVAILABLE = False


def _migrate_bq_schema() -> None:
    """Idempotent schema migration for the FSM persistence iteration.
    Adds new columns to scan_results and creates breadth_snapshots table.
    Runs on every BQ init (cheap — uses IF NOT EXISTS + get_table check).
    """
    from google.cloud import bigquery
    client = _bq_client
    if client is None:
        return

    # ── 1. Add new columns to scan_results ──
    new_cols = [
        ("sub_stage",       "STRING"),
        ("sma10",           "FLOAT64"),
        ("sma20",           "FLOAT64"),
        ("sma200_roc20",    "FLOAT64"),
        ("pivot_price",     "FLOAT64"),
        ("pivot_stop",      "FLOAT64"),
        ("stage_weakening", "BOOL"),
    ]
    table_id = f"{_bq_project}.{_bq_dataset}.scan_results"
    for col_name, col_type in new_cols:
        ddl = f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
        try:
            client.query(ddl).result(timeout=60)
        except Exception as exc:
            logger.warning("scan_results ADD COLUMN %s: %s", col_name, exc)

    # ── 2. breadth_snapshots time-series table ──
    breadth_id = f"{_bq_project}.{_bq_dataset}.breadth_snapshots"
    try:
        client.get_table(breadth_id)
        return  # already exists
    except Exception:
        pass
    breadth_schema = [
        bigquery.SchemaField("scanned_at",      "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("scan_type",       "STRING"),
        bigquery.SchemaField("mode",            "STRING"),
        bigquery.SchemaField("total_stocks",    "INT64"),
        bigquery.SchemaField("stage1_count",    "INT64"),
        bigquery.SchemaField("stage2_count",    "INT64"),
        bigquery.SchemaField("stage3_count",    "INT64"),
        bigquery.SchemaField("stage4_count",    "INT64"),
        bigquery.SchemaField("stage_1_base",         "INT64"),
        bigquery.SchemaField("stage_1_prep",         "INT64"),
        bigquery.SchemaField("stage_2_ignition",     "INT64"),
        bigquery.SchemaField("stage_2_overextended", "INT64"),
        bigquery.SchemaField("stage_2_contraction",  "INT64"),
        bigquery.SchemaField("stage_2_pivot_ready",  "INT64"),
        bigquery.SchemaField("stage_2_markup",       "INT64"),
        bigquery.SchemaField("stage_3_volatile",     "INT64"),
        bigquery.SchemaField("stage_3_dist_dist",    "INT64"),
        bigquery.SchemaField("stage_4_breakdown",    "INT64"),
        bigquery.SchemaField("stage_4_downtrend",    "INT64"),
        bigquery.SchemaField("advancing",       "INT64"),
        bigquery.SchemaField("declining",       "INT64"),
        bigquery.SchemaField("unchanged",       "INT64"),
        bigquery.SchemaField("new_highs_52w",   "INT64"),
        bigquery.SchemaField("new_lows_52w",    "INT64"),
        bigquery.SchemaField("breakout_count",  "INT64"),
        bigquery.SchemaField("vcp_count",       "INT64"),
        bigquery.SchemaField("above_ma200",     "INT64"),
        bigquery.SchemaField("below_ma200",     "INT64"),
        bigquery.SchemaField("set_index_close",      "FLOAT64"),
        bigquery.SchemaField("set_index_change_pct", "FLOAT64"),
    ]
    table = bigquery.Table(breadth_id, schema=breadth_schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="scanned_at",
    )
    client.create_table(table)
    logger.info("Created breadth_snapshots table (partitioned by scanned_at)")


def _bq_table() -> str:
    return f"`{_bq_project}.{_bq_dataset}.ohlcv`"


def _ensure_bq_table() -> None:
    from google.cloud import bigquery
    schema = [bigquery.SchemaField(n, t) for n, t in _BQ_SCHEMA]
    table_id = f"{_bq_project}.{_bq_dataset}.ohlcv"
    table = bigquery.Table(table_id, schema=schema)
    # No time partitioning — avoids 5,000 partition-modification/day quota.
    # Clustering by symbol keeps per-symbol queries fast.
    table.clustering_fields = ["symbol", "date"]
    _bq_client.create_table(table, exists_ok=True)
    logger.info("BQ table ready: %s", table_id)


def _ensure_scan_results_table() -> None:
    from google.cloud import bigquery
    schema = [bigquery.SchemaField(n, t) for n, t in _SCAN_RESULTS_SCHEMA]
    table_id = f"{_bq_project}.{_bq_dataset}.scan_results"
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="scanned_at")
    table.clustering_fields = ["symbol"]
    _bq_client.create_table(table, exists_ok=True)
    logger.info("BQ table ready: %s", table_id)


def save_signals_to_bq(signals: list) -> None:
    """Append scan results to BQ scan_results table. Called on every scan.

    Includes the FSM-iteration fields (sub_stage, sma10, sma20,
    sma200_roc20, pivot_price, pivot_stop, stage_weakening) so historical
    SQL queries can answer 'how many PIVOT_READY stocks 30 days ago' etc.
    Pre-migration rows have NULLs for these columns; the schema migration
    in init_bq adds them idempotently on next deploy.
    """
    if not signals or _bq_client is None:
        return
    import json
    scanned_at = signals[0].scanned_at  # same for all signals in this scan
    rows = []
    for s in signals:
        rows.append({
            "scanned_at": scanned_at,
            "symbol": s.symbol, "name": s.name, "stage": s.stage,
            "pattern": s.pattern, "close": s.close, "change_pct": s.change_pct,
            "volume": s.volume, "volume_ratio": s.volume_ratio,
            "sma50": s.sma50, "sma150": s.sma150, "sma200": s.sma200,
            "high_52w": s.high_52w, "low_52w": s.low_52w,
            "strength_score": s.strength_score, "atr": s.atr,
            "trade_value_m": getattr(s, "trade_value_m", 0.0),
            "pct_from_52w_high": getattr(s, "pct_from_52w_high", 0.0),
            "stop_loss": getattr(s, "stop_loss", 0.0),
            "target_price": getattr(s, "target_price", 0.0),
            "breakout_count_1y": getattr(s, "breakout_count_1y", 0),
            "tradingview_url": s.tradingview_url,
            "breakout_details": json.dumps(getattr(s, "breakout_details", {}) or {}),
            # FSM-iteration fields (added by _migrate_bq_schema)
            "sub_stage":       getattr(s, "sub_stage", "") or "",
            "sma10":           getattr(s, "sma10", 0.0),
            "sma20":           getattr(s, "sma20", 0.0),
            "sma200_roc20":    getattr(s, "sma200_roc20", 0.0),
            "pivot_price":     getattr(s, "pivot_price", 0.0),
            "pivot_stop":      getattr(s, "pivot_stop", 0.0),
            "stage_weakening": bool(getattr(s, "stage_weakening", False)),
        })
    table_id = f"{_bq_project}.{_bq_dataset}.scan_results"
    try:
        errors = _bq_client.insert_rows_json(table_id, rows)
        if errors:
            logger.error("save_signals_to_bq insert errors: %s", errors[:3])
        else:
            logger.info("Saved %d signals to BQ scan_results (scanned_at=%s)", len(rows), scanned_at[:16])
    except Exception as exc:
        logger.error("save_signals_to_bq failed: %s", exc)


def load_latest_signals_from_bq() -> list:
    """Load all signals from the single most-recent scan stored in BQ scan_results.
    Uses MAX(scanned_at) as a timestamp — not DATE — so only one scan is returned
    even when multiple scans run on the same calendar day (4x daily schedule).
    """
    if _bq_client is None:
        return []
    import json
    import dataclasses
    from analyzer import StockSignal
    valid_fields = {f.name for f in dataclasses.fields(StockSignal)}
    query = f"""
        SELECT * EXCEPT(scanned_at)
        FROM `{_bq_project}.{_bq_dataset}.scan_results`
        WHERE scanned_at = (
            SELECT MAX(scanned_at)
            FROM `{_bq_project}.{_bq_dataset}.scan_results`
        )
        ORDER BY strength_score DESC
    """
    try:
        rows = list(_bq_client.query(query).result())
        if not rows:
            logger.info("BQ scan_results: no rows found")
            return []
        signals = []
        for row in rows:
            d = dict(row)
            d["breakout_details"] = json.loads(d.get("breakout_details") or "{}")
            d["stage"] = int(d.get("stage") or 1)
            d["volume"] = int(d.get("volume") or 0)
            d["breakout_count_1y"] = int(d.get("breakout_count_1y") or 0)
            filtered = {k: v for k, v in d.items() if k in valid_fields and v is not None}
            try:
                sig = StockSignal(**filtered)
                if sig.symbol:
                    signals.append(sig)
            except Exception as e:
                logger.debug("load_latest_signals_from_bq skip row: %s", e)
        logger.info("Loaded %d signals from BQ scan_results", len(signals))
        return signals
    except Exception as exc:
        logger.error("load_latest_signals_from_bq failed: %s", exc)
        return []


def _df_to_bq(symbol: str, df: pd.DataFrame) -> "pd.DataFrame":
    """Convert OHLCV DataFrame to BQ-ready DataFrame."""
    bq = df.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    bq.columns = ["date", "open", "high", "low", "close", "volume"]
    bq["symbol"] = symbol
    bq["date"] = pd.to_datetime(bq["date"]).dt.date
    bq = bq.dropna(subset=["close"])
    bq["volume"] = bq["volume"].fillna(0).astype("int64")
    return bq[["symbol", "date", "open", "high", "low", "close", "volume"]]


def save_ohlcv_to_bq(symbol: str, df: pd.DataFrame) -> None:
    """Append OHLCV history for one symbol to BQ, skipping already-stored rows."""
    if _bq_client is None or df is None or df.empty:
        return
    try:
        from google.cloud import bigquery
        bq_df = _df_to_bq(symbol, df)
        # Only insert rows newer than what's already in BQ for this symbol
        max_df = _bq_client.query(
            f"SELECT MAX(date) AS max_date FROM {_bq_table()} WHERE symbol = '{symbol}'"
        ).to_dataframe()
        last = max_df["max_date"].iloc[0] if not max_df.empty else None
        if pd.notna(last):
            bq_df = bq_df[bq_df["date"] > last]
        if bq_df.empty:
            logger.debug("BQ: %s already up to date", symbol)
            return
        table_id = f"{_bq_project}.{_bq_dataset}.ohlcv"
        job = _bq_client.load_table_from_dataframe(
            bq_df, table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[bigquery.SchemaField(n, t) for n, t in _BQ_SCHEMA],
            ),
        )
        job.result()
        logger.info("BQ: saved %d new rows for %s", len(bq_df), symbol)
    except Exception as exc:
        logger.error("save_ohlcv_to_bq(%s) failed: %s", symbol, exc)


_PRICE_COLS = ["Open", "High", "Low", "Close"]

def load_all_ohlcv_from_bq(lookback_days: int = 400) -> dict[str, pd.DataFrame]:
    """Load last N days of OHLCV for all symbols from BQ. Returns dict[symbol, df]."""
    if _bq_client is None:
        return {}
    try:
        query = f"""
        SELECT symbol, date, open, high, low, close, volume
        FROM {_bq_table()}
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
        ORDER BY symbol, date
        """
        df = _bq_client.query(query).to_dataframe()
        if df.empty:
            return {}
        df["date"] = pd.to_datetime(df["date"])
        result: dict[str, pd.DataFrame] = {}
        for symbol, grp in df.groupby("symbol"):
            g = grp.set_index("date").drop(columns=["symbol"])
            g.index.name = "Date"
            g.columns = ["Open", "High", "Low", "Close", "Volume"]
            g[_PRICE_COLS] = g[_PRICE_COLS].astype("float32")
            result[symbol] = g
        del df  # free the flat BQ result before returning
        logger.info("BQ: loaded %d symbols (%d-day window)", len(result), lookback_days)
        return result
    except Exception as exc:
        logger.error("load_all_ohlcv_from_bq failed: %s", exc)
        return {}


def load_ath_from_bq() -> dict[str, float]:
    """Compute ATH for every symbol from full BQ history. Returns {symbol: ath}."""
    if _bq_client is None:
        return {}
    try:
        query = f"SELECT symbol, MAX(high) AS ath FROM {_bq_table()} GROUP BY symbol"
        df = _bq_client.query(query).to_dataframe()
        return {row.symbol: round(float(row.ath), 4) for row in df.itertuples()}
    except Exception as exc:
        logger.error("load_ath_from_bq failed: %s", exc)
        return {}


def append_new_candles_to_bq(all_data: dict[str, pd.DataFrame]) -> None:
    """After a scan, append any candles newer than what's already in BQ."""
    if _bq_client is None or not all_data:
        return
    try:
        from google.cloud import bigquery
        symbols_csv = ", ".join(f"'{s}'" for s in all_data if s != "SET")
        max_df = _bq_client.query(f"""
            SELECT symbol, MAX(date) AS max_date
            FROM {_bq_table()}
            WHERE symbol IN ({symbols_csv})
            GROUP BY symbol
        """).to_dataframe()
        max_dates = dict(zip(max_df["symbol"], max_df["max_date"]))

        new_rows = []
        for symbol, df in all_data.items():
            if symbol == "SET" or df is None or df.empty:
                continue
            bq_df = _df_to_bq(symbol, df)
            last = max_dates.get(symbol)
            if last is not None:
                bq_df = bq_df[bq_df["date"] > last]
            if not bq_df.empty:
                new_rows.append(bq_df)

        if not new_rows:
            logger.info("BQ: no new candles to append")
            return
        combined = pd.concat(new_rows, ignore_index=True)
        table_id = f"{_bq_project}.{_bq_dataset}.ohlcv"
        job = _bq_client.load_table_from_dataframe(
            combined, table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[bigquery.SchemaField(n, t) for n, t in _BQ_SCHEMA],
            ),
        )
        job.result()
        logger.info("BQ: appended %d new candles across %d symbols", len(combined), len(new_rows))
    except Exception as exc:
        logger.error("append_new_candles_to_bq failed: %s", exc)


def fetch_all_stocks(period: str = "1y") -> dict[str, pd.DataFrame]:
    """
    Fetch OHLCV for all SET_STOCKS + SET index.
    Priority (always in this order — NO short-circuit on BQ):
      1. Settrade Open API for all SET stocks (primary — native Thai data,
         always the freshest since it goes straight to source).
      2. BigQuery backfill for older rows (Settrade wins on overlapping
         dates) and as fallback for stocks Settrade missed. BQ is historical
         supplement only — never a substitute that lets us skip Settrade.
      3. yfinance for the SET index (not in Settrade) + stocks still missing.

    Historical note: a prior version short-circuited here on "BQ ≤5d stale
    AND ≥95% coverage" and returned BQ immediately. That caused a self-
    reinforcing cache-rot loop — every scan saw BQ as "fresh enough",
    skipped Settrade, wrote no new candles, and BQ stayed frozen on
    whatever day it stopped advancing. Removed.
    """
    results: dict[str, pd.DataFrame] = {}

    # --- Step 1: Settrade Open API (primary for stocks) ---
    try:
        from settrade_client import get_bulk_ohlcv, is_api_available as _st_ok
        if _st_ok():
            st_period = "1Y" if period.lower() in ("1y", "1yr", "1year") else period.upper()
            st_data = get_bulk_ohlcv(SET_STOCKS, period=st_period, max_workers=30)
            for sym, df in st_data.items():
                if df is None or df.empty or df["Close"].dropna().empty:
                    continue
                df = df.dropna(subset=["Close"]).copy()
                df.index = pd.to_datetime(df.index).tz_localize(None)
                df.index.name = "Date"
                price_cols = [c for c in _PRICE_COLS if c in df.columns]
                if price_cols:
                    df[price_cols] = df[price_cols].astype("float32")
                results[sym] = df
            logger.info("fetch_all_stocks: Settrade returned %d/%d stock symbols",
                        len(results), len(SET_STOCKS))
        else:
            logger.warning("fetch_all_stocks: Settrade API unavailable — using BQ + yfinance only")
    except Exception as exc:
        logger.warning("fetch_all_stocks: Settrade primary failed: %s — using BQ + yfinance", exc)

    # --- Step 2: yfinance fallback for Settrade misses (BEFORE BQ) ---
    # Settrade is intermittent — when it returns empty for a stock, we used
    # to fall straight to BQ. But BQ can carry dividend-touched values from
    # past scans (e.g. a yfinance fallback using auto_adjust=True at some
    # earlier point, or a Settrade response that was adjusted at the time
    # of write). yfinance auto_adjust=False is more reliably unadjusted at
    # fetch time than our potentially-rotted BQ cache, so try it first.
    settrade_misses = [s for s in SET_STOCKS if s not in results]
    if settrade_misses:
        logger.info("fetch_all_stocks: Settrade missed %d stocks — trying yfinance unadjusted",
                    len(settrade_misses))
        yf_tickers_for_misses = [_to_yf_ticker(s) for s in settrade_misses]
        try:
            raw = yf.download(
                yf_tickers_for_misses,
                period="1y",
                group_by="ticker",
                progress=False,
                auto_adjust=False,
                threads=True,
            )
            yf_recovered = 0
            for sym, yfk in zip(settrade_misses, yf_tickers_for_misses):
                try:
                    if isinstance(raw.columns, pd.MultiIndex):
                        df_one = raw[yfk].dropna(subset=["Close"])
                    else:
                        df_one = raw.dropna(subset=["Close"])
                    if df_one is None or df_one.empty:
                        continue
                    df_one.index = pd.to_datetime(df_one.index).tz_localize(None)
                    df_one.index.name = "Date"
                    price_cols = [c for c in _PRICE_COLS if c in df_one.columns]
                    if price_cols:
                        df_one[price_cols] = df_one[price_cols].astype("float32")
                    results[sym] = df_one
                    yf_recovered += 1
                except Exception:
                    continue
            logger.info("fetch_all_stocks: yfinance recovered %d/%d Settrade misses",
                        yf_recovered, len(settrade_misses))
        except Exception as exc:
            logger.warning("fetch_all_stocks: yfinance bulk fallback failed: %s", exc)

    # --- Step 3 REMOVED: BQ history backfill ---
    # Was: extend Settrade's 1Y with older BQ history, with BQ-only fallback
    # for stocks Settrade missed. Removed because:
    #   1. Settrade and yfinance index timestamps don't dedup against BQ's
    #      (different timezone normalisation), so concat+dedup leaves both
    #      copies in the merged dataframe — bars=624 for SYMC.
    #   2. The 252-bar tail used by high_52w then doesn't span a full year
    #      of unique trading dates, producing nonsense like merged-hi52=4.42
    #      when both Settrade and yfinance individually report 4.90.
    #   3. BQ rows for some dividend-paying stocks have rotted values from
    #      past scans (~3-6% lower than yfinance unadj). Even when the
    #      merge worked correctly, those polluted bars dragged SMA200 down
    #      and produced false Stage 2 alignment.
    # Settrade's 1Y window + yfinance fallback (Step 2) gives enough bars
    # (~241-365) for SMA200 + 20-bar rising check. BQ stays useful for the
    # ATH cache (separate table) — it's just not in the scan path anymore.

    # --- Step 4: yfinance for SET index (always) + any stocks still missing ---
    missing_stocks = [s for s in SET_STOCKS if s not in results]
    yf_targets = missing_stocks + ["SET"]
    yf_tickers = [("^SET.BK" if s == "SET" else _to_yf_ticker(s)) for s in yf_targets]
    logger.info("fetch_all_stocks: yfinance fallback for %d symbols (SET index + %d missing stocks)",
                len(yf_targets), len(missing_stocks))

    try:
        raw = yf.download(
            yf_tickers,
            period=period,
            group_by="ticker",
            progress=False,
            auto_adjust=False,
            threads=True,
        )
    except Exception as exc:
        logger.error("fetch_all_stocks: yfinance batch download failed: %s", exc)
        return results

    multi_ticker = len(yf_tickers) > 1
    top_level = raw.columns.get_level_values(0) if multi_ticker else None
    for symbol, ticker in zip(yf_targets, yf_tickers):
        try:
            if not multi_ticker:
                df = raw.copy()
            elif ticker in top_level:
                df = raw[ticker].copy()
            else:
                continue

            if df.empty or df["Close"].dropna().empty:
                continue

            df = df.dropna(subset=["Close"])
            df.index = pd.to_datetime(df.index).tz_localize(None)
            df.index.name = "Date"
            df.columns = [c.replace(" ", "_") for c in df.columns]
            price_cols = [c for c in _PRICE_COLS if c in df.columns]
            df[price_cols] = df[price_cols].astype("float32")
            results[symbol] = df
        except Exception as exc:
            logger.warning("fetch_all_stocks: could not process %s: %s", symbol, exc)

    del raw
    logger.info("fetch_all_stocks: final coverage %d symbols (stocks+SET)", len(results))
    return results


def GET_ALL_SYMBOLS_WITH_INDEX() -> list[str]:
    return SET_STOCKS + ["SET"]


def fetch_latest_candles(lookback_days: int = 400) -> dict[str, pd.DataFrame]:
    """
    Intraday scan: BQ history (base) + Settrade recent candles + real-time quote patch.
    Settrade is the primary source (authoritative for Thai stocks, real-time prices).
    Falls back to yfinance 5d if Settrade is unavailable or returns < 50% coverage.
    """
    bq_data = load_all_ohlcv_from_bq(lookback_days=lookback_days) if BQ_AVAILABLE else {}
    if not bq_data:
        logger.warning("fetch_latest_candles: BQ unavailable — falling back to full fetch")
        return fetch_all_stocks(period="1y")

    all_symbols = GET_ALL_SYMBOLS_WITH_INDEX()
    stock_symbols = [s for s in all_symbols if s != "SET"]  # SET index not in Settrade symbol list

    # --- Step 1: Settrade recent daily candles + real-time quote patch ---
    recent_data: dict[str, pd.DataFrame] = {}
    try:
        from settrade_client import get_bulk_ohlcv, get_bulk_quotes, is_api_available as _st_ok
        if _st_ok():
            # 1a. Last 30 daily candles (fills any gap between BQ and today)
            recent_data = get_bulk_ohlcv(stock_symbols, period="1M", max_workers=10)

            # 1b. Real-time quotes — patch today's Close/Volume with live price
            today = pd.Timestamp.now().normalize()
            quotes = get_bulk_quotes(stock_symbols, max_workers=10)
            patched = 0
            for sym, q in quotes.items():
                last = float(q.get("last") or 0)
                if last <= 0:
                    continue
                today_row = pd.DataFrame({
                    "Open":   [last],
                    "High":   [float(q.get("high") or last)],
                    "Low":    [float(q.get("low") or last)],
                    "Close":  [last],
                    "Volume": [int(q.get("volume") or 0)],
                }, index=[today])
                base = recent_data.get(sym, pd.DataFrame())
                if not base.empty:
                    base = base[base.index < today]
                recent_data[sym] = pd.concat([base, today_row]).sort_index() if not base.empty else today_row
                patched += 1
            logger.info("fetch_latest_candles: Settrade %d OHLCV + %d live quotes patched", len(recent_data), patched)
    except Exception as exc:
        logger.error("fetch_latest_candles: Settrade failed: %s", exc)

    # --- Step 2: yfinance 5d fallback (only for symbols Settrade didn't cover) ---
    missing = [s for s in all_symbols if s not in recent_data]
    if len(recent_data) < len(stock_symbols) * 0.5:
        logger.warning("fetch_latest_candles: Settrade <50%% — using yfinance 5d for all")
        missing = all_symbols  # redo everything via yfinance
    if missing:
        tickers = [("^SET.BK" if s == "SET" else _to_yf_ticker(s)) for s in missing]
        try:
            raw = yf.download(tickers, period="5d", group_by="ticker", progress=False, auto_adjust=False, threads=True)
            multi = len(tickers) > 1
            top_level = raw.columns.get_level_values(0) if multi else None
            for symbol, ticker in zip(missing, tickers):
                try:
                    ndf = raw[ticker].copy() if (multi and ticker in top_level) else (raw.copy() if not multi else pd.DataFrame())
                    if not ndf.empty:
                        ndf = ndf.dropna(subset=["Close"])
                        ndf.index = pd.to_datetime(ndf.index).tz_localize(None)
                        ndf.index.name = "Date"
                        if isinstance(ndf.columns, pd.MultiIndex):
                            ndf.columns = ndf.columns.get_level_values(0)
                        ndf.columns = [c.replace(" ", "_") for c in ndf.columns]
                        recent_data[symbol] = ndf
                except Exception:
                    pass
        except Exception as exc:
            logger.error("fetch_latest_candles: yfinance fallback failed: %s", exc)

    # --- Step 3: Merge BQ history + recent data ---
    results: dict[str, pd.DataFrame] = {}
    for symbol in all_symbols:
        bq_df = bq_data.get(symbol)
        new_df = recent_data.get(symbol, pd.DataFrame())
        if bq_df is not None and not bq_df.empty and not new_df.empty:
            combined = pd.concat([bq_df, new_df])
            combined = combined[~combined.index.duplicated(keep="last")]
            results[symbol] = combined.sort_index()
        elif bq_df is not None and not bq_df.empty:
            results[symbol] = bq_df
        elif not new_df.empty:
            results[symbol] = new_df

    # Final SET backfill — index is not in Settrade and may be silently missing from
    # yfinance fallback. Guarantee a usable DataFrame so compute_market_breadth always
    # gets real SET numbers for the breadth card hero.
    set_df = results.get("SET")
    if set_df is None or len(set_df) < 2:
        try:
            raw = yf.download("^SET.BK", period="1mo", progress=False, auto_adjust=False)
            if raw is not None and not raw.empty:
                ndf = raw.dropna(subset=["Close"]).copy()
                ndf.index = pd.to_datetime(ndf.index).tz_localize(None)
                ndf.index.name = "Date"
                if isinstance(ndf.columns, pd.MultiIndex):
                    ndf.columns = ndf.columns.get_level_values(0)
                ndf.columns = [c.replace(" ", "_") for c in ndf.columns]
                if len(ndf) >= 2:
                    results["SET"] = ndf
        except Exception as exc:
            logger.error("fetch_latest_candles: SET backfill failed: %s", exc)

    logger.info("fetch_latest_candles: %d symbols merged (BQ + Settrade/yfinance)", len(results))
    return results


def fetch_indexes_with_history(period: str = "1y") -> dict[str, pd.DataFrame]:
    """
    Fetch full OHLCV history DataFrames for all SET indexes.
    Used by analyze_index() for MACD/RSI calculations.

    Per-ticker yf.Ticker().history() rather than batched yf.download(
    ..., group_by="ticker") because the batch path silently dropped
    5/6 Thai indexes in prod — the MultiIndex only contained ^SET.BK.
    Per-ticker is reliable but sequential = slow (6 HTTP calls,
    ~6-10s wall). Parallelised here via ThreadPoolExecutor (4 workers)
    matching the fetch_global_snapshot pattern, cuts wall time to
    ~2-3s with no observable yfinance sqlite contention at this size.
    """
    from concurrent.futures import ThreadPoolExecutor

    def _fetch_one(name: str, ticker: str):
        try:
            df = yf.Ticker(ticker).history(period=period, auto_adjust=False)
            if df is None or df.empty:
                logger.warning("fetch_indexes_with_history: %s (%s) returned empty", name, ticker)
                return None
            df = df.dropna(subset=["Close"])
            if len(df) < 30:
                logger.warning("fetch_indexes_with_history: %s (%s) only %d rows", name, ticker, len(df))
                return None
            df.index = pd.to_datetime(df.index).tz_localize(None)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            return df
        except Exception as exc:
            logger.error("fetch_indexes_with_history(%s / %s) failed: %s", name, ticker, exc)
            return None

    result: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {
            ex.submit(_fetch_one, name, ticker): name
            for name, ticker in INDEX_SYMBOLS.items()
        }
        for fut in futures:
            df = fut.result()
            if df is not None:
                result[futures[fut]] = df
    logger.info("fetch_indexes_with_history: %d/%d indexes fetched",
                len(result), len(INDEX_SYMBOLS))
    return result


def get_latest_price(symbol: str) -> Optional[dict]:
    """
    Get latest price info for a single symbol.

    Returns dict with: symbol, close, change_pct, volume, date
    """
    df = fetch_ohlcv(symbol, period="5d")
    if df is None or len(df) < 2:
        return None

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    change_pct = ((latest["Close"] - prev["Close"]) / prev["Close"]) * 100

    return {
        "symbol": symbol,
        "close": round(float(latest["Close"]), 2),
        "change_pct": round(float(change_pct), 2),
        "volume": int(latest["Volume"]),
        "date": df.index[-1].strftime("%Y-%m-%d"),
    }


def tradingview_url(symbol: str) -> str:
    """Return TradingView chart URL for a SET symbol."""
    if symbol == "SET":
        return "https://www.tradingview.com/chart/?symbol=SET%3ASET"
    return f"https://www.tradingview.com/chart/?symbol=SET%3A{symbol}"




def fetch_ohlcv_max(symbol: str) -> Optional[pd.DataFrame]:
    """Fetch maximum available history for a symbol — used by sync_ath to compute
    the true all-time high.

    Uses yfinance period=max with auto_adjust=False so the stored ATH is the
    real unadjusted price peak (matches what users see on broker charts).
    Settrade can't help here: its SDK silently rejects get_candlestick with
    limit>=1095, capping us at ~1Y of history — not enough for stocks whose
    real ATH is years old (e.g. PTT's ATH is 2018-04-24).
    """
    ticker = "^SET.BK" if symbol == "SET" else _to_yf_ticker(symbol)
    try:
        df = yf.download(ticker, period="max", progress=False, auto_adjust=False)
        if df.empty:
            return None
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df.index.name = "Date"
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.columns = [c.replace(" ", "_") for c in df.columns]
        return df.dropna(subset=["Close"])
    except Exception as exc:
        logger.error("fetch_ohlcv_max(%s) failed: %s", symbol, exc)
        return None


def sync_ath_to_firestore(db, symbols: list[str], chunk: int = 0, chunk_size: int = 100) -> dict[str, float]:
    """
    Fetch max-period history and store ATH for each symbol in Firestore.
    Processes one chunk at a time (chunk * chunk_size to (chunk+1) * chunk_size).
    Returns {symbol: ath} for the processed chunk.
    """
    subset = symbols[chunk * chunk_size:(chunk + 1) * chunk_size]
    synced: dict[str, float] = {}
    now_str = datetime.now(BANGKOK_TZ).isoformat()

    for symbol in subset:
        df = fetch_ohlcv_max(symbol)
        if df is None or df.empty:
            logger.warning("sync_ath: no data for %s", symbol)
            continue
        try:
            ath_val = round(float(df["High"].max()), 4)
            ath_date = str(df["High"].idxmax().date())
            if db:
                db.collection("ath_cache").document(symbol).set(
                    {"ath": ath_val, "ath_date": ath_date, "synced_at": now_str},
                    merge=True,
                )
            synced[symbol] = ath_val
            # Save full OHLCV history to BigQuery while we have the data
            if BQ_AVAILABLE:
                save_ohlcv_to_bq(symbol, df)
        except Exception as exc:
            logger.warning("sync_ath: error for %s: %s", symbol, exc)

    logger.info("sync_ath: synced %d/%d symbols (chunk %d)", len(synced), len(subset), chunk)
    return synced


def load_ath_cache(db) -> dict[str, float]:
    """Load all ATH values from Firestore ath_cache collection."""
    if db is None:
        return {}
    try:
        docs = db.collection("ath_cache").stream()
        return {doc.id: doc.to_dict().get("ath", 0.0) for doc in docs}
    except Exception as exc:
        logger.error("load_ath_cache failed: %s", exc)
        return {}


def save_scan_state(db, breadth, indexes: dict, sector_trends: list, scan_type: str, mode: str,
                    sector_indexes: dict | None = None) -> None:
    """Persist full scan state to scan_state/latest for warm startup.

    sector_indexes: {sector_code: {close, change_pct, scanned_at}} — the 8
    SET industry indexes fetched by fetch_sector_index_prices. Persisting
    these survives multi-instance Cloud Run state loss: if instance A runs
    /scan and populates _last_sector_indexes in RAM, instance B (serving a
    LINE webhook moments later) can warm from this Firestore doc instead
    of showing empty index prices on the sector overview card.
    """
    if db is None:
        return
    try:
        scanned_at = getattr(breadth, "scanned_at", "")
        stamped_indexes = {name: {**data, "scanned_at": scanned_at} for name, data in indexes.items()}
        doc = {
            "scanned_at": scanned_at,
            "scan_type": scan_type,
            "mode": mode,
            "total_stocks": getattr(breadth, "total_stocks", 0),
            "breadth": breadth.__dict__,
            "indexes": stamped_indexes,
            "sector_trends": [s.__dict__ for s in sector_trends],
            "sector_indexes": sector_indexes or {},
        }
        db.collection("scan_state").document("latest").set(doc)
        logger.info("Saved scan_state/latest (type=%s, mode=%s, sector_indexes=%d)",
                    scan_type, mode, len(sector_indexes or {}))
    except Exception as exc:
        logger.error("save_scan_state failed: %s", exc)


def load_scan_state(db) -> "dict | None":
    """Load full scan state from Firestore scan_state/latest."""
    if db is None:
        return None
    try:
        import dataclasses
        from analyzer import MarketBreadth, SectorSummary
        doc_ref = db.collection("scan_state").document("latest").get()
        if not doc_ref.exists:
            return None
        data = doc_ref.to_dict()

        valid_breadth = {f.name for f in dataclasses.fields(MarketBreadth)}
        breadth = MarketBreadth(**{k: v for k, v in data.get("breadth", {}).items() if k in valid_breadth})

        valid_sector = {f.name for f in dataclasses.fields(SectorSummary)}
        sector_trends = []
        for s in data.get("sector_trends", []):
            try:
                sector_trends.append(SectorSummary(**{k: v for k, v in s.items() if k in valid_sector}))
            except Exception:
                continue

        logger.info("Loaded scan_state/latest (type=%s, scanned_at=%s)", data.get("scan_type"), data.get("scanned_at", "")[:16])
        return {
            "breadth": breadth,
            "indexes": data.get("indexes", {}),
            "sector_trends": sector_trends,
            "sector_indexes": data.get("sector_indexes", {}),
            "scanned_at": data.get("scanned_at", ""),
            "scan_type": data.get("scan_type", ""),
        }
    except Exception as exc:
        logger.error("load_scan_state failed: %s", exc)
        return None


def save_signals_to_firestore(signals: list, db) -> None:
    """Batch-write latest scan signals to Firestore signals/{symbol}.
    Firestore batch limit is 500 ops — chunked to avoid silent failure."""
    if not signals or db is None:
        return
    try:
        BATCH_LIMIT = 499
        saved = 0
        for i in range(0, len(signals), BATCH_LIMIT):
            batch = db.batch()
            for signal in signals[i:i + BATCH_LIMIT]:
                doc_ref = db.collection("signals").document(signal.symbol)
                batch.set(doc_ref, signal.__dict__)
            batch.commit()
            saved += len(signals[i:i + BATCH_LIMIT])
        logger.info("Saved %d signals to Firestore (%d batches)", saved, -(-len(signals) // BATCH_LIMIT))
    except Exception as exc:
        logger.error("save_signals_to_firestore failed: %s", exc)


# ─── Paper-trading portfolio ──────────────────────────────────────────────
# Single-user paper portfolio for the Signalix trading simulation.
# Stored as a single Firestore doc (`paper_portfolio/default`). Tracks
# starting cash (1M THB), open positions, closed trades, and pending
# trade proposals awaiting user approval.
#
# Position sizing: Minervini-style 1% risk per trade.
#   risk_thb        = equity × risk_pct
#   risk_per_share  = entry - stop
#   shares          = floor(risk_thb / risk_per_share)
#   cost            = shares × entry
# Capped at max_position_pct (default 20%) of equity.

PAPER_PORTFOLIO_COLLECTION = "paper_portfolio"
PAPER_PORTFOLIO_DOC = "default"


def _new_paper_portfolio(starting_cash: float = 1_000_000.0) -> dict:
    """Return a fresh portfolio dict with starting cash and empty positions."""
    now = datetime.now(BANGKOK_TZ).isoformat()
    return {
        "starting_cash_thb": starting_cash,
        "cash_thb": starting_cash,
        "started_at": now,
        "last_updated": now,
        "max_positions": 5,
        "risk_per_trade_pct": 1.0,
        "max_position_pct": 20.0,
        "positions": [],          # open positions
        "closed_trades": [],      # historical closed trades
        "pending_proposals": [],  # trade proposals awaiting approval
    }


def load_paper_portfolio(db) -> dict:
    """Load paper portfolio state. Returns a fresh portfolio if none exists."""
    if db is None:
        return _new_paper_portfolio()
    try:
        doc = db.collection(PAPER_PORTFOLIO_COLLECTION).document(PAPER_PORTFOLIO_DOC).get()
        if not doc.exists:
            return _new_paper_portfolio()
        data = doc.to_dict()
        # Backfill any missing fields for forward compat with older docs.
        defaults = _new_paper_portfolio()
        for k, v in defaults.items():
            data.setdefault(k, v)
        return data
    except Exception as exc:
        logger.error("load_paper_portfolio failed: %s", exc)
        return _new_paper_portfolio()


def save_paper_portfolio(db, portfolio: dict) -> bool:
    """Persist portfolio state. Returns True on success."""
    if db is None:
        return False
    try:
        portfolio["last_updated"] = datetime.now(BANGKOK_TZ).isoformat()
        db.collection(PAPER_PORTFOLIO_COLLECTION).document(PAPER_PORTFOLIO_DOC).set(portfolio)
        return True
    except Exception as exc:
        logger.error("save_paper_portfolio failed: %s", exc)
        return False


def reset_paper_portfolio(db, starting_cash: float = 1_000_000.0) -> dict:
    """Reset portfolio to starting cash. Wipes positions + history."""
    fresh = _new_paper_portfolio(starting_cash)
    save_paper_portfolio(db, fresh)
    return fresh


def compute_position_size(equity_thb: float, entry: float, stop: float,
                          risk_pct: float = 1.0,
                          max_position_pct: float = 20.0) -> tuple[int, float, float]:
    """Minervini 1%-risk position sizing.

    Returns (shares, cost_thb, at_risk_thb). Returns (0, 0, 0) when the
    stop is at-or-above entry (no valid risk).

    risk_thb        = equity × risk_pct/100
    risk_per_share  = entry - stop
    shares          = floor(risk_thb / risk_per_share)
    cost            = shares × entry
    Cap at max_position_pct% of equity (concentration limit).
    """
    if entry <= 0 or stop <= 0 or entry <= stop or equity_thb <= 0:
        return 0, 0.0, 0.0
    risk_thb = equity_thb * (risk_pct / 100.0)
    risk_per_share = entry - stop
    shares = int(risk_thb / risk_per_share)
    cost = shares * entry
    max_cost = equity_thb * (max_position_pct / 100.0)
    if cost > max_cost > 0:
        shares = int(max_cost / entry)
        cost = shares * entry
    at_risk = shares * risk_per_share
    return shares, cost, at_risk


def portfolio_equity(portfolio: dict, last_prices: dict) -> tuple[float, float]:
    """Compute current equity and unrealized P&L.

    last_prices: {symbol: close_price} from latest scan. Positions held
    in symbols missing from last_prices are valued at entry_price (no
    unrealized change).

    Returns (equity_thb, unrealized_pnl_thb).
    """
    cash = float(portfolio.get("cash_thb") or 0.0)
    unrealized = 0.0
    positions_value = 0.0
    for pos in portfolio.get("positions") or []:
        sym = pos.get("symbol", "")
        shares = int(pos.get("shares") or 0)
        entry = float(pos.get("entry_price") or 0)
        last = float(last_prices.get(sym) or entry)
        positions_value += shares * last
        unrealized += shares * (last - entry)
    return cash + positions_value, unrealized


def save_signal_transitions(prev_signals: list, new_signals: list, db) -> int:
    """Detect sub_stage changes between previous and new scan, append a
    transition record to Firestore `signal_transitions` for each change.

    Each doc is auto-id'd and contains:
      symbol, transitioned_at (= new scan's scanned_at), prev_sub_stage,
      new_sub_stage, prev_close, new_close, prev_scanned_at,
      strength_score, parent_stage_changed (bool).

    Returns the number of transitions written. Both empty/missing
    sub_stage values are treated as "no transition" (suppresses noise
    from old Firestore docs that pre-date the FSM iteration).
    """
    if db is None or not new_signals:
        return 0
    prev_by_sym = {s.symbol: s for s in (prev_signals or [])}
    transitions = []
    scanned_at = new_signals[0].scanned_at if new_signals else ""
    for s in new_signals:
        prev = prev_by_sym.get(s.symbol)
        if prev is None:
            continue  # new symbol — not a transition, just an entry
        prev_sub = (getattr(prev, "sub_stage", "") or "").strip()
        new_sub  = (getattr(s, "sub_stage", "") or "").strip()
        if not prev_sub or not new_sub:
            continue  # one side is empty — skip (Firestore doc lag)
        if prev_sub == new_sub:
            continue  # no change
        transitions.append({
            "symbol":            s.symbol,
            "transitioned_at":   scanned_at,
            "prev_scanned_at":   getattr(prev, "scanned_at", ""),
            "prev_sub_stage":    prev_sub,
            "new_sub_stage":     new_sub,
            "prev_close":        getattr(prev, "close", 0.0),
            "new_close":         s.close,
            "prev_stage":        getattr(prev, "stage", 0),
            "new_stage":         s.stage,
            "parent_stage_changed": getattr(prev, "stage", 0) != s.stage,
            "strength_score":    s.strength_score,
            "pivot_price":       getattr(s, "pivot_price", 0.0),
        })
    if not transitions:
        logger.info("save_signal_transitions: no sub_stage changes detected")
        return 0
    try:
        BATCH_LIMIT = 499
        saved = 0
        for i in range(0, len(transitions), BATCH_LIMIT):
            batch = db.batch()
            for tr in transitions[i:i + BATCH_LIMIT]:
                # Auto-id doc — append-only log
                doc_ref = db.collection("signal_transitions").document()
                batch.set(doc_ref, tr)
            batch.commit()
            saved += len(transitions[i:i + BATCH_LIMIT])
        logger.info("Saved %d signal_transitions to Firestore", saved)
        return saved
    except Exception as exc:
        logger.error("save_signal_transitions failed: %s", exc)
        return 0


def save_breadth_snapshot(breadth, signals: list, scan_type: str = "full",
                          mode: str = "full") -> None:
    """Persist a per-scan breadth + sub-stage count snapshot to BQ
    `breadth_snapshots` table. Enables time-series queries like
    'PIVOT_READY count over the last 30 days' that can't be answered
    from `signals/{symbol}` (Firestore overwrites latest only).

    Called via `await loop.run_in_executor(...)` from /scan so it runs
    synchronously (single-row insert, ~50ms) — fire-and-forget executor
    queueing was unreliable on Cloud Run since the default ThreadPool
    can starve newly-queued tasks when the instance scales down.
    """
    if _bq_client is None or breadth is None:
        return
    from collections import Counter
    sub_counts = Counter(getattr(s, "sub_stage", "") or "" for s in (signals or []))
    row = {
        "scanned_at":     getattr(breadth, "scanned_at", ""),
        "scan_type":      scan_type,
        "mode":           mode,
        "total_stocks":   getattr(breadth, "total_stocks", 0),
        "stage1_count":   getattr(breadth, "stage1_count", 0),
        "stage2_count":   getattr(breadth, "stage2_count", 0),
        "stage3_count":   getattr(breadth, "stage3_count", 0),
        "stage4_count":   getattr(breadth, "stage4_count", 0),
        # Per-sub-stage counts (matches breadth_snapshots schema columns)
        "stage_1_base":         sub_counts.get("STAGE_1_BASE", 0),
        "stage_1_prep":         sub_counts.get("STAGE_1_PREP", 0),
        "stage_2_ignition":     sub_counts.get("STAGE_2_IGNITION", 0),
        "stage_2_overextended": sub_counts.get("STAGE_2_OVEREXTENDED", 0),
        "stage_2_contraction":  sub_counts.get("STAGE_2_CONTRACTION", 0),
        "stage_2_pivot_ready":  sub_counts.get("STAGE_2_PIVOT_READY", 0),
        "stage_2_markup":       sub_counts.get("STAGE_2_MARKUP", 0),
        "stage_3_volatile":     sub_counts.get("STAGE_3_VOLATILE", 0),
        "stage_3_dist_dist":    sub_counts.get("STAGE_3_DIST_DIST", 0),
        "stage_4_breakdown":    sub_counts.get("STAGE_4_BREAKDOWN", 0),
        "stage_4_downtrend":    sub_counts.get("STAGE_4_DOWNTREND", 0),
        "advancing":      getattr(breadth, "advancing", 0),
        "declining":      getattr(breadth, "declining", 0),
        "unchanged":      getattr(breadth, "unchanged", 0),
        "new_highs_52w":  getattr(breadth, "new_highs_52w", 0),
        "new_lows_52w":   getattr(breadth, "new_lows_52w", 0),
        "breakout_count": getattr(breadth, "breakout_count", 0),
        "vcp_count":      getattr(breadth, "vcp_count", 0),
        "above_ma200":    getattr(breadth, "above_ma200", 0),
        "below_ma200":    getattr(breadth, "below_ma200", 0),
        "set_index_close":      getattr(breadth, "set_index_close", 0.0),
        "set_index_change_pct": getattr(breadth, "set_index_change_pct", 0.0),
    }
    table_id = f"{_bq_project}.{_bq_dataset}.breadth_snapshots"
    try:
        errors = _bq_client.insert_rows_json(table_id, [row])
        if errors:
            logger.error("save_breadth_snapshot insert errors: %s", errors[:2])
        else:
            logger.info("Saved breadth_snapshot to BQ at %s", row["scanned_at"])
    except Exception as exc:
        logger.error("save_breadth_snapshot failed: %s", exc)


def load_recent_signal_transitions(db, limit: int = 50, symbol: str = "") -> list:
    """Read most-recent transitions for diagnostic endpoints. Optional
    symbol filter narrows to a single stock's history."""
    if db is None:
        return []
    try:
        from google.cloud import firestore as _fs
        q = db.collection("signal_transitions")
        if symbol:
            q = q.where("symbol", "==", symbol)
        q = q.order_by("transitioned_at", direction=_fs.Query.DESCENDING).limit(limit)
        return [doc.to_dict() for doc in q.stream()]
    except Exception as exc:
        logger.error("load_recent_signal_transitions failed: %s", exc)
        return []


def load_recent_breadth_snapshots(limit: int = 30) -> list:
    """Read recent breadth snapshots from BQ for the time-series
    diagnostic endpoint. Returns most-recent first."""
    if _bq_client is None:
        return []
    table_id = f"{_bq_project}.{_bq_dataset}.breadth_snapshots"
    query = f"""
        SELECT *
        FROM `{table_id}`
        ORDER BY scanned_at DESC
        LIMIT {int(limit)}
    """
    try:
        return [dict(row) for row in _bq_client.query(query).result()]
    except Exception as exc:
        logger.error("load_recent_breadth_snapshots failed: %s", exc)
        return []


def load_signals_from_firestore(db, max_staleness_days: int = 10) -> list:
    """Load latest signals snapshot from Firestore signals collection.

    Filters out orphan docs (symbols whose scans have since stopped producing
    signals — e.g. ACAP became suspended; its Firestore doc isn't auto-deleted
    but scan_stock now rejects it via MAX_CANDLE_STALENESS_DAYS). Without this
    filter, load returns stale entries that leak into _last_signals and get
    classified under bogus patterns.
    """
    if db is None:
        return []
    try:
        import dataclasses
        from datetime import datetime, date
        from analyzer import StockSignal
        valid_fields = {f.name for f in dataclasses.fields(StockSignal)}
        today = date.today()
        docs = db.collection("signals").stream()
        signals = []
        dropped_stale = 0
        for doc in docs:
            try:
                data = {k: v for k, v in doc.to_dict().items() if k in valid_fields and v is not None}
                sig = StockSignal(**data)
                if not sig.symbol:
                    continue
                data_date = getattr(sig, "data_date", "") or ""
                if data_date:
                    try:
                        dd = datetime.strptime(data_date, "%Y-%m-%d").date()
                        if (today - dd).days > max_staleness_days:
                            dropped_stale += 1
                            continue
                    except ValueError:
                        pass  # unparseable date — let it through rather than silent drop
                signals.append(sig)
            except Exception:
                continue
        signals.sort(key=lambda s: s.strength_score, reverse=True)
        logger.info("Loaded %d signals from Firestore (dropped %d stale >%dd)",
                    len(signals), dropped_stale, max_staleness_days)
        return signals
    except Exception as exc:
        logger.error("load_signals_from_firestore failed: %s", exc)
        return []


def load_signal_from_firestore(db, symbol: str, max_staleness_days: int = 10):
    """Load a single signal from Firestore signals/{symbol}. Returns StockSignal or None.

    Mirrors the staleness filter in load_signals_from_firestore: a doc whose
    data_date is older than max_staleness_days is treated as a delisted /
    suspended orphan (the scan-side freshness gate rejected its symbol in the
    most recent scan but the Firestore doc is never auto-deleted).
    """
    if db is None or not symbol:
        return None
    try:
        import dataclasses
        from datetime import datetime, date
        from analyzer import StockSignal
        valid_fields = {f.name for f in dataclasses.fields(StockSignal)}
        doc = db.collection("signals").document(symbol).get()
        if not doc.exists:
            return None
        data = {k: v for k, v in doc.to_dict().items() if k in valid_fields and v is not None}
        sig = StockSignal(**data)
        if not sig.symbol:
            return None
        data_date = getattr(sig, "data_date", "") or ""
        if data_date:
            try:
                dd = datetime.strptime(data_date, "%Y-%m-%d").date()
                if (date.today() - dd).days > max_staleness_days:
                    logger.info("load_signal_from_firestore(%s): dropping stale doc data_date=%s",
                                symbol, data_date)
                    return None
            except ValueError:
                pass
        return sig
    except Exception as exc:
        logger.warning("load_signal_from_firestore(%s) failed: %s", symbol, exc)
        return None


# ─── Performance Review ───────────────────────────────────────────────────────

def log_breakout(db, signal) -> None:
    """Record a Stage-2 breakout entry in Firestore breakout_log/{symbol}."""
    if db is None:
        return
    if signal.stage != 2 or signal.pattern not in ("breakout", "ath_breakout", "vcp"):
        return
    try:
        db.collection("breakout_log").document(signal.symbol).set({
            "symbol": signal.symbol,
            "breakout_price": signal.close,
            "breakout_date": signal.scanned_at[:10],
            "pattern": signal.pattern,
            "logged_at": signal.scanned_at,
        })
    except Exception as exc:
        logger.warning("log_breakout(%s) failed: %s", signal.symbol, exc)


def load_breakout_review(db, current_signals: list) -> list:
    """Return sorted list of dicts: breakout_log joined with current signal price."""
    if db is None:
        return []
    try:
        docs = {d.id: d.to_dict() for d in db.collection("breakout_log").stream()}
    except Exception as exc:
        logger.warning("load_breakout_review failed: %s", exc)
        return []
    sigs_map = {s.symbol: s for s in current_signals}
    rows = []
    for sym, log in docs.items():
        sig = sigs_map.get(sym)
        bp = log.get("breakout_price", 0)
        if sig and bp > 0:
            gain = (sig.close - bp) / bp * 100
            rows.append({
                "symbol": sym, "breakout_price": bp,
                "breakout_date": log.get("breakout_date", ""),
                "pattern": log.get("pattern", ""),
                "current_close": sig.close,
                "gain_pct": gain,
                "current_stage": sig.stage,
            })
    return sorted(rows, key=lambda r: r["gain_pct"], reverse=True)


# ─── Gamification ─────────────────────────────────────────────────────────────

def update_user_score(db, user_id: str, delta: int, reason: str, symbol: str = "") -> None:
    """Increment/decrement user score and append a history entry."""
    if db is None or not user_id:
        return
    try:
        from google.cloud import firestore as _fs
        db.collection("users").document(user_id).set({
            "score": _fs.Increment(delta),
            "score_history": _fs.ArrayUnion([{
                "date": _dt_bangkok()[:10],
                "delta": delta,
                "reason": reason,
                "symbol": symbol,
            }]),
        }, merge=True)
    except Exception as exc:
        logger.warning("update_user_score(%s) failed: %s", user_id, exc)


def increment_stage4_views(db, user_id: str) -> None:
    """Increment stage4_views_this_week counter (reset weekly)."""
    if db is None or not user_id:
        return
    try:
        from google.cloud import firestore as _fs
        import datetime as _dt
        today = _dt.date.today()
        week_start = (today - _dt.timedelta(days=today.weekday())).isoformat()
        ref = db.collection("users").document(user_id)
        data = (ref.get().to_dict() or {})
        stored_week = data.get("stage4_week_start", "")
        if stored_week != week_start:
            ref.set({"stage4_views_this_week": 1, "stage4_week_start": week_start}, merge=True)
        else:
            ref.set({"stage4_views_this_week": _fs.Increment(1)}, merge=True)
    except Exception as exc:
        logger.warning("increment_stage4_views(%s) failed: %s", user_id, exc)


def _dt_bangkok() -> str:
    """Return current Bangkok time as ISO string."""
    import datetime as _dt
    import pytz
    return _dt.datetime.now(pytz.timezone("Asia/Bangkok")).isoformat()


def fetch_fundamentals(symbol: str) -> dict:
    """Fetch fundamental data via yfinance Ticker.info for a SET stock."""
    ticker = _to_yf_ticker(symbol)
    try:
        info = yf.Ticker(ticker).info
        market_cap = info.get("marketCap")
        return {
            "pe_ratio":       info.get("trailingPE"),
            "forward_pe":     info.get("forwardPE"),
            "pb_ratio":       info.get("priceToBook"),
            "dividend_yield": round(info.get("dividendYield", 0) * 100, 2) if info.get("dividendYield") else None,
            "market_cap":     market_cap,
            "market_cap_bn":  round(market_cap / 1e9, 1) if market_cap else None,
            "eps":            info.get("trailingEps"),
            "sector":         info.get("sector"),
            "fetched_at":     datetime.now(BANGKOK_TZ).isoformat(),
        }
    except Exception as exc:
        logger.error("fetch_fundamentals(%s) failed: %s", symbol, exc)
        return {}


def get_cached_fundamentals(symbol: str, db) -> dict:
    """Return Firestore-cached fundamentals only — never fetches fresh from yfinance."""
    if db is None:
        return {}
    try:
        doc = db.collection("fundamentals_cache").document(symbol).get()
        return doc.to_dict() if doc.exists else {}
    except Exception:
        return {}


def get_fundamentals(symbol: str, db=None) -> dict:
    """Return cached fundamentals from Firestore or fetch fresh if >24h old."""
    if db is not None:
        try:
            doc = db.collection("fundamentals_cache").document(symbol).get()
            if doc.exists:
                data = doc.to_dict()
                fetched_at_str = data.get("fetched_at", "")
                if fetched_at_str:
                    fetched_at = datetime.fromisoformat(fetched_at_str)
                    age_hours = (datetime.now(BANGKOK_TZ) - fetched_at).total_seconds() / 3600
                    if age_hours < 24:
                        return data
        except Exception:
            pass

    fund = fetch_fundamentals(symbol)
    if fund and db is not None:
        try:
            db.collection("fundamentals_cache").document(symbol).set(fund)
        except Exception:
            pass
    return fund
