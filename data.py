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
}

# TradingView URLs per index
INDEX_TV_URLS: dict[str, str] = {
    "SET":    "https://www.tradingview.com/chart/?symbol=SET%3ASET",
    "SET50":  "https://www.tradingview.com/chart/?symbol=SET%3ASET50",
    "SET100": "https://www.tradingview.com/chart/?symbol=SET%3ASET100",
    "MAI":    "https://www.tradingview.com/chart/?symbol=SET%3AMAI",
    "sSET":   "https://www.tradingview.com/chart/?symbol=SET%3ASSET",
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


def get_all_symbols() -> list[str]:
    """Return stocks + index symbols."""
    return SET_STOCKS + ["SET"]  # "SET" maps to ^SET.BK


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
        df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
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


def fetch_all_stocks(period: str = "1y") -> dict[str, pd.DataFrame]:
    """
    Fetch OHLCV for all SET_STOCKS + SET index.

    Returns:
        Dict mapping clean symbol → DataFrame.
        Symbols that failed are omitted.
    """
    results: dict[str, pd.DataFrame] = {}
    all_symbols = GET_ALL_SYMBOLS_WITH_INDEX()

    tickers = [("^SET.BK" if s == "SET" else _to_yf_ticker(s)) for s in all_symbols]
    logger.info("Downloading %d tickers from yfinance...", len(tickers))

    try:
        raw = yf.download(
            tickers,
            period=period,
            group_by="ticker",
            progress=False,
            auto_adjust=True,
            threads=True,
        )
    except Exception as exc:
        logger.error("Batch download failed: %s", exc)
        return results

    for symbol, ticker in zip(all_symbols, tickers):
        try:
            if len(tickers) == 1:
                df = raw.copy()
            else:
                df = raw[ticker].copy() if ticker in raw.columns.get_level_values(0) else pd.DataFrame()

            if df.empty or df["Close"].dropna().empty:
                logger.warning("Empty data for %s", symbol)
                continue

            df = df.dropna(subset=["Close"])
            df.index = pd.to_datetime(df.index).tz_localize(None)
            df.index.name = "Date"
            df.columns = [c.replace(" ", "_") for c in df.columns]
            results[symbol] = df
        except Exception as exc:
            logger.warning("Could not process %s: %s", symbol, exc)

    logger.info("Fetched data for %d/%d symbols", len(results), len(all_symbols))
    return results


def GET_ALL_SYMBOLS_WITH_INDEX() -> list[str]:
    return SET_STOCKS + ["SET"]


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


def fetch_indexes(period: str = "5d") -> dict[str, dict]:
    """
    Fetch latest close and daily change% for all major SET indexes.
    Returns {name: {close, change_pct, prev_close}}.
    """
    tickers = list(INDEX_SYMBOLS.values())
    names = list(INDEX_SYMBOLS.keys())
    result: dict[str, dict] = {}
    try:
        raw = yf.download(tickers, period=period, group_by="ticker", progress=False, auto_adjust=True)
        for name, ticker in zip(names, tickers):
            try:
                if len(tickers) == 1:
                    df = raw
                else:
                    df = raw[ticker] if ticker in raw.columns.get_level_values(0) else pd.DataFrame()
                df = df.dropna(subset=["Close"])
                if len(df) < 2:
                    continue
                close = round(float(df["Close"].iloc[-1]), 2)
                prev = round(float(df["Close"].iloc[-2]), 2)
                change_pct = round((close - prev) / prev * 100, 2) if prev else 0.0
                result[name] = {"close": close, "change_pct": change_pct, "prev_close": prev}
            except Exception:
                pass
    except Exception as exc:
        logger.error("fetch_indexes failed: %s", exc)
    return result


def fetch_ohlcv_max(symbol: str) -> Optional[pd.DataFrame]:
    """Fetch maximum available history for a symbol via yfinance (for ATH calculation)."""
    ticker = "^SET.BK" if symbol == "SET" else _to_yf_ticker(symbol)
    try:
        df = yf.download(ticker, period="max", progress=False, auto_adjust=True)
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


def save_signals_to_firestore(signals: list, db) -> None:
    """Batch-write latest scan signals to Firestore signals/{symbol}."""
    if not signals or db is None:
        return
    try:
        batch = db.batch()
        for signal in signals:
            doc_ref = db.collection("signals").document(signal.symbol)
            batch.set(doc_ref, signal.__dict__)
        batch.commit()
        logger.info("Saved %d signals to Firestore", len(signals))
    except Exception as exc:
        logger.error("save_signals_to_firestore failed: %s", exc)


def load_signals_from_firestore(db) -> list:
    """Load latest signals snapshot from Firestore signals collection."""
    if db is None:
        return []
    try:
        from analyzer import StockSignal
        docs = db.collection("signals").stream()
        signals = []
        for doc in docs:
            try:
                signals.append(StockSignal(**doc.to_dict()))
            except Exception:
                continue
        signals.sort(key=lambda s: s.strength_score, reverse=True)
        logger.info("Loaded %d signals from Firestore", len(signals))
        return signals
    except Exception as exc:
        logger.error("load_signals_from_firestore failed: %s", exc)
        return []


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
