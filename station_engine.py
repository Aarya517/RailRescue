"""
station_engine.py — Universal Station Intelligence Engine.
Registry of 50 major IR stations, dynamic NetworkX topology builder,
and RailRadar live board fetcher (with simulated fallback).
"""
import requests
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import networkx as nx

# ─────────────────────────────────────────────────────────────────────────────
# STATION DATABASE — 50 MAJOR IR STATIONS
# corridors: dir -> {label, neighbor, entry_node, block_dist_km, speed_limit_kmh}
# ─────────────────────────────────────────────────────────────────────────────
STATION_DB: Dict[str, Dict] = {
    "NDLS": {
        "name": "New Delhi", "zone": "NR", "state": "DL", "platforms": 16,
        "lat": 28.6448, "lon": 77.2167,
        "corridors": {
            "N": {"label": "Ambala/Chandigarh",   "neighbor": "UMB", "entry_node": "NORTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 130},
            "S": {"label": "Agra/Mathura UP",      "neighbor": "MTJ", "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 160},
            "E": {"label": "Ghaziabad/Kanpur",     "neighbor": "GZB", "entry_node": "EAST_OUTER",  "block_dist_km": 15, "speed_limit_kmh": 130},
            "W": {"label": "Rohtak/Rewari",        "neighbor": "ROK", "entry_node": "WEST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 110},
        }
    },
    "AGC": {
        "name": "Agra Cantt", "zone": "NCR", "state": "UP", "platforms": 6,
        "lat": 27.1551, "lon": 78.0533,
        "corridors": {
            "N": {"label": "Mathura/Delhi UP",   "neighbor": "MTJ", "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 130},
            "S": {"label": "Gwalior/Jhansi DN",  "neighbor": "GWL", "entry_node": "SOUTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "E": {"label": "Etawah/Kanpur",      "neighbor": "ETW", "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 110},
        }
    },
    "GWL": {
        "name": "Gwalior Junction", "zone": "NCR", "state": "MP", "platforms": 6,
        "lat": 26.2163, "lon": 78.1728,
        "corridors": {
            "N": {"label": "Agra/Delhi UP",       "neighbor": "AGC",  "entry_node": "BANMORE_IN",      "block_dist_km": 15, "speed_limit_kmh": 130},
            "S": {"label": "Jhansi/Bhopal DN",    "neighbor": "JHS",  "entry_node": "SITHOULI_IN",     "block_dist_km": 12, "speed_limit_kmh": 130},
            "E": {"label": "Bhind/Etawah Branch", "neighbor": "BTE",  "entry_node": "MALANPUR_BRANCH", "block_dist_km": 10, "speed_limit_kmh": 80},
            "W": {"label": "Guna/Bhopal Branch",  "neighbor": "GUNA", "entry_node": "PANIHAR_BRANCH",  "block_dist_km": 18, "speed_limit_kmh": 100},
        }
    },
    "JHS": {
        "name": "Jhansi Junction", "zone": "NCR", "state": "UP", "platforms": 8,
        "lat": 25.4560, "lon": 78.5680,
        "corridors": {
            "N": {"label": "Gwalior/Agra",    "neighbor": "GWL",  "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 130},
            "S": {"label": "Bina/Bhopal",     "neighbor": "BINA", "entry_node": "SOUTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "E": {"label": "Kanpur/Banda",    "neighbor": "BDA",  "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 110},
            "W": {"label": "Lalitpur/Bhopal", "neighbor": "LAR",  "entry_node": "WEST_OUTER",  "block_dist_km": 15, "speed_limit_kmh": 100},
        }
    },
    "CNB": {
        "name": "Kanpur Central", "zone": "NCR", "state": "UP", "platforms": 10,
        "lat": 26.4499, "lon": 80.3319,
        "corridors": {
            "N": {"label": "Lucknow/Moradabad",    "neighbor": "LKO", "entry_node": "NORTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "S": {"label": "Jhansi/Agra",          "neighbor": "JHS", "entry_node": "SOUTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 130},
            "E": {"label": "Allahabad/Varanasi",   "neighbor": "ALD", "entry_node": "EAST_OUTER",  "block_dist_km": 15, "speed_limit_kmh": 130},
            "W": {"label": "Etawah/Agra",          "neighbor": "ETW", "entry_node": "WEST_OUTER",  "block_dist_km": 14, "speed_limit_kmh": 110},
        }
    },
    "LKO": {
        "name": "Lucknow NR", "zone": "NR", "state": "UP", "platforms": 8,
        "lat": 26.8467, "lon": 80.9462,
        "corridors": {
            "N": {"label": "Moradabad/Delhi",    "neighbor": "MB",  "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "S": {"label": "Kanpur Central",     "neighbor": "CNB", "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "E": {"label": "Barabanki/Varanasi", "neighbor": "BBK", "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 110},
        }
    },
    "BSB": {
        "name": "Varanasi Junction", "zone": "NER", "state": "UP", "platforms": 9,
        "lat": 25.3176, "lon": 82.9739,
        "corridors": {
            "N": {"label": "Jaunpur/Lucknow",    "neighbor": "JNU", "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 110},
            "S": {"label": "Mughal Sarai/Patna", "neighbor": "MGS", "entry_node": "SOUTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 130},
            "E": {"label": "Allahabad/Patna",    "neighbor": "ALD", "entry_node": "EAST_OUTER",  "block_dist_km": 16, "speed_limit_kmh": 130},
        }
    },
    "GKP": {
        "name": "Gorakhpur Junction", "zone": "NER", "state": "UP", "platforms": 10,
        "lat": 26.7606, "lon": 83.3732,
        "corridors": {
            "S": {"label": "Varanasi/Patna",  "neighbor": "BSB", "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "E": {"label": "Gonda/Lucknow",   "neighbor": "GD",  "entry_node": "EAST_OUTER",  "block_dist_km": 14, "speed_limit_kmh": 110},
        }
    },
    "HWH": {
        "name": "Howrah Junction", "zone": "ER", "state": "WB", "platforms": 23,
        "lat": 22.5839, "lon": 88.3431,
        "corridors": {
            "N": {"label": "Bandel/Bardhaman UP",   "neighbor": "BDC",  "entry_node": "NORTH_OUTER", "block_dist_km": 15, "speed_limit_kmh": 130},
            "S": {"label": "Santragachi/Kharagpur", "neighbor": "SRC",  "entry_node": "SOUTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 130},
            "E": {"label": "Sealdah/Belur",         "neighbor": "SDAH", "entry_node": "EAST_OUTER",  "block_dist_km": 8,  "speed_limit_kmh": 80},
            "W": {"label": "Jharkhand/Patna",       "neighbor": "DHN",  "entry_node": "WEST_OUTER",  "block_dist_km": 20, "speed_limit_kmh": 130},
        }
    },
    "SDAH": {
        "name": "Sealdah", "zone": "ER", "state": "WB", "platforms": 13,
        "lat": 22.5646, "lon": 88.3701,
        "corridors": {
            "N": {"label": "Dankuni/Bardhaman",        "neighbor": "DKA", "entry_node": "NORTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 110},
            "S": {"label": "Sonarpur/Lakshmikantapur", "neighbor": "SPR", "entry_node": "SOUTH_OUTER", "block_dist_km": 10, "speed_limit_kmh": 100},
            "E": {"label": "Krishnanagar/Bangaon",     "neighbor": "KNJ", "entry_node": "EAST_OUTER",  "block_dist_km": 14, "speed_limit_kmh": 110},
        }
    },
    "PNBE": {
        "name": "Patna Junction", "zone": "ECR", "state": "BR", "platforms": 10,
        "lat": 25.5941, "lon": 85.1376,
        "corridors": {
            "N": {"label": "Hajipur/Muzaffarpur",   "neighbor": "HJP", "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 130},
            "S": {"label": "Gaya/Mughal Sarai",     "neighbor": "GAYA","entry_node": "SOUTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "E": {"label": "Bakhtiyarpur/Bhagalpur","neighbor": "BKP", "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 130},
            "W": {"label": "Ara/Buxar",             "neighbor": "ARA", "entry_node": "WEST_OUTER",  "block_dist_km": 15, "speed_limit_kmh": 130},
        }
    },
    "MGS": {
        "name": "Pt. DDU Junction", "zone": "ECR", "state": "UP", "platforms": 8,
        "lat": 25.2789, "lon": 83.1196,
        "corridors": {
            "N": {"label": "Varanasi/Lucknow", "neighbor": "BSB",  "entry_node": "NORTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 130},
            "E": {"label": "Patna/Howrah",     "neighbor": "PNBE", "entry_node": "EAST_OUTER",  "block_dist_km": 18, "speed_limit_kmh": 130},
            "W": {"label": "Allahabad/Agra",   "neighbor": "ALD",  "entry_node": "WEST_OUTER",  "block_dist_km": 20, "speed_limit_kmh": 130},
        }
    },
    "ADI": {
        "name": "Ahmedabad Junction", "zone": "WR", "state": "GJ", "platforms": 9,
        "lat": 23.0225, "lon": 72.5714,
        "corridors": {
            "N": {"label": "Palanpur/Delhi",    "neighbor": "PNU", "entry_node": "NORTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 130},
            "S": {"label": "Vadodara/Mumbai",   "neighbor": "BRC", "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "E": {"label": "Gandhinagar/Kalol", "neighbor": "GNC", "entry_node": "EAST_OUTER",  "block_dist_km": 10, "speed_limit_kmh": 110},
        }
    },
    "BRC": {
        "name": "Vadodara Junction", "zone": "WR", "state": "GJ", "platforms": 7,
        "lat": 22.3072, "lon": 73.1812,
        "corridors": {
            "N": {"label": "Ahmedabad/Delhi", "neighbor": "ADI", "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "S": {"label": "Surat/Mumbai",    "neighbor": "ST",  "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "E": {"label": "Ratlam/Indore",   "neighbor": "RTM", "entry_node": "EAST_OUTER",  "block_dist_km": 20, "speed_limit_kmh": 110},
        }
    },
    "BCT": {
        "name": "Mumbai Central", "zone": "WR", "state": "MH", "platforms": 7,
        "lat": 18.9676, "lon": 72.8197,
        "corridors": {
            "N": {"label": "Borivali/Surat/Vadodara", "neighbor": "ST",  "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "S": {"label": "Bandra/Church Gate",      "neighbor": "BVI", "entry_node": "SOUTH_OUTER", "block_dist_km": 6,  "speed_limit_kmh": 80},
        }
    },
    "RTM": {
        "name": "Ratlam Junction", "zone": "WR", "state": "MP", "platforms": 6,
        "lat": 23.3315, "lon": 75.0367,
        "corridors": {
            "N": {"label": "Kota/Delhi",       "neighbor": "KOTA", "entry_node": "NORTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "S": {"label": "Vadodara/Mumbai",  "neighbor": "BRC",  "entry_node": "SOUTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 130},
            "E": {"label": "Ujjain/Bhopal",    "neighbor": "UJN",  "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 110},
        }
    },
    "CSTM": {
        "name": "Mumbai CST", "zone": "CR", "state": "MH", "platforms": 18,
        "lat": 18.9402, "lon": 72.8356,
        "corridors": {
            "N": {"label": "Thane/Kalyan", "neighbor": "TNA",   "entry_node": "NORTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 110},
            "E": {"label": "Kurla/Pune",   "neighbor": "KURLA", "entry_node": "EAST_OUTER",  "block_dist_km": 10, "speed_limit_kmh": 110},
        }
    },
    "NGP": {
        "name": "Nagpur Junction", "zone": "CR", "state": "MH", "platforms": 8,
        "lat": 21.1458, "lon": 79.0882,
        "corridors": {
            "N": {"label": "Bhopal/Delhi",        "neighbor": "BPL", "entry_node": "NORTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 130},
            "S": {"label": "Wardha/Secunderabad", "neighbor": "WRD", "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "E": {"label": "Chandrapur/Balharshah","neighbor": "BPQ","entry_node": "EAST_OUTER",  "block_dist_km": 15, "speed_limit_kmh": 110},
            "W": {"label": "Amravati/Akola",      "neighbor": "AMI", "entry_node": "WEST_OUTER",  "block_dist_km": 16, "speed_limit_kmh": 110},
        }
    },
    "SUR": {
        "name": "Solapur Junction", "zone": "CR", "state": "MH", "platforms": 5,
        "lat": 17.6805, "lon": 75.9064,
        "corridors": {
            "N": {"label": "Pune/Mumbai",         "neighbor": "PUNE", "entry_node": "NORTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "S": {"label": "Gulbarga/Secunderabad","neighbor": "GR",   "entry_node": "SOUTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
        }
    },
    "SC": {
        "name": "Secunderabad Junction", "zone": "SCR", "state": "TS", "platforms": 10,
        "lat": 17.4344, "lon": 78.5013,
        "corridors": {
            "N": {"label": "Kazipet/Nagpur",       "neighbor": "KZJ", "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "S": {"label": "Hyderabad/Kachiguda",  "neighbor": "HYB", "entry_node": "SOUTH_OUTER", "block_dist_km": 4,  "speed_limit_kmh": 60},
            "W": {"label": "Lingampally/Bidar",    "neighbor": "LPI", "entry_node": "WEST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 110},
        }
    },
    "BZA": {
        "name": "Vijayawada Junction", "zone": "SCR", "state": "AP", "platforms": 10,
        "lat": 16.5167, "lon": 80.6167,
        "corridors": {
            "N": {"label": "Warangal/Secunderabad", "neighbor": "WL",  "entry_node": "NORTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "S": {"label": "Nellore/Chennai",       "neighbor": "NLR", "entry_node": "SOUTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 130},
            "E": {"label": "Eluru/Rajahmundry",     "neighbor": "EE",  "entry_node": "EAST_OUTER",  "block_dist_km": 14, "speed_limit_kmh": 110},
            "W": {"label": "Guntur/Tenali",         "neighbor": "GNT", "entry_node": "WEST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 110},
        }
    },
    "VSKP": {
        "name": "Visakhapatnam Jn", "zone": "ECoR", "state": "AP", "platforms": 6,
        "lat": 17.7231, "lon": 83.2949,
        "corridors": {
            "N": {"label": "Bhubaneswar/Howrah",       "neighbor": "BBS", "entry_node": "NORTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 130},
            "S": {"label": "Rajahmundry/Vijayawada",   "neighbor": "RJY", "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
        }
    },
    "MAS": {
        "name": "Chennai Central", "zone": "SR", "state": "TN", "platforms": 17,
        "lat": 13.0827, "lon": 80.2707,
        "corridors": {
            "N": {"label": "Perambur/Arakkonam",  "neighbor": "AJJ", "entry_node": "NORTH_OUTER", "block_dist_km": 15, "speed_limit_kmh": 130},
            "S": {"label": "Tambaram/Villupuram", "neighbor": "TBM", "entry_node": "SOUTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 110},
            "W": {"label": "Katpadi/Jolarpettai", "neighbor": "KPD", "entry_node": "WEST_OUTER",  "block_dist_km": 18, "speed_limit_kmh": 130},
        }
    },
    "CBE": {
        "name": "Coimbatore Junction", "zone": "SR", "state": "TN", "platforms": 5,
        "lat": 11.0003, "lon": 76.9628,
        "corridors": {
            "N": {"label": "Erode/Salem",      "neighbor": "ED",  "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 110},
            "S": {"label": "Palakkad/Shoranur","neighbor": "PGT", "entry_node": "SOUTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 110},
            "E": {"label": "Tiruppur/Erode",   "neighbor": "TUP", "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 100},
        }
    },
    "SBC": {
        "name": "KSR Bengaluru City", "zone": "SWR", "state": "KA", "platforms": 10,
        "lat": 12.9789, "lon": 77.5713,
        "corridors": {
            "N": {"label": "Tumkur/Hubli/Pune",    "neighbor": "TK",  "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "S": {"label": "Krishnarajapuram/Mys", "neighbor": "KJM", "entry_node": "SOUTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 110},
            "E": {"label": "Whitefield/Chennai",   "neighbor": "WFD", "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 130},
        }
    },
    "JP": {
        "name": "Jaipur Junction", "zone": "NWR", "state": "RJ", "platforms": 6,
        "lat": 26.9124, "lon": 75.7873,
        "corridors": {
            "N": {"label": "Ringas/Delhi",          "neighbor": "RIA", "entry_node": "NORTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "S": {"label": "Ajmer/Ahmedabad",       "neighbor": "AII", "entry_node": "SOUTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 130},
            "E": {"label": "Bandikui/Sawai Madhopur","neighbor": "BKI","entry_node": "EAST_OUTER",  "block_dist_km": 14, "speed_limit_kmh": 110},
        }
    },
    "JU": {
        "name": "Jodhpur Junction", "zone": "NWR", "state": "RJ", "platforms": 5,
        "lat": 26.2389, "lon": 73.0243,
        "corridors": {
            "N": {"label": "Marwar/Ajmer","neighbor": "MWR", "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 110},
            "E": {"label": "Rani/Jaipur", "neighbor": "JP",  "entry_node": "EAST_OUTER",  "block_dist_km": 20, "speed_limit_kmh": 130},
        }
    },
    "BSP": {
        "name": "Bilaspur Junction", "zone": "SECR", "state": "CG", "platforms": 7,
        "lat": 22.0796, "lon": 82.1391,
        "corridors": {
            "N": {"label": "Katni/Jabalpur",    "neighbor": "KTE", "entry_node": "NORTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 110},
            "S": {"label": "Raipur/Nagpur",     "neighbor": "R",   "entry_node": "SOUTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "E": {"label": "Jharsuguda/Rourkela","neighbor": "JSG", "entry_node": "EAST_OUTER",  "block_dist_km": 18, "speed_limit_kmh": 130},
        }
    },
    "BBS": {
        "name": "Bhubaneswar", "zone": "ECoR", "state": "OR", "platforms": 6,
        "lat": 20.2636, "lon": 85.8236,
        "corridors": {
            "N": {"label": "Cuttack/Visakhapatnam", "neighbor": "CTC", "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 130},
            "S": {"label": "Khordha/Berhampur",     "neighbor": "KUR", "entry_node": "SOUTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 110},
            "W": {"label": "Sambalpur/Bilaspur",    "neighbor": "SBP", "entry_node": "WEST_OUTER",  "block_dist_km": 16, "speed_limit_kmh": 110},
        }
    },
    "JBP": {
        "name": "Jabalpur Junction", "zone": "WCR", "state": "MP", "platforms": 6,
        "lat": 23.1815, "lon": 79.9864,
        "corridors": {
            "N": {"label": "Katni/Satna/Manikpur", "neighbor": "KTE", "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 110},
            "S": {"label": "Narsinghpur/Nagpur",   "neighbor": "NP",  "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "W": {"label": "Sagar/Bhopal",         "neighbor": "BPL", "entry_node": "WEST_OUTER",  "block_dist_km": 20, "speed_limit_kmh": 110},
        }
    },
    "BPL": {
        "name": "Bhopal Junction", "zone": "WCR", "state": "MP", "platforms": 6,
        "lat": 23.2699, "lon": 77.4048,
        "corridors": {
            "N": {"label": "Vidisha/Jhansi/Delhi", "neighbor": "VDA", "entry_node": "NORTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "S": {"label": "Itarsi/Nagpur",        "neighbor": "ET",  "entry_node": "SOUTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "E": {"label": "Sagar/Jabalpur",       "neighbor": "SGO", "entry_node": "EAST_OUTER",  "block_dist_km": 20, "speed_limit_kmh": 110},
        }
    },
    "KOTA": {
        "name": "Kota Junction", "zone": "WCR", "state": "RJ", "platforms": 5,
        "lat": 25.1802, "lon": 75.8333,
        "corridors": {
            "N": {"label": "Sawai Madhopur/Jaipur", "neighbor": "SWM",   "entry_node": "NORTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "S": {"label": "Nagda/Ratlam/Mumbai",   "neighbor": "NAGDA", "entry_node": "SOUTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 130},
        }
    },
    "MTJ": {
        "name": "Mathura Junction", "zone": "NCR", "state": "UP", "platforms": 8,
        "lat": 27.4924, "lon": 77.6737,
        "corridors": {
            "N": {"label": "Faridabad/Delhi","neighbor": "NDLS","entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 160},
            "S": {"label": "Agra/Gwalior",  "neighbor": "AGC", "entry_node": "SOUTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 130},
            "E": {"label": "Hathras/Tundla","neighbor": "HRS", "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 110},
            "W": {"label": "Bharatpur/JP",  "neighbor": "BTE", "entry_node": "WEST_OUTER",  "block_dist_km": 15, "speed_limit_kmh": 130},
        }
    },
    "ALD": {
        "name": "Prayagraj Junction", "zone": "NCR", "state": "UP", "platforms": 10,
        "lat": 25.4358, "lon": 81.8463,
        "corridors": {
            "N": {"label": "Lucknow/Kanpur",    "neighbor": "CNB", "entry_node": "NORTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "E": {"label": "Mughal Sarai/Patna","neighbor": "MGS", "entry_node": "EAST_OUTER",  "block_dist_km": 14, "speed_limit_kmh": 130},
            "W": {"label": "Manikpur/Satna",    "neighbor": "MKP", "entry_node": "WEST_OUTER",  "block_dist_km": 15, "speed_limit_kmh": 110},
        }
    },
    "DHN": {
        "name": "Dhanbad Junction", "zone": "ECR", "state": "JH", "platforms": 5,
        "lat": 23.7946, "lon": 86.4304,
        "corridors": {
            "N": {"label": "Asansol/Howrah",   "neighbor": "ASN", "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 130},
            "S": {"label": "Gomoh/Bokaro",     "neighbor": "GMO", "entry_node": "SOUTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 110},
            "W": {"label": "Giridih/Hazaribagh","neighbor": "GRD","entry_node": "WEST_OUTER",  "block_dist_km": 10, "speed_limit_kmh": 80},
        }
    },
    "GAYA": {
        "name": "Gaya Junction", "zone": "ECR", "state": "BR", "platforms": 7,
        "lat": 24.7957, "lon": 85.0002,
        "corridors": {
            "N": {"label": "Patna Junction",    "neighbor": "PNBE", "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "S": {"label": "Sasaram/Dehri",     "neighbor": "SSM",  "entry_node": "SOUTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 110},
            "E": {"label": "Koderma/Hazaribagh","neighbor": "KQR",  "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 100},
        }
    },
    "ST": {
        "name": "Surat", "zone": "WR", "state": "GJ", "platforms": 7,
        "lat": 21.2043, "lon": 72.8373,
        "corridors": {
            "N": {"label": "Bharuch/Vadodara", "neighbor": "BH", "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "S": {"label": "Valsad/Mumbai",    "neighbor": "BL", "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
        }
    },
    "BSL": {
        "name": "Bhusaval Junction", "zone": "CR", "state": "MH", "platforms": 4,
        "lat": 21.0432, "lon": 75.7816,
        "corridors": {
            "N": {"label": "Burhanpur/Khandwa","neighbor": "BAU", "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 110},
            "S": {"label": "Nashik/Mumbai",   "neighbor": "NK",  "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "E": {"label": "Akola/Nagpur",    "neighbor": "AK",  "entry_node": "EAST_OUTER",  "block_dist_km": 20, "speed_limit_kmh": 130},
        }
    },
    "HYB": {
        "name": "Hyderabad Deccan", "zone": "SCR", "state": "TS", "platforms": 6,
        "lat": 17.3850, "lon": 78.4867,
        "corridors": {
            "N": {"label": "Secunderabad/Nagpur","neighbor": "SC",  "entry_node": "NORTH_OUTER", "block_dist_km": 6,  "speed_limit_kmh": 60},
            "S": {"label": "Kachiguda/Gulbarga", "neighbor": "KCG", "entry_node": "SOUTH_OUTER", "block_dist_km": 8,  "speed_limit_kmh": 80},
        }
    },
    "WL": {
        "name": "Warangal", "zone": "SCR", "state": "TS", "platforms": 4,
        "lat": 17.9784, "lon": 79.5941,
        "corridors": {
            "N": {"label": "Kazipet/Secunderabad","neighbor": "KZJ", "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 130},
            "S": {"label": "Vijayawada/Chennai",  "neighbor": "BZA", "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
        }
    },
    "GNT": {
        "name": "Guntur Junction", "zone": "SCR", "state": "AP", "platforms": 6,
        "lat": 16.3067, "lon": 80.4365,
        "corridors": {
            "N": {"label": "Vijayawada",      "neighbor": "BZA", "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 110},
            "S": {"label": "Ongole/Nellore",  "neighbor": "OGL", "entry_node": "SOUTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 110},
        }
    },
    "PURI": {
        "name": "Puri", "zone": "ECoR", "state": "OR", "platforms": 5,
        "lat": 19.8135, "lon": 85.8312,
        "corridors": {
            "N": {"label": "Khordha/Bhubaneswar","neighbor": "KUR","entry_node": "NORTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 110},
        }
    },
    "ED": {
        "name": "Erode Junction", "zone": "SR", "state": "TN", "platforms": 4,
        "lat": 11.3410, "lon": 77.7172,
        "corridors": {
            "N": {"label": "Salem/Chennai",         "neighbor": "SA",  "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "S": {"label": "Coimbatore/TVC",        "neighbor": "CBE", "entry_node": "SOUTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 110},
            "E": {"label": "Tiruchirapalli/Madurai","neighbor": "TPJ", "entry_node": "EAST_OUTER",  "block_dist_km": 18, "speed_limit_kmh": 110},
        }
    },
    "MYS": {
        "name": "Mysuru Junction", "zone": "SWR", "state": "KA", "platforms": 5,
        "lat": 12.3052, "lon": 76.6551,
        "corridors": {
            "N": {"label": "Mandya/Bangalore","neighbor": "MYA", "entry_node": "NORTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 110},
            "W": {"label": "Hassan/Mangalore","neighbor": "HAS", "entry_node": "WEST_OUTER",  "block_dist_km": 16, "speed_limit_kmh": 100},
        }
    },
    "AII": {
        "name": "Ajmer Junction", "zone": "NWR", "state": "RJ", "platforms": 4,
        "lat": 26.4499, "lon": 74.6399,
        "corridors": {
            "N": {"label": "Jaipur/Delhi",    "neighbor": "JP",  "entry_node": "NORTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
            "S": {"label": "Marwar/Jodhpur",  "neighbor": "MWR", "entry_node": "SOUTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 110},
            "E": {"label": "Kishangarh",      "neighbor": "KSG", "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 110},
        }
    },
    "R": {
        "name": "Raipur Junction", "zone": "SECR", "state": "CG", "platforms": 7,
        "lat": 21.2514, "lon": 81.6296,
        "corridors": {
            "N": {"label": "Bilaspur/Jharsuguda","neighbor": "BSP",  "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 130},
            "S": {"label": "Durg/Nagpur",        "neighbor": "DURG", "entry_node": "SOUTH_OUTER", "block_dist_km": 18, "speed_limit_kmh": 130},
        }
    },
    "MFP": {
        "name": "Muzaffarpur Junction", "zone": "ECR", "state": "BR", "platforms": 5,
        "lat": 26.1209, "lon": 85.3647,
        "corridors": {
            "N": {"label": "Sitamarhi/Raxaul",      "neighbor": "SMI", "entry_node": "NORTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 80},
            "S": {"label": "Hajipur/Patna",         "neighbor": "HJP", "entry_node": "SOUTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 110},
            "E": {"label": "Darbhanga/Samastipur",  "neighbor": "DBG", "entry_node": "EAST_OUTER",  "block_dist_km": 10, "speed_limit_kmh": 80},
        }
    },
    "MB": {
        "name": "Moradabad Junction", "zone": "NR", "state": "UP", "platforms": 7,
        "lat": 28.8386, "lon": 78.7733,
        "corridors": {
            "N": {"label": "Rampur/Bareilly",    "neighbor": "BE",  "entry_node": "NORTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 110},
            "S": {"label": "Ghaziabad/Delhi",    "neighbor": "GZB", "entry_node": "SOUTH_OUTER", "block_dist_km": 20, "speed_limit_kmh": 130},
            "E": {"label": "Kashipur/Haldwani",  "neighbor": "KPV", "entry_node": "EAST_OUTER",  "block_dist_km": 12, "speed_limit_kmh": 100},
        }
    },
    "TVC": {
        "name": "Thiruvananthapuram Central", "zone": "SR", "state": "KL", "platforms": 6,
        "lat": 8.4855, "lon": 76.9492,
        "corridors": {
            "N": {"label": "Kollam/Ernakulam","neighbor": "QLN",  "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 110},
            "S": {"label": "Kanyakumari",     "neighbor": "CAPE", "entry_node": "SOUTH_OUTER", "block_dist_km": 8,  "speed_limit_kmh": 80},
        }
    },
    "ERS": {
        "name": "Ernakulam Junction", "zone": "SR", "state": "KL", "platforms": 6,
        "lat": 9.9816, "lon": 76.2999,
        "corridors": {
            "N": {"label": "Thrissur/Shoranur",           "neighbor": "TCR", "entry_node": "NORTH_OUTER", "block_dist_km": 14, "speed_limit_kmh": 110},
            "S": {"label": "Kollam/Thiruvananthapuram",   "neighbor": "QLN", "entry_node": "SOUTH_OUTER", "block_dist_km": 16, "speed_limit_kmh": 110},
            "E": {"label": "Aluva/Angamaly",              "neighbor": "AWY", "entry_node": "EAST_OUTER",  "block_dist_km": 10, "speed_limit_kmh": 80},
        }
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# AUTHENTIC REAL-WORLD TRAIN SCHEDULES PER STATION
# ─────────────────────────────────────────────────────────────────────────────
STATION_TRAIN_SCHEDULES: Dict[str, List[Dict]] = {
    "GWL": [
        {"no": "12002", "name": "Bhopal Shatabdi Express",      "tier": 2, "mps": 150, "mass": 450,  "pax": 1100},
        {"no": "22470", "name": "Khajuraho Vande Bharat Exp",   "tier": 2, "mps": 160, "mass": 430,  "pax": 1128},
        {"no": "12280", "name": "Taj Express Superfast",        "tier": 4, "mps": 110, "mass": 820,  "pax": 1700},
        {"no": "12616", "name": "Grand Trunk (GT) Express",     "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
        {"no": "12618", "name": "Mangala Lakshadweep Exp",      "tier": 4, "mps": 110, "mass": 860,  "pax": 1850},
        {"no": "12920", "name": "Malwa SF Express",             "tier": 4, "mps": 110, "mass": 840,  "pax": 1750},
        {"no": "12138", "name": "Punjab Mail",                  "tier": 5, "mps": 100, "mass": 800,  "pax": 1600},
        {"no": "11842", "name": "Gita Jayanti Express",         "tier": 5, "mps": 100, "mass": 780,  "pax": 1500},
        {"no": "11124", "name": "Gwalior - Barauni Mail",       "tier": 5, "mps": 100, "mass": 760,  "pax": 1400},
        {"no": "41502", "name": "NCR Container Freight Rake",   "tier": 7, "mps": 75,  "mass": 3800, "pax": 0},
    ],
    "NDLS": [
        {"no": "12301", "name": "Howrah Rajdhani Express",      "tier": 2, "mps": 130, "mass": 520,  "pax": 1200},
        {"no": "12952", "name": "Mumbai Rajdhani Express",      "tier": 2, "mps": 130, "mass": 520,  "pax": 1200},
        {"no": "22439", "name": "Varanasi Vande Bharat Exp",    "tier": 2, "mps": 160, "mass": 430,  "pax": 1128},
        {"no": "12004", "name": "Lucknow Shatabdi Express",     "tier": 2, "mps": 150, "mass": 450,  "pax": 1100},
        {"no": "12002", "name": "Bhopal Shatabdi Express",      "tier": 2, "mps": 150, "mass": 450,  "pax": 1100},
        {"no": "12418", "name": "Prayagraj Express",            "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
        {"no": "14311", "name": "Ala Hazrat / Bareilly Exp",    "tier": 5, "mps": 100, "mass": 780,  "pax": 1500},
        {"no": "12430", "name": "Lucknow AC Superfast",         "tier": 4, "mps": 110, "mass": 820,  "pax": 1600},
    ],
    "CNB": [
        {"no": "22436", "name": "Vande Bharat Express",         "tier": 2, "mps": 160, "mass": 430,  "pax": 1128},
        {"no": "12418", "name": "Prayagraj Express",            "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
        {"no": "12419", "name": "Gomti Express Superfast",      "tier": 4, "mps": 110, "mass": 800,  "pax": 1700},
        {"no": "12302", "name": "Kolkata Rajdhani Express",     "tier": 2, "mps": 130, "mass": 520,  "pax": 1200},
        {"no": "12802", "name": "Purushottam Express",          "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
        {"no": "40106", "name": "DFC Freight Container Rake",   "tier": 7, "mps": 75,  "mass": 4200, "pax": 0},
    ],
    "JBP": [
        {"no": "20174", "name": "Vande Bharat Express (Rewa)",  "tier": 2, "mps": 160, "mass": 430,  "pax": 1128},
        {"no": "12192", "name": "Shridham Superfast Express",   "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
        {"no": "12062", "name": "Jabalpur Janshatabdi Exp",     "tier": 4, "mps": 110, "mass": 800,  "pax": 1650},
        {"no": "12189", "name": "Mahakaushal Express",          "tier": 5, "mps": 100, "mass": 780,  "pax": 1500},
        {"no": "12294", "name": "Duronto Express (LTT-ALD)",    "tier": 2, "mps": 130, "mass": 520,  "pax": 1200},
    ],
    "MAS": [
        {"no": "20643", "name": "Coimbatore Vande Bharat Exp",  "tier": 2, "mps": 160, "mass": 430,  "pax": 1128},
        {"no": "12622", "name": "Tamil Nadu Express",           "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
        {"no": "12626", "name": "Kerala Express",               "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
        {"no": "12007", "name": "Mysuru Shatabdi Express",      "tier": 2, "mps": 150, "mass": 450,  "pax": 1100},
        {"no": "12616", "name": "Grand Trunk (GT) Express",     "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
    ],
    "HWH": [
        {"no": "12301", "name": "Howrah Rajdhani Express",      "tier": 2, "mps": 130, "mass": 520,  "pax": 1200},
        {"no": "22301", "name": "NJP Vande Bharat Express",     "tier": 2, "mps": 160, "mass": 430,  "pax": 1128},
        {"no": "12860", "name": "Gitanjali Express",            "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
        {"no": "12840", "name": "Howrah Mail",                  "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
        {"no": "12019", "name": "Ranchi Shatabdi Express",      "tier": 2, "mps": 150, "mass": 450,  "pax": 1100},
    ],
}

# Generic fallback pool for unlisted stations
TRAIN_POOL = [
    {"no": "12952", "name": "Mumbai Rajdhani Exp",  "tier": 2, "mps": 130, "mass": 520,  "pax": 1200},
    {"no": "12301", "name": "Howrah Rajdhani Exp",  "tier": 2, "mps": 130, "mass": 520,  "pax": 1200},
    {"no": "22691", "name": "Rajdhani Express",     "tier": 2, "mps": 130, "mass": 520,  "pax": 1200},
    {"no": "12430", "name": "Rajdhani Express",     "tier": 2, "mps": 130, "mass": 520,  "pax": 1200},
    {"no": "22439", "name": "Vande Bharat Express", "tier": 2, "mps": 160, "mass": 430,  "pax": 1128},
    {"no": "12001", "name": "Bhopal Shatabdi Exp",  "tier": 2, "mps": 150, "mass": 450,  "pax": 1100},
    {"no": "12616", "name": "Grand Trunk (GT) Exp", "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
    {"no": "12627", "name": "Karnataka Express",    "tier": 4, "mps": 110, "mass": 850,  "pax": 1800},
    {"no": "12419", "name": "Gomti Express SF",     "tier": 4, "mps": 110, "mass": 800,  "pax": 1700},
    {"no": "12138", "name": "Punjab Mail",          "tier": 5, "mps": 100, "mass": 800,  "pax": 1600},
    {"no": "14311", "name": "Bareilly Express",     "tier": 5, "mps": 100, "mass": 780,  "pax": 1500},
    {"no": "41502", "name": "BOXN Goods Train",     "tier": 7, "mps": 75,  "mass": 3800, "pax": 0},
]


DIR_TO_CORRIDOR = {
    "N": "NORTH_CORRIDOR", "S": "SOUTH_CORRIDOR",
    "E": "EAST_BRANCH",    "W": "WEST_BRANCH",
    "NE":"NORTH_CORRIDOR", "SW":"SOUTH_CORRIDOR",
    "NW":"WEST_BRANCH",    "SE":"EAST_BRANCH",
}
TIER_COLORS = {2: "#f43f5e", 4: "#38bdf8", 5: "#a78bfa", 7: "#fb923c"}


def _default_station(code: str) -> Dict:
    return {
        "name": f"{code.upper()} Station", "zone": "IR", "state": "India", "platforms": 4,
        "lat": 22.5726, "lon": 88.3639,
        "corridors": {
            "N": {"label": "North Approach", "neighbor": "N_OUT", "entry_node": "NORTH_OUTER", "block_dist_km": 15, "speed_limit_kmh": 110},
            "S": {"label": "South Approach", "neighbor": "S_OUT", "entry_node": "SOUTH_OUTER", "block_dist_km": 12, "speed_limit_kmh": 110},
        }
    }


def get_station(code: str) -> Dict:
    return STATION_DB.get(code.upper(), _default_station(code))


def build_station_graph(station_code: str) -> nx.DiGraph:
    """Builds a directed NetworkX topology graph for any station."""
    station = get_station(station_code)
    G = nx.DiGraph()
    pf_count = station["platforms"]
    for pf in range(1, pf_count + 1):
        G.add_node(f"PF_{pf}", type="PLATFORM", length_m=650, max_speed_kmh=30.0)
    G.add_node("NORTH_THROAT", type="INTERLOCKING", max_speed_kmh=30.0)
    G.add_node("SOUTH_THROAT", type="INTERLOCKING", max_speed_kmh=30.0)
    for pf in range(1, pf_count + 1):
        G.add_edge("NORTH_THROAT", f"PF_{pf}", length_m=800, speed_limit_kmh=30)
        G.add_edge(f"PF_{pf}", "SOUTH_THROAT", length_m=800, speed_limit_kmh=30)
    for direction, corridor in station["corridors"].items():
        entry = corridor["entry_node"]
        dist_m = corridor["block_dist_km"] * 1000
        spd = corridor["speed_limit_kmh"]
        G.add_node(entry, type="BOUNDARY", max_speed_kmh=spd)
        if direction in ("N", "NE", "NW"):
            G.add_edge(entry, "NORTH_THROAT", length_m=dist_m, speed_limit_kmh=spd)
            G.add_edge("NORTH_THROAT", entry, length_m=dist_m, speed_limit_kmh=spd)
        else:
            G.add_edge(entry, "SOUTH_THROAT", length_m=dist_m, speed_limit_kmh=spd)
            G.add_edge("SOUTH_THROAT", entry, length_m=dist_m, speed_limit_kmh=spd)
    return G


def _infer_specs(name: str, type_str: str = "") -> tuple:
    """Returns (tier, mps, mass, pax) from train name."""
    n = name.upper()
    if "VANDE" in n or "TEJAS" in n:
        return 2, 160, 430, 1128
    if "RAJDHANI" in n or "SHATABDI" in n or "DURONTO" in n:
        return 2, 130, 520, 1200
    if "SF" in n or "SUPERFAST" in n or type_str.upper() == "SUPERFAST":
        return 4, 110, 850, 1800
    if "EXPRESS" in n or "MAIL" in n:
        return 5, 100, 780, 1500
    if "GOODS" in n or "BOXN" in n or "FREIGHT" in n or "CONTAINER" in n:
        return 7, 75, 3800, 0
    return 5, 100, 780, 1400


def _parse_api_response(raw_list: List[Dict], station: Dict, station_code: str) -> List[Dict]:
    """Convert raw RailRadar API list into unified simulation train dicts."""
    result = []
    corridors = list(station["corridors"].items())
    now = datetime.now()
    for i, raw in enumerate(raw_list):
        train_no   = str(raw.get("trainNumber") or raw.get("train_no") or f"0{i}000")
        train_name = str(raw.get("trainName")   or raw.get("train_name") or f"Train {train_no}")
        status_str = str(raw.get("status", "running")).lower()
        delay_min  = int(raw.get("delayMinutes", 0))
        dist_km    = float(raw.get("distanceToDestination", raw.get("distance", 15.0)))
        speed_kmh  = float(raw.get("speed", raw.get("currentSpeed", 85.0)))
        source     = str(raw.get("source", "ORIGIN"))

        tier, mps, mass, pax = _infer_specs(train_name, raw.get("type", ""))
        dir_key, corridor_info = corridors[i % len(corridors)]
        dist_m        = dist_km * 1000.0
        cur_speed     = speed_kmh if status_str == "running" else max(float(mps) * 0.7, 60.0)
        # ideal_sec: how long it would take at MPS with no delay
        ideal_sec     = dist_m / max(float(mps) / 3.6, 1.0)
        # scheduled = ideal arrival offset from sim start (always positive & reasonable)
        sched_sec     = max(ideal_sec, 60.0)
        # Wall-clock scheduled arrival time
        scheduled_dt  = now + timedelta(seconds=sched_sec)

        result.append({
            "id": train_no, "name": train_name, "tier": tier,
            "corridor_dir": dir_key,
            "corridor": DIR_TO_CORRIDOR.get(dir_key, "NORTH_CORRIDOR"),
            "route_type": f"{corridor_info['label']} (Live Inbound)",
            "best_route": f"{corridor_info['label']} → Platform {(i % station['platforms']) + 1}",
            "dist_m": dist_m, "current_speed": cur_speed, "mps": float(mps),
            "mass": float(mass), "pax": pax,
            "delay_min": delay_min,
            "scheduled_arrival_offset_sec": max(sched_sec, 10.0),
            "scheduled_arrival_str": scheduled_dt.strftime("%H:%M:%S"),
            "color": TIER_COLORS.get(tier, "#94a3b8"),
            "source": source, "dest": station_code.upper(),
        })
    return result


def _generate_simulated_board(station_code: str, station: Dict) -> List[Dict]:
    """Realistic simulated station board using authentic station-specific train schedules."""
    corridors = list(station["corridors"].items())
    trains = []
    now = datetime.now()
    used: set = set()
    code_up = station_code.upper()
    pool = [dict(t) for t in STATION_TRAIN_SCHEDULES.get(code_up, TRAIN_POOL)]
    random.shuffle(pool)

    def pick():
        for t in pool:
            if t["no"] not in used:
                used.add(t["no"])
                pool.remove(t)
                return t
        # Fallback to generic pool if station pool exhausted
        for t in TRAIN_POOL:
            if t["no"] not in used:
                used.add(t["no"])
                return dict(t)
        return None

    for dir_key, corridor_info in corridors:
        base_km  = corridor_info["block_dist_km"]
        n_trains = 2 if base_km < 12 else random.randint(2, 3)
        # Generate safe, non-overlapping staggered distances (at least 4.5 km headway gap per line)
        for i in range(n_trains):
            tmpl = pick()
            if not tmpl:
                continue
            # Base separation: Train 0 at 4-7km, Train 1 at 12-16km, Train 2 at 22-28km
            headway_base = 4.0 + (i * 7.5) + random.uniform(0.5, 3.0)
            dist_km      = round(max(3.5, min(40.0, headway_base)), 2)
            speed_pct    = random.uniform(0.70, 0.92)
            speed_kmh    = round(tmpl["mps"] * speed_pct, 1)
            dist_m       = dist_km * 1000.0
            ideal_sec    = dist_m / max(float(tmpl["mps"]) / 3.6, 1.0)
            sched_sec    = max(ideal_sec, 60.0)
            pred_sec     = dist_m / max(speed_kmh / 3.6, 1.0)
            delay_min    = max(0, round((pred_sec - sched_sec) / 60.0, 1))
            sched_dt     = now + timedelta(seconds=sched_sec)
            pf_num     = (len(trains) % station["platforms"]) + 1
            trains.append({
                "id": tmpl["no"], "name": tmpl["name"], "tier": tmpl["tier"],
                "corridor_dir": dir_key,
                "corridor": DIR_TO_CORRIDOR.get(dir_key, "NORTH_CORRIDOR"),
                "route_type": f"{corridor_info['label']} (Simulated Inbound)",
                "best_route": f"{corridor_info['label']} → Platform {pf_num}",
                "dist_m": dist_m, "current_speed": speed_kmh, "mps": float(tmpl["mps"]),
                "mass": float(tmpl["mass"]), "pax": tmpl["pax"],
                "delay_min": delay_min,
                "scheduled_arrival_offset_sec": max(sched_sec, 10.0),
                "scheduled_arrival_str": sched_dt.strftime("%H:%M:%S"),
                "color": TIER_COLORS.get(tmpl["tier"], "#94a3b8"),
                "source": corridor_info["neighbor"], "dest": station_code.upper(),
            })

    trains.sort(key=lambda t: (t["tier"], t["scheduled_arrival_offset_sec"]))
    return trains


class StationBoardFetcher:
    """Fetches live station board from RailRadar API with simulated fallback."""

    @classmethod
    def fetch_board(cls, station_code: str, api_key: str = "") -> List[Dict]:
        station   = get_station(station_code)
        code_up   = station_code.upper()
        if api_key.strip():
            live = cls._try_live_api(code_up, api_key.strip(), station)
            if live:
                return live
        return _generate_simulated_board(code_up, station)

    @classmethod
    def _try_live_api(cls, code: str, api_key: str, station: Dict) -> Optional[List[Dict]]:
        url     = f"https://api.railradar.in/v1/stations/{code}/live"
        headers = {"Authorization": f"Bearer {api_key}", "x-api-key": api_key}
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                print(f"[StationBoard] RailRadar HTTP {resp.status_code} for {code}")
                return None
            data     = resp.json()
            raw_list = data.get("data", data) if isinstance(data, dict) else data
            if not isinstance(raw_list, list) or len(raw_list) == 0:
                return None
            return _parse_api_response(raw_list, station, code)
        except Exception as e:
            print(f"[StationBoard] API error: {e}")
            return None
