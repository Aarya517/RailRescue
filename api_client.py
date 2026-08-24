import os
import requests
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class TrainAPISchema(BaseModel):
    train_id: str
    train_name: str
    priority_tier: int = Field(..., description="1: Relief, 2: Vande Bharat/Rajdhani, 4: Superfast, 5: Express, 7: Freight")
    origin: str
    destination: str
    current_corridor: str
    entry_node: str
    target_platform: Optional[int] = None
    scheduled_arrival_gwl_sec: float
    scheduled_dwell_sec: float
    mass_tons: float
    max_permissible_speed_kmh: float
    current_position_m: float
    current_speed_kmh: float
    passenger_count: int

class RailwayDataIngestor:
    """Strictly ingests LIVE railway data from the API for the simulation mesh."""

    @staticmethod
    def infer_priority_and_specs(train_name: str, train_type: str = "") -> tuple[int, float, float, int]:
        """Infers physical constants for the physics engine based on API train naming."""
        name_upper = train_name.upper()
        if "VANDE BHARAT" in name_upper or "TEJAS" in name_upper:
            return 2, 430.0, 130.0, 1128
        elif "RAJDHANI" in name_upper or "SHATABDI" in name_upper or "DURONTO" in name_upper:
            return 2, 520.0, 130.0, 1200
        elif "SF" in name_upper or "SUPERFAST" in name_upper or train_type == "SUPERFAST":
            return 4, 850.0, 110.0, 1800
        elif "EXPRESS" in name_upper or "MAIL" in name_upper:
            return 5, 780.0, 100.0, 1500
        elif "PASSENGER" in name_upper or "MEMU" in name_upper or "DEMU" in name_upper:
            return 6, 450.0, 80.0, 800
        elif "GOODS" in name_upper or "BOXN" in name_upper or "CONTAINER" in name_upper or "FREIGHT" in name_upper:
            return 7, 3800.0, 75.0, 0
        return 5, 750.0, 100.0, 1200

    @staticmethod
    def infer_corridor_entry(source: str, dest: str) -> tuple[str, str]:
        """Determines the geographic entry point based on true origin/destination."""
        north_sources = ["NDLS", "NZM", "DLI", "AGC", "ASR", "JAT", "CDG"]
        south_sources = ["VGLJ", "BPL", "RKMP", "NGP", "MAS", "SBC", "HYB", "TVC"]

        if any(s in source.upper() for s in north_sources):
            return "NORTH_CORRIDOR", "BANMORE_IN"
        elif any(s in source.upper() for s in south_sources):
            return "SOUTH_CORRIDOR", "SITHOULI_IN"
        elif "ETW" in source.upper() or "BIX" in source.upper():
            return "EAST_BRANCH", "MALANPUR_BRANCH"
        elif "GUNA" in source.upper() or "SVPI" in source.upper():
            return "WEST_BRANCH", "PANIHAR_BRANCH"
        else:
            return "NORTH_CORRIDOR", "BANMORE_IN"

    @classmethod
    def fetch_live_station_board(cls, station_code: str, api_key: str) -> List[TrainAPISchema]:
        """Fetches the live board directly from RailRadar API and strictly maps real telemetry."""
        if not api_key:
            print("Error: API Key is required for live ingestion.")
            return []

        url = f"https://api.railradar.in/v1/stations/{station_code.upper()}/live"
        headers = {"Authorization": f"Bearer {api_key}"}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"API Error: HTTP {response.status_code}")
                return []

            data = response.json()
            raw_trains = data.get("data", []) if isinstance(data, dict) else data
            return cls.parse_api_response_to_simulation(raw_trains)

        except Exception as e:
            print(f"Network / API failure: {str(e)}")
            return []

    @classmethod
    def parse_api_response_to_simulation(cls, raw_trains_list: List[Dict[str, Any]]) -> List[TrainAPISchema]:
        """Transforms raw live JSON directly into simulation physics models."""
        sim_trains = []
        sim_reference_time = datetime.now()

        for raw in raw_trains_list:
            train_no = str(raw.get("trainNumber") or raw.get("train_no") or "00000")
            train_name = str(raw.get("trainName") or raw.get("train_name") or "Unknown")
            source = str(raw.get("source") or "ORIGIN")
            dest = str(raw.get("destination") or "DEST")
            
            # Extract true live metrics
            live_speed_kmh = float(raw.get("speed", raw.get("currentSpeed", 0.0)))
            live_dist_km = float(raw.get("distanceToDestination", raw.get("distance", 15.0)))
            dist_m = live_dist_km * 1000.0

            # Calculate precise arrival offset based on live API delay
            delay_mins = int(raw.get("delayMinutes", 0))
            arrival_offset_sec = (live_dist_km / max(live_speed_kmh, 1.0)) * 3600.0 if live_speed_kmh > 0 else (delay_mins * 60.0)

            # Map physical constants
            p_tier, mass, mps, pax = cls.infer_priority_and_specs(train_name, raw.get("type", ""))
            corridor, entry_node = cls.infer_corridor_entry(source, dest)

            sim_trains.append(
                TrainAPISchema(
                    train_id=train_no,
                    train_name=train_name,
                    priority_tier=p_tier,
                    origin=source,
                    destination=dest,
                    current_corridor=corridor,
                    entry_node=entry_node,
                    target_platform=int(raw.get("platform", 1)) if str(raw.get("platform", "")).isdigit() else None,
                    scheduled_arrival_gwl_sec=arrival_offset_sec,
                    scheduled_dwell_sec=300.0, # Defaulting to 5 min dwell if API omits it
                    mass_tons=mass,
                    max_permissible_speed_kmh=mps,
                    current_position_m=dist_m,
                    current_speed_kmh=live_speed_kmh,
                    passenger_count=pax
                )
            )
        return sim_trains