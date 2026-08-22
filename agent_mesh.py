"""
Defines autonomous section agents collaborating over Gwalior corridors.
"""
from typing import Dict, Any

class SectionAgent:
    def __init__(self, section_id: str):
        self.section_id = section_id
        self.active_trains = []
        self.boundary_reservations = {}

    def negotiate_slot(self, train_id: str, requested_entry_time: float, neighbor_agent: 'SectionAgent') -> float:
        """
        Coordinates boundary acceptance time with adjacent section agent.
        """
        accepted_time = requested_entry_time
        # If conflict exists at boundary, bump time by safety headway (180s)
        if requested_entry_time in self.boundary_reservations.values():
            accepted_time += 180.0
        
        self.boundary_reservations[train_id] = accepted_time
        return accepted_time