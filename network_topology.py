"""
Defines the directed topology graph for Gwalior Junction Hub and approach sectors.
"""
import networkx as nx

def build_gwalior_network() -> nx.DiGraph:
    G = nx.DiGraph()

    # Platforms at Gwalior (Capacity, max coach length)
    for pf in range(1, 7):
        G.add_node(f"GWL_PF_{pf}", type="PLATFORM", length_m=650, max_speed_kmh=30.0)

    # Core Outer Junction Neck & Yard Throats
    G.add_node("RAYARU_JUNCTION", type="SIGNAL_BLOCK", max_speed_kmh=110.0)
    G.add_node("BANMORE_IN", type="BOUNDARY", max_speed_kmh=130.0)
    G.add_node("GWL_NORTH_THROAT", type="INTERLOCKING", max_speed_kmh=30.0)
    
    G.add_node("SITHOULI_IN", type="BOUNDARY", max_speed_kmh=130.0)
    G.add_node("GWL_SOUTH_THROAT", type="INTERLOCKING", max_speed_kmh=30.0)
    
    G.add_node("PANIHAR_BRANCH", type="BRANCH_BOUNDARY", max_speed_kmh=80.0) # Towards Guna
    G.add_node("MALANPUR_BRANCH", type="BRANCH_BOUNDARY", max_speed_kmh=60.0) # Towards Bhind

    # Tracks / Edges (distance in meters, speed limit km/h)
    G.add_edge("BANMORE_IN", "RAYARU_JUNCTION", length_m=6000.0, speed_limit_kmh=130.0)
    G.add_edge("RAYARU_JUNCTION", "GWL_NORTH_THROAT", length_m=4000.0, speed_limit_kmh=90.0)
    
    # North throat to platforms
    for pf in range(1, 7):
        G.add_edge("GWL_NORTH_THROAT", f"GWL_PF_{pf}", length_m=800.0, speed_limit_kmh=30.0)
        G.add_edge(f"GWL_PF_{pf}", "GWL_SOUTH_THROAT", length_m=800.0, speed_limit_kmh=30.0)

    # South throat to Sithouli
    G.add_edge("GWL_SOUTH_THROAT", "SITHOULI_IN", length_m=7000.0, speed_limit_kmh=120.0)
    
    # Reverse directions
    G.add_edge("SITHOULI_IN", "GWL_SOUTH_THROAT", length_m=7000.0, speed_limit_kmh=120.0)
    G.add_edge("GWL_NORTH_THROAT", "RAYARU_JUNCTION", length_m=4000.0, speed_limit_kmh=90.0)
    G.add_edge("RAYARU_JUNCTION", "BANMORE_IN", length_m=6000.0, speed_limit_kmh=130.0)

    return G