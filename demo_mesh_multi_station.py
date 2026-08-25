"""
demo_mesh_multi_station.py — Multi-Node Station Launcher & Protocol Simulator.
Runs a 3-Station Multi-Agent System (GWL -> AGC -> NDLS).

Usage:
  python demo_mesh_multi_station.py --mode sim      # Runs console simulation of 3-agent DMAPPC protocol
  python demo_mesh_multi_station.py --mode launch   # Spawns 3 live FastAPI servers on ports 8000, 8001, 8002
"""
import argparse
import asyncio
import subprocess
import sys
import time
from datetime import datetime

# Windows encoding safety
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
# 1. PROTOCOL SIMULATION MODE
# ─────────────────────────────────────────────────────────────────────────────
async def run_simulated_mesh_protocol():
    print("=" * 80)
    print(" [>>] RAILRESCUE DISTRIBUTED MULTI-AGENT SYSTEM (MAS) PROTOCOL SIMULATION")
    print("=" * 80)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Initializing 3 Station Agents: Agent-GWL, Agent-AGC, Agent-NDLS...\n")

    # Step 1: Agent GWL detects Train 12002 at North Boundary (2.8 km out)
    print("--- [STEP 1: BOUNDARY DETECTION AT AGENT-GWL] ---")
    print("Train: 12002 Bhopal Shatabdi Express | Tier 2 (Top Priority) | Speed: 130 km/h")
    print("Location: North Corridor Block Section 102 (2.8 km to Agra Division Boundary)")
    print("Action: Triggering automated inter-station handoff proposal to Agent-AGC...\n")
    await asyncio.sleep(0.5)

    # Step 2: Handoff Proposal transmission
    print("--- [STEP 2: HANDOFF PROPOSAL TRANSMISSION (GWL -> AGC)] ---")
    prop = {
        "proposal_id": "prop_gwl_agc_12002",
        "source": "Agent-GWL",
        "target": "Agent-AGC",
        "corridor": "North UP Trunk Line",
        "requested_slot": "12:05:00",
        "train_specs": "Mass: 450T | Pax: 1100 | MPS: 150 km/h"
    }
    print("Payload transmitted via Webhook POST /api/mesh/handoff/propose:")
    print(f" -> Proposal ID: {prop['proposal_id']}")
    print(f" -> Source: {prop['source']} | Target: {prop['target']}")
    print(f" -> Requested Entry Slot: {prop['requested_slot']}\n")
    await asyncio.sleep(0.5)

    # Step 3: Agent AGC checks 3-Way Corridor Cascade with Agent NDLS
    print("--- [STEP 3: 3-WAY CORRIDOR CASCADE INQUIRY (AGC -> NDLS)] ---")
    print("Agent-AGC evaluates downstream corridor: querying Agent-NDLS for track clearance...")
    print("Cascade Query: Train 12002 transit to NDLS Mathura-Delhi trunk corridor.")
    print("Response from Agent-NDLS: [CORRIDOR CLEAR] - Block Section 204 open. Speed ceiling: 130 km/h.\n")
    await asyncio.sleep(0.5)

    # Step 4: DMAPPC Consensus Evaluation at Agent AGC
    print("--- [STEP 4: DMAPPC CONSENSUS EVALUATION AT AGENT-AGC] ---")
    print("Evaluating Platform Interlock Matrix at Agra Cantt (AGC)...")
    print(" -> Platforms: 6 total | Available: Platform 1 Main UP")
    print(" -> Precedence Weight: Tier 2 (Premium Rake) -> Granted Non-Stop Precedence")
    print(" -> Consensus Result: ACCEPTED (Platform 1 Locked, Speed: 130 km/h)\n")
    await asyncio.sleep(0.5)

    # Step 5: Final Driver Machine Directive
    print("--- [STEP 5: CONSENSUS CONFIRMATION & IN-CAB DIRECTIVE] ---")
    print("Transmitted to Loco Pilot 12002 via Kavach LTE:")
    print(" [*] 'GREEN CORRIDOR GRANTED: Agent-AGC accepted Train 12002 on Platform 1. Maintain section speed 130 km/h.'")
    print("\n" + "=" * 80)
    print(" [OK] MULTI-AGENT PROTOCOL SIMULATION COMPLETED WITH 100% CONSENSUS AGREEMENT")
    print("=" * 80)


# ─────────────────────────────────────────────────────────────────────────────
# 2. LIVE MULTI-NODE SPAWNER MODE
# ─────────────────────────────────────────────────────────────────────────────
def launch_multi_node_servers():
    print("=" * 80)
    print(" [*] SPAWNING 3 LIVE RAILRESCUE MULTI-AGENT NODES")
    print("=" * 80)
    
    nodes = [
        {"code": "GWL",  "port": "8000"},
        {"code": "AGC",  "port": "8001"},
        {"code": "NDLS", "port": "8002"},
    ]

    procs = []
    try:
        for node in nodes:
            print(f"[>] Starting Agent-{node['code']} on http://127.0.0.1:{node['port']}...")
            cmd = [sys.executable, "-c", f"""
import os, uvicorn
os.environ['STATION_CODE'] = '{node['code']}'
os.environ['PORT'] = '{node['port']}'
from app import app
uvicorn.run(app, host='127.0.0.1', port={node['port']}, log_level='warning')
"""]
            p = subprocess.Popen(cmd)
            procs.append(p)
            time.sleep(1.0)

        print("\n[OK] All 3 Multi-Agent Nodes Running!")
        print("  - Node 1 (Agent GWL):  http://127.0.0.1:8000")
        print("  - Node 2 (Agent AGC):  http://127.0.0.1:8001")
        print("  - Node 3 (Agent NDLS): http://127.0.0.1:8002")
        print("\nPress Ctrl+C to terminate all nodes.")
        
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[!] Shutting down all multi-agent nodes...")
        for p in procs:
            p.terminate()
        print("All nodes terminated cleanly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RailRescue Multi-Agent Mesh Runner")
    parser.add_argument("--mode", choices=["sim", "launch"], default="sim", help="Mode: sim (protocol simulation) or launch (multi-node spawner)")
    args = parser.parse_args()

    if args.mode == "sim":
        asyncio.run(run_simulated_mesh_protocol())
    else:
        launch_multi_node_servers()
