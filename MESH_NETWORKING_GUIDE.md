# RailRescue Distributed Multi-Agent System (MAS) — Networking & Deployment Guide

This guide explains how to run **RailRescue** as a true **Distributed Multi-Agent Network** across multiple ports or separate physical laptops during your hackathon presentation.

---

## 🌟 Architecture Overview

In RailRescue MAS, each major station runs as an **independent autonomous intelligent agent**:
- **Agent GWL** (Gwalior Junction)
- **Agent AGC** (Agra Cantt)
- **Agent NDLS** (New Delhi)
- **Agent JHS** (Jhansi Junction)

These agents communicate via asynchronous REST webhooks (`/api/mesh/...`) to execute:
1. **Automated Boundary Handoffs**: When trains approach $\le 3.5\text{ km}$ of a division boundary.
2. **DMAPPC Consensus Engine**: Evaluates platform capacity, priority tiers, and interlocking safety.
3. **3-Way Corridor Cascades**: Multi-station transit verification (GWL $\rightarrow$ AGC $\rightarrow$ NDLS).
4. **Capacity Rejection (`REJECTED_HOLD`)**: Holds lower-priority trains at boundary signals when downstream stations are at 100% capacity.

---

## 🚀 Option 1: Single Machine Multi-Node (Localhost Multi-Port)

You can launch 3 fully independent station agents on your single laptop with one command:

```bash
# Mode 1: Run Console Protocol Simulation
python demo_mesh_multi_station.py --mode sim

# Mode 2: Launch 3 Live Station Web Servers
python demo_mesh_multi_station.py --mode launch
```

Once launched, open your browser to view the distinct station dashboards:
* **Node 1 (Gwalior Agent)**: `http://127.0.0.1:8000`
* **Node 2 (Agra Agent)**: `http://127.0.0.1:8001`
* **Node 3 (New Delhi Agent)**: `http://127.0.0.1:8002`

---

## 🌐 Option 2: Multi-Laptop Presentation Setup (Local LAN / Wi-Fi)

To demonstrate true distributed multi-agent intelligence across 2 or 3 team laptops:

### On Laptop 1 (Gwalior Control Room):
```bash
# Find your Local IP (e.g., 192.168.1.50)
ipconfig

# Launch Gwalior Node
$env:STATION_CODE="GWL"
$env:PORT="8000"
python app.py
```

### On Laptop 2 (Agra Cantt Control Room):
```bash
# Launch Agra Node on Laptop 2 (e.g., IP: 192.168.1.55)
$env:STATION_CODE="AGC"
$env:PORT="8000"
python app.py
```

### Peer Registration (Connect the Agents):
From Laptop 1, register Laptop 2 as a peer:
```http
POST http://192.168.1.50:8000/api/mesh/peers/register
Content-Type: application/json

{
  "station_code": "AGC",
  "url": "http://192.168.1.55:8000"
}
```

Now, whenever a train approaches the boundary on Laptop 1, it will automatically negotiate and appear on Laptop 2 in real time!

---

## ☁️ Option 3: Cloud / Ngrok Global Tunneling

If you want to demo the system over the internet or mobile hotspots:

1. Start Ngrok on Laptop 1:
   ```bash
   ngrok http 8000
   # Provides public URL: https://gwl-agent.ngrok-free.app
   ```
2. Start Ngrok on Laptop 2:
   ```bash
   ngrok http 8000
   # Provides public URL: https://agc-agent.ngrok-free.app
   ```
3. Register the public URLs via the `/api/mesh/peers/register` endpoint.

---

## 🧪 Automated Test Suite

To run the automated verification test suite:

```bash
pytest test_agent_mesh.py -v
```

All 6 tests verify:
- Pydantic Telemetry Schema Validation
- DMAPPC High-Priority Train Acceptance
- Saturated Station Capacity Rejection (`REJECTED_HOLD`)
- Directional Corridor Peer Routing
- FastAPI Mesh Status & REST Webhook Endpoints
- Asynchronous Handoff Execution
