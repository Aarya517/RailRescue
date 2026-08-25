"""
test_agent_mesh.py — Comprehensive Test Suite for RailRescue Multi-Agent Mesh.
Uses Python's built-in unittest module (Zero external dependencies).
"""
import unittest
import asyncio
from datetime import datetime, timedelta
from fastapi.testclient import TestClient

from app import app, session
from agent_mesh_communicator import (
    AgentMeshCommunicator,
    HandoffProposal,
    TrainTelemetryPayload,
    CorridorCascadeQuery,
    CascadeResponse,
    HandoffResponse
)

class TestMultiAgentMesh(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.agent = AgentMeshCommunicator("GWL", session)

    def test_01_telemetry_schema_validation(self):
        payload = TrainTelemetryPayload(
            train_id="12002",
            train_name="Bhopal Shatabdi Express",
            tier=2,
            current_speed=130.0,
            mps=150.0,
            mass=450.0,
            pax=1100,
            delay_min=0.5,
            corridor_dir="N",
            dist_remaining_m=2800.0,
            boundary_eta_str="12:04:15",
            target_platform=1,
        )
        self.assertEqual(payload.train_id, "12002")
        self.assertEqual(payload.tier, 2)
        self.assertEqual(payload.dist_remaining_m, 2800.0)

    def test_02_dmappc_high_priority_accepted(self):
        session.trains = []
        session.disruption_active = False
        
        proposal = HandoffProposal(
            proposal_id="prop_test_12002",
            source_station="AGC",
            target_station="GWL",
            corridor_dir="N",
            timestamp=datetime.now().strftime("%H:%M:%S"),
            train=TrainTelemetryPayload(
                train_id="12002",
                train_name="Bhopal Shatabdi Exp",
                tier=2,
                current_speed=130.0,
                mps=150.0,
                mass=450.0,
                pax=1100,
                delay_min=0.0,
                corridor_dir="N",
                dist_remaining_m=3000.0,
                boundary_eta_str="12:05:00",
                target_platform=1
            ),
            requested_slot_sec=120.0,
            requested_platform=1
        )
        
        resp = self.agent.evaluate_incoming_proposal(proposal)
        self.assertEqual(resp.status, "ACCEPTED")
        self.assertIn(resp.allocated_platform, [1, 2])
        self.assertGreater(resp.advised_speed_kmh, 100.0)

    def test_03_dmappc_saturated_station_rejection(self):
        # Fill all platforms
        session.trains = [
            {"id": f"t_mock_{i}", "allocated_pf": i, "dist_remaining": 1500.0, "current_speed": 40.0}
            for i in range(1, 7)
        ]
        
        freight_proposal = HandoffProposal(
            proposal_id="prop_test_freight",
            source_station="AGC",
            target_station="GWL",
            corridor_dir="N",
            timestamp=datetime.now().strftime("%H:%M:%S"),
            train=TrainTelemetryPayload(
                train_id="41502",
                train_name="NCR Container Freight",
                tier=7,
                current_speed=65.0,
                mps=75.0,
                mass=3800.0,
                pax=0,
                delay_min=5.0,
                corridor_dir="N",
                dist_remaining_m=3200.0,
                boundary_eta_str="12:10:00"
            ),
            requested_slot_sec=300.0
        )
        
        resp = self.agent.evaluate_incoming_proposal(freight_proposal)
        self.assertEqual(resp.status, "REJECTED_HOLD")
        self.assertEqual(resp.advised_speed_kmh, 0.0)
        self.assertEqual(resp.kavach_aspect, "DANGER")

    def test_04_corridor_peer_routing(self):
        self.assertEqual(self.agent.get_neighbor_agent_for_corridor("N"), "AGC")
        self.assertEqual(self.agent.get_neighbor_agent_for_corridor("S"), "JHS")
        self.assertEqual(self.agent.get_neighbor_agent_for_corridor("E"), "BTE")
        self.assertEqual(self.agent.get_neighbor_agent_for_corridor("W"), "GUNA")

    def test_05_fastapi_mesh_endpoints(self):
        r_status = self.client.get("/api/mesh/status")
        self.assertEqual(r_status.status_code, 200)
        data = r_status.json()
        self.assertIn("agent_id", data)
        self.assertIn("peers", data)
        
        r_telemetry = self.client.get("/api/telemetry")
        self.assertEqual(r_telemetry.status_code, 200)
        tel = r_telemetry.json()
        self.assertIn("station_code", tel)
        self.assertIn("trains", tel)

    def test_06_simulated_mesh_runner(self):
        from demo_mesh_multi_station import run_simulated_mesh_protocol
        asyncio.run(run_simulated_mesh_protocol())


if __name__ == "__main__":
    unittest.main(verbosity=2)
