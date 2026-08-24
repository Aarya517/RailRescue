import simpy
import json
from api_client import RailwayDataIngestor
from network_topology import build_gwalior_network
from kinematics import KinematicsEngine
from conflict_engine import ConflictRiskEngine
from optimizer import ScheduleOptimizer
from agent_mesh import SectionAgent

def run_simulation():
    env = simpy.Environment()
    topology = build_gwalior_network()
    
    # 1. Ingest Data via Ingestor (Local JSON or live fallback)
    print(" [1/4] Ingesting Live Corridor Feeds from Railway Ingestion Engine...")
    trains = RailwayDataIngestor.load_from_local_json("gwalior_live_sample.json")
    print(f"       -> Successfully loaded {len(trains)} active trains for Gwalior Hub.")
    
    # 2. Optimize Platform & Sequence using CP-SAT
    print(" [2/4] Solving Mixed-Integer Platform & Headway Model (CP-SAT)...")
    optimized_schedule = ScheduleOptimizer.solve_platform_and_sequencing(trains)
    
    # 3. Setup Mesh Agents
    agent_north = SectionAgent("AGENT_NORTH_CORRIDOR")
    agent_gwl = SectionAgent("AGENT_GWL_CORE")
    
    print(" [3/4] Initialized Mesh Agents & Interlocking Topology.")
    print(" [4/4] Starting Continuous Kinematics Simulation...\n")

    # Train state tracking
    states = {
        t.train_id: {
            "dist_remaining_m": t.current_position_m,
            "speed_kmh": t.current_speed_kmh,
            "allocated_pf": optimized_schedule[t.train_id]["allocated_platform"] if t.train_id in optimized_schedule else 1
        }
        for t in trains
    }

    def train_simulation_step(env):
        dt = 5.0 # Simulation step duration (seconds)
        while True:
            sim_time = env.now
            print(f"\n================ [ SIMULATION TICK: T+{sim_time:.0f}s ] ================")

            # Conflict evaluation across North Corridor trains if present
            active_ids = list(states.keys())
            if len(active_ids) >= 2:
                t1_id, t2_id = active_ids[0], active_ids[1]
                t1_st, t2_st = states[t1_id], states[t2_id]
                conflict_info = ConflictRiskEngine.evaluate_risk(
                    lead_train_dist_m=t1_st["dist_remaining_m"],
                    lead_train_speed_kmh=t1_st["speed_kmh"],
                    trail_train_dist_m=t2_st["dist_remaining_m"],
                    trail_train_speed_kmh=t2_st["speed_kmh"],
                    same_track=True
                )
                print(f" [Collision Predictor] North Corridor Risk: {conflict_info['risk_score']} | Status: {conflict_info['status']}")

            for t in trains:
                st = states[t.train_id]
                if st["dist_remaining_m"] > 0:
                    target_speed, advisory = KinematicsEngine.calculate_optimal_target_speed(
                        current_dist_to_target_m=st["dist_remaining_m"],
                        section_mps_kmh=t.max_permissible_speed_kmh,
                        target_speed_limit_kmh=30.0 # Platform entry speed
                    )
                    
                    if st["speed_kmh"] > target_speed:
                        st["speed_kmh"] = max(target_speed, st["speed_kmh"] - (KinematicsEngine.SERVICE_DECELERATION * 3.6 * dt))
                    elif st["speed_kmh"] < target_speed:
                        st["speed_kmh"] = min(target_speed, st["speed_kmh"] + (KinematicsEngine.ACCELERATION * 3.6 * dt))

                    travelled = (st["speed_kmh"] / 3.6) * dt
                    st["dist_remaining_m"] = max(0.0, st["dist_remaining_m"] - travelled)
                else:
                    st["speed_kmh"] = 0.0
                    advisory = "AT_PLATFORM_DWELL"

                print(f" Train {t.train_id:<8} | {t.train_name[:22]:<22} | Speed: {st['speed_kmh']:>5.1f} km/h | Dist: {st['dist_remaining_m']:>7.1f}m | PF: {st['allocated_pf']} | Status: {advisory}")

            if all(s["dist_remaining_m"] <= 0 for s in states.values()):
                print("\n All scheduled trains safely berthed at Gwalior Junction.")
                break

            yield env.timeout(dt)

    env.process(train_simulation_step(env))
    env.run(until=150.0)

if __name__ == "__main__":
    run_simulation()