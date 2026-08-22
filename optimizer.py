"""
Uses OR-Tools CP-SAT to resolve platform occupancy and sequence arrival based on IR priorities.
"""
from ortools.sat.python import cp_model
from typing import List, Dict
from api_client import TrainAPISchema

class ScheduleOptimizer:
    @staticmethod
    def solve_platform_and_sequencing(trains: List[TrainAPISchema]) -> Dict[str, Dict[str, int]]:
        model = cp_model.CpModel()
        
        train_starts = {}
        train_ends = {}
        train_pfs = {}
        intervals_per_pf = {pf: [] for pf in range(1, 7)}
        
        # Priority Weightings (Lower Tier -> Higher Priority Weight)
        weight_map = {1: 100, 2: 50, 4: 20, 5: 10, 7: 2}

        for t in trains:
            # Time bounds in seconds from t=0
            earliest_arr = int(t.scheduled_arrival_gwl_sec)
            latest_arr = earliest_arr + 3600
            
            start_var = model.NewIntVar(earliest_arr, latest_arr, f"start_{t.train_id}")
            dwell_var = int(t.scheduled_dwell_sec)
            end_var = model.NewIntVar(earliest_arr + dwell_var, latest_arr + dwell_var, f"end_{t.train_id}")
            
            train_starts[t.train_id] = start_var
            train_ends[t.train_id] = end_var
            
            # Platform choices (1-6)
            pf_var = model.NewIntVar(1, 6, f"pf_{t.train_id}")
            train_pfs[t.train_id] = pf_var
            
            # Assign optional interval per platform
            for p in range(1, 7):
                is_on_p = model.NewBoolVar(f"t_{t.train_id}_is_on_pf_{p}")
                model.Add(pf_var == p).OnlyEnforceIf(is_on_p)
                model.Add(pf_var != p).OnlyEnforceIf(is_on_p.Not())
                
                # Headway margin: 180 seconds between trains on same platform
                interval = model.NewOptionalIntervalVar(
                    start_var, dwell_var + 180, end_var + 180, is_on_p, f"ival_{t.train_id}_pf_{p}"
                )
                intervals_per_pf[p].append(interval)

        # Hard Constraint: Non-overlapping platform occupation
        for p in range(1, 7):
            model.AddNoOverlap(intervals_per_pf[p])

        # Objective: Minimize weighted delay
        delay_penalties = []
        for t in trains:
            p_weight = weight_map.get(t.priority_tier, 5)
            delay = model.NewIntVar(0, 3600, f"delay_{t.train_id}")
            model.Add(delay == train_starts[t.train_id] - int(t.scheduled_arrival_gwl_sec))
            delay_penalties.append(delay * p_weight)

        model.Minimize(sum(delay_penalties))
        
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = 2.0
        status = solver.Solve(model)
        
        plan = {}
        if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            for t in trains:
                plan[t.train_id] = {
                    "allocated_platform": solver.Value(train_pfs[t.train_id]),
                    "planned_arrival_sec": solver.Value(train_starts[t.train_id]),
                    "delay_sec": solver.Value(train_starts[t.train_id]) - int(t.scheduled_arrival_gwl_sec)
                }
        return plan