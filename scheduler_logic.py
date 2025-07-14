# File: scheduler_logic.py (Optimized Version)
import pandas as pd
from io import StringIO
from datetime import datetime, time
from itertools import permutations

# ==============================================================================
# SECTION 1: SHARED CONFIGURATION AND HELPERS
# ==============================================================================

FINAL_SCHEDULE_ROW_ORDER = [
    "Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Expo",
    "Drink Maker 1", "Drink Maker 2", "Line Buster 3", "Break", "ToffTL"
]
WORK_POSITIONS = [p for p in FINAL_SCHEDULE_ROW_ORDER if p not in ["Break", "ToffTL"]]
LINE_BUSTER_ROLES = ["Line Buster 1", "Line Buster 2", "Line Buster 3"]
TOP_TIER_ROLES = ["Handout", "Line Buster 1", "Conductor"]

def parse_time_input(time_val, ref_date):
    if pd.isna(time_val) or str(time_val).strip().upper() in ['N/A', '']: return pd.NaT
    try: return pd.to_datetime(f"{ref_date.strftime('%Y-%m-%d')} {str(time_val).strip()}")
    except ValueError: return pd.NaT

def preprocess_employee_data(employee_data_list):
    all_slots = []
    ref_date = datetime(1970, 1, 1).date()
    for emp_data in employee_data_list:
        name_parts = emp_data.get('Name', '').split(' ', 1)
        name = f"{name_parts[0]} {name_parts[1][0] if len(name_parts) > 1 and name_parts[1] else ''}.".strip()
        s_start, s_end = parse_time_input(emp_data.get('Shift Start'), ref_date), parse_time_input(emp_data.get('Shift End'), ref_date)
        b_start, t_start = parse_time_input(emp_data.get('Break'), ref_date), parse_time_input(emp_data.get('ToffTL Start'), ref_date)
        b_end = b_start + pd.Timedelta(minutes=30) if pd.notna(b_start) else None
        t_end = t_start + pd.Timedelta(minutes=60) if pd.notna(t_start) else None
        if pd.notna(s_start) and pd.notna(s_end):
            curr = s_start
            while curr < s_end:
                on_break, on_tofftl = (pd.notna(b_start) and b_start <= curr < b_end), (pd.notna(t_start) and t_start <= curr < t_end)
                all_slots.append({'Time': curr.strftime('%I:%M %p').lstrip('0'), 'EmployeeNameFML': name, 'IsOnBreak': on_break, 'IsOnToffTL': on_tofftl})
                curr += pd.Timedelta(minutes=30)
    return pd.DataFrame(all_slots) if all_slots else pd.DataFrame()

# ==============================================================================
# SECTION 2: HEURISTIC (CONDUCTOR FIRST) SCHEDULER (Largely unchanged)
# ==============================================================================
def create_schedule_heuristic(store_open_time_obj, store_close_time_obj, employee_data_list):
    df_long = preprocess_employee_data(employee_data_list)
    if df_long.empty: return "No employee data to process."
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    availability = {t: set(g['EmployeeNameFML']) for t, g in df_long[~df_long['IsOnBreak'] & ~df_long['IsOnToffTL']].groupby('Time')}
    schedule = {t: {p: ("" if p not in ["Break", "ToffTL"] else []) for p in FINAL_SCHEDULE_ROW_ORDER} for t in time_slots}
    employee_last_worked = {emp: -100 for emp in df_long['EmployeeNameFML'].unique()}
    for i, slot_str in enumerate(time_slots):
        slot_time = parse_time_input(slot_str, datetime(1970,1,1).date()).time()
        if slot_time.minute != 0 or i + 1 >= len(time_slots): continue
        next_slot_str, best_candidate, max_idle_time = time_slots[i+1], None, -1
        possible_candidates = list(availability.get(slot_str, set()).intersection(availability.get(next_slot_str, set())))
        for emp in sorted(possible_candidates):
            idle_time = i - employee_last_worked[emp]
            if idle_time > max_idle_time: max_idle_time, best_candidate = idle_time, emp
        if best_candidate:
            schedule[slot_str]['Conductor'], schedule[next_slot_str]['Conductor'] = best_candidate, best_candidate
            availability[slot_str].remove(best_candidate)
            availability[next_slot_str].remove(best_candidate)
            employee_last_worked[best_candidate] = i + 1
    employee_states = {}
    for i, slot_str in enumerate(time_slots):
        for _, row in df_long[df_long['Time'] == slot_str].iterrows():
            if row['IsOnBreak']: schedule[slot_str]['Break'].append(row['EmployeeNameFML'])
            if row['IsOnToffTL']: schedule[slot_str]['ToffTL'].append(row['EmployeeNameFML'])
        for pos in WORK_POSITIONS:
            if schedule[slot_str][pos]: continue
            best_candidate = None
            for emp in sorted(list(availability.get(slot_str, set()))):
                state = employee_states.get(emp, {})
                last_pos, time_in_pos = state.get('last_pos'), state.get('time_in_pos', 0)
                if (pos in LINE_BUSTER_ROLES and last_pos in LINE_BUSTER_ROLES) or \
                   (pos not in LINE_BUSTER_ROLES and last_pos == pos and time_in_pos >= 2): continue
                best_candidate = emp
                break
            if best_candidate:
                schedule[slot_str][pos] = best_candidate
                availability[slot_str].remove(best_candidate)
                state = employee_states.get(best_candidate, {})
                state['time_in_pos'] = state.get('time_in_pos', 0) + 1 if state.get('last_pos') == pos else 1
                state['last_pos'] = pos
                employee_states[best_candidate] = state
    schedule_rows = [{"Time": time, **positions} for time, positions in schedule.items()]
    out_df = pd.DataFrame(schedule_rows, columns=["Time"] + FINAL_SCHEDULE_ROW_ORDER).fillna("")
    for col in ["Break", "ToffTL"]: out_df[col] = out_df[col].apply(lambda x: ", ".join(sorted(list(set(x)))) if isinstance(x, list) else x)
    final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
    return final_df.to_csv(index=False)

# ==============================================================================
# SECTION 3: BACKTRACKING (PHOENIX EDITION) - OPTIMIZED
# ==============================================================================
def calculate_assignment_cost(pos, emp, prev_state, slot_obj):
    cost = 0
    last_pos, time_in_pos = prev_state.get('last_pos'), prev_state.get('time_in_pos', 0)
    last_top_tier = prev_state.get('last_top_tier', 100)
    if pos == last_pos and pos != 'Conductor': cost += 10
    history = prev_state.get('history', [])
    if len(history) >= 3 and history[-2] == pos: cost += 5
    if pos in LINE_BUSTER_ROLES and last_pos in LINE_BUSTER_ROLES: cost += 1000
    if pos in TOP_TIER_ROLES: cost -= last_top_tier
    if pos == 'Conductor' and prev_state.get('last_pos') != 'Conductor' and slot_obj.minute != 0:
        cost += 500 # Make it a high-cost violation
    return cost

memo_cache = {} # OPTIMIZATION: Memoization cache

def solve_phoenix_recursive(time_idx, time_slots, availability, schedule, prev_states, best_cost_so_far):
    # OPTIMIZATION: Memoization - Check cache first
    state_key = (time_idx, tuple(sorted(prev_states.items())))
    if state_key in memo_cache:
        cached_cost, cached_schedule = memo_cache[state_key]
        if cached_cost <= best_cost_so_far:
            return cached_cost, cached_schedule

    if time_idx >= len(time_slots): return 0, schedule
    slot_str, slot_obj = time_slots[time_idx], parse_time_input(time_slots[time_idx], datetime(1970,1,1).date())
    avail_emps = availability.get(slot_str, [])
    positions_to_fill = WORK_POSITIONS[:len(avail_emps)]
    if len(positions_to_fill) != len(avail_emps): return float('inf'), None
    
    best_cost_at_level = best_cost_so_far
    best_schedule_for_rest_of_day = None

    for p in permutations(avail_emps):
        assignments = {pos: emp for pos, emp in zip(positions_to_fill, p)}
        
        # OPTIMIZATION: Aggressive Pruning - Check hard rule violations early
        is_hard_violation = False
        for pos, emp in assignments.items():
            state = prev_states.get(emp, {})
            last_pos, time_in_pos = state.get('last_pos'), state.get('time_in_pos', 0)
            if (pos == 'Conductor' and last_pos == 'Conductor' and time_in_pos >= 2) or \
               (pos not in LINE_BUSTER_ROLES and pos != 'Conductor' and last_pos == pos and time_in_pos >= 2):
                is_hard_violation = True
                break
        if is_hard_violation: continue

        current_cost = sum(calculate_assignment_cost(pos, emp, prev_states.get(emp, {}), slot_obj) for pos, emp in assignments.items())
        
        if current_cost >= best_cost_at_level:
            continue

        # OPTIMIZATION: Avoid deepcopy by creating a new state dict on the fly
        new_states = prev_states.copy()
        for pos, emp in assignments.items():
            state = prev_states.get(emp, {})
            history = state.get('history', [])
            new_history = (history + [pos])[-4:]
            time_in_top_tier = 0 if pos in TOP_TIER_ROLES else state.get('last_top_tier', 100) + 1
            new_states[emp] = {
                'last_pos': pos, 
                'time_in_pos': (state.get('time_in_pos', 0) + 1 if state.get('last_pos') == pos else 1), 
                'history': new_history, 
                'last_top_tier': time_in_top_tier
            }
        
        future_cost, resulting_schedule = solve_phoenix_recursive(time_idx + 1, time_slots, availability, schedule, new_states, best_cost_at_level - current_cost)
        
        if future_cost != float('inf'):
            total_cost = current_cost + future_cost
            if total_cost < best_cost_at_level:
                best_cost_at_level = total_cost
                resulting_schedule[time_idx] = assignments
                best_schedule_for_rest_of_day = resulting_schedule

    # OPTIMIZATION: Memoization - Store result in cache
    result = (best_cost_at_level, best_schedule_for_rest_of_day) if best_schedule_for_rest_of_day is not None else (float('inf'), None)
    memo_cache[state_key] = result
    
    return result

def create_schedule_phoenix(store_open_time_obj, store_close_time_obj, employee_data_list):
    # OPTIMIZATION: Reset Memoization Cache for each run
    global memo_cache
    memo_cache = {}

    df_long = preprocess_employee_data(employee_data_list)
    if df_long.empty: return "No employee data to process."
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    availability = {t: list(g['EmployeeNameFML']) for t, g in df_long[~df_long['IsOnBreak'] & ~df_long['IsOnToffTL']].groupby('Time')}
    
    total_cost, final_assignments = solve_phoenix_recursive(0, time_slots, availability, [{} for _ in time_slots], {}, float('inf'))

    if final_assignments is None: return "Could not find a valid schedule that meets all hard rules."
    
    note = ""
    if total_cost >= 1000: note = "NOTE: A valid schedule was only found by relaxing the consecutive Line Buster rule.\n\n"
    elif total_cost >= 500: note = "NOTE: A valid schedule was only found by relaxing the Conductor start time rule.\n\n"
    
    rows = []
    for i, slot_str in enumerate(time_slots):
        row = {"Time": slot_str, **final_assignments[i]}
        breaks = df_long[(df_long['Time'] == slot_str) & df_long['IsOnBreak']]['EmployeeNameFML'].tolist()
        tofftl = df_long[(df_long['Time'] == slot_str) & df_long['IsOnToffTL']]['EmployeeNameFML'].tolist()
        row["Break"], row["ToffTL"] = ", ".join(sorted(list(set(breaks)))), ", ".join(sorted(list(set(tofftl))))
        rows.append(row)
    out_df = pd.DataFrame(rows, columns=["Time"] + FINAL_SCHEDULE_ROW_ORDER).set_index("Time").fillna("").transpose().reset_index().rename(columns={'index':'Position'})
    return note + out_df.to_csv(index=False)

# ==============================================================================
# SECTION 4: PHOENIX (LIMITED CONDUCTOR BREAKS) - OPTIMIZED
# ==============================================================================
# This version also benefits from avoiding deepcopy and aggressive pruning.
# A separate memoization cache could be added if this function were called frequently.

def solve_phoenix_limited_breaks_recursive(time_idx, time_slots, availability, schedule, prev_states, best_cost_so_far, conductor_breaks_count):
    if time_idx >= len(time_slots): return 0, schedule
    slot_str, slot_obj = time_slots[time_idx], parse_time_input(time_slots[time_idx], datetime(1970,1,1).date())
    avail_emps = availability.get(slot_str, [])
    positions_to_fill = WORK_POSITIONS[:len(avail_emps)]
    if len(positions_to_fill) != len(avail_emps): return float('inf'), None
    
    best_cost_at_level = best_cost_so_far
    best_schedule_for_rest_of_day = None

    for p in permutations(avail_emps):
        assignments = {pos: emp for pos, emp in zip(positions_to_fill, p)}
        current_cost, is_valid = 0, True
        
        current_breaks = sum(1 for pos, emp in assignments.items() if pos == 'Conductor' and prev_states.get(emp, {}).get('last_pos') != 'Conductor' and slot_obj.minute != 0)
        
        if conductor_breaks_count + current_breaks > 2:
            continue

        # OPTIMIZATION: Aggressive Pruning
        for pos, emp in assignments.items():
            state = prev_states.get(emp, {})
            last_pos, time_in_pos = state.get('last_pos'), state.get('time_in_pos', 0)
            if (pos == 'Conductor' and last_pos == 'Conductor' and time_in_pos >= 2) or \
               (pos not in LINE_BUSTER_ROLES and pos != 'Conductor' and last_pos == pos and time_in_pos >= 2):
                is_valid = False
                break
        if not is_valid: continue

        current_cost = sum(calculate_assignment_cost(pos, emp, prev_states.get(emp, {}), slot_obj) for pos, emp in assignments.items())
        
        if current_cost >= best_cost_at_level:
            continue

        # OPTIMIZATION: Avoid deepcopy
        new_states = prev_states.copy()
        for pos, emp in assignments.items():
            state = prev_states.get(emp, {})
            history = state.get('history', [])
            new_history = (history + [pos])[-4:]
            time_in_top_tier = 0 if pos in TOP_TIER_ROLES else state.get('last_top_tier', 100) + 1
            new_states[emp] = {
                'last_pos': pos, 
                'time_in_pos': (state.get('time_in_pos',0)+1 if state.get('last_pos')==pos else 1), 
                'history': new_history, 
                'last_top_tier': time_in_top_tier
            }
        
        future_cost, resulting_schedule = solve_phoenix_limited_breaks_recursive(
            time_idx + 1, time_slots, availability, schedule, new_states, 
            best_cost_at_level - current_cost, conductor_breaks_count + current_breaks
        )
        
        if future_cost != float('inf'):
            total_cost = current_cost + future_cost
            if total_cost < best_cost_at_level:
                best_cost_at_level = total_cost
                resulting_schedule[time_idx] = assignments
                best_schedule_for_rest_of_day = resulting_schedule

    if best_schedule_for_rest_of_day is None:
        return float('inf'), None

    return best_cost_at_level, best_schedule_for_rest_of_day

def create_schedule_phoenix_limited(store_open_time_obj, store_close_time_obj, employee_data_list):
    df_long = preprocess_employee_data(employee_data_list)
    if df_long.empty: return "No employee data to process."
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    availability = {t: list(g['EmployeeNameFML']) for t, g in df_long[~df_long['IsOnBreak'] & ~df_long['IsOnToffTL']].groupby('Time')}
    
    total_cost, final_assignments = solve_phoenix_limited_breaks_recursive(0, time_slots, availability, [{} for _ in time_slots], {}, float('inf'), 0)

    if final_assignments is None: return "Could not find a valid schedule, even with up to 2 breaks of the Conductor start-time rule."
    
    note = "NOTE: The Conductor start time rule was broken to generate this schedule."
    
    rows = []
    for i, slot_str in enumerate(time_slots):
        row = {"Time": slot_str, **final_assignments[i]}
        breaks = df_long[(df_long['Time'] == slot_str) & df_long['IsOnBreak']]['EmployeeNameFML'].tolist()
        tofftl = df_long[(df_long['Time'] == slot_str) & df_long['IsOnToffTL']]['EmployeeNameFML'].tolist()
        row["Break"], row["ToffTL"] = ", ".join(sorted(list(set(breaks)))), ", ".join(sorted(list(set(tofftl))))
        rows.append(row)
    out_df = pd.DataFrame(rows, columns=["Time"] + FINAL_SCHEDULE_ROW_ORDER).set_index("Time").fillna("").transpose().reset_index().rename(columns={'index':'Position'})
    return note + out_df.to_csv(index=False)


# ==============================================================================
# SECTION 5: BACKTRACKING (CLASSIC) - OPTIMIZED
# ==============================================================================
def is_assignment_valid_backtracking_classic(assignments, time_slot_obj, prev_states):
    for pos, emp in assignments.items():
        last_pos, time_in_pos = prev_states.get(emp, {}).get('last_pos'), prev_states.get(emp, {}).get('time_in_pos', 0)
        if (pos in LINE_BUSTER_ROLES and last_pos in LINE_BUSTER_ROLES) or \
           (pos == 'Conductor' and last_pos == 'Conductor' and time_in_pos >= 2) or \
           (pos not in LINE_BUSTER_ROLES and pos != 'Conductor' and last_pos == pos and time_in_pos >= 2): return False
        if pos == 'Conductor' and last_pos != 'Conductor' and time_slot_obj.minute != 0: return False
    return True

def solve_classic_recursive(time_idx, time_slots, availability, schedule, states):
    if time_idx >= len(time_slots): return True, schedule
    slot_str, slot_obj = time_slots[time_idx], parse_time_input(time_slots[time_idx], datetime(1970,1,1).date())
    avail_emps = list(availability.get(slot_str, []))
    positions_to_fill = WORK_POSITIONS[:len(avail_emps)]
    if len(positions_to_fill) != len(avail_emps): return False, None
    for p in permutations(avail_emps):
        assignments = {pos: emp for pos, emp in zip(positions_to_fill, p)}
        if is_assignment_valid_backtracking_classic(assignments, slot_obj, states):
            # OPTIMIZATION: Avoid deepcopy
            new_states = states.copy()
            for pos, emp in assignments.items():
                state = states.get(emp, {})
                new_states[emp] = {
                    'last_pos': pos, 
                    'time_in_pos': (state.get('time_in_pos', 0) + 1 if state.get('last_pos') == pos else 1)
                }

            schedule[time_idx] = assignments
            is_solved, final_schedule = solve_classic_recursive(time_idx + 1, time_slots, availability, schedule, new_states)
            if is_solved: return True, final_schedule
    return False, None

def create_schedule_backtracking_classic(store_open_time_obj, store_close_time_obj, employee_data_list):
    df_long = preprocess_employee_data(employee_data_list)
    if df_long.empty: return "No employee data to process."
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    availability = {t: list(g['EmployeeNameFML']) for t, g in df_long[~df_long['IsOnBreak'] & ~df_long['IsOnToffTL']].groupby('Time')}
    is_solved, final_assignments = solve_classic_recursive(0, time_slots, availability, [{} for _ in time_slots], {})
    if not is_solved: return "Could not find a valid schedule that meets all hard rules."
    rows = []
    for i, slot_str in enumerate(time_slots):
        row = {"Time": slot_str, **final_assignments[i]}
        breaks = df_long[(df_long['Time'] == slot_str) & df_long['IsOnBreak']]['EmployeeNameFML'].tolist()
        tofftl = df_long[(df_long['Time'] == slot_str) & df_long['IsOnToffTL']]['EmployeeNameFML'].tolist()
        row["Break"] = ", ".join(sorted(list(set(breaks))))
        row["ToffTL"] = ", ".join(sorted(list(set(tofftl))))
        rows.append(row)
    out_df = pd.DataFrame(rows, columns=["Time"] + FINAL_SCHEDULE_ROW_ORDER).set_index("Time").fillna("").transpose().reset_index().rename(columns={'index':'Position'})
    return out_df.to_csv(index=False)

# ==============================================================================
# SECTION 6: CLASSIC (LIMITED CONDUCTOR BREAKS) - OPTIMIZED
# ==============================================================================
def solve_classic_limited_breaks_recursive(time_idx, time_slots, availability, schedule, states, conductor_breaks_count):
    if time_idx >= len(time_slots): return True, schedule
    slot_str, slot_obj = time_slots[time_idx], parse_time_input(time_slots[time_idx], datetime(1970,1,1).date())
    avail_emps = list(availability.get(slot_str, []))
    positions_to_fill = WORK_POSITIONS[:len(avail_emps)]
    if len(positions_to_fill) != len(avail_emps): return False, None
    
    for p in permutations(avail_emps):
        assignments = {pos: emp for pos, emp in zip(positions_to_fill, p)}
        
        is_valid = True
        current_breaks = 0
        for pos, emp in assignments.items():
            last_pos, time_in_pos = states.get(emp, {}).get('last_pos'), states.get(emp, {}).get('time_in_pos', 0)
            
            if (pos in LINE_BUSTER_ROLES and last_pos in LINE_BUSTER_ROLES) or \
               (pos == 'Conductor' and last_pos == 'Conductor' and time_in_pos >= 2) or \
               (pos not in LINE_BUSTER_ROLES and pos != 'Conductor' and last_pos == pos and time_in_pos >= 2):
                is_valid = False
                break
            
            if pos == 'Conductor' and last_pos != 'Conductor' and slot_obj.minute != 0:
                current_breaks += 1

        if not is_valid or (conductor_breaks_count + current_breaks > 2):
            continue

        # OPTIMIZATION: Avoid deepcopy
        new_states = states.copy()
        for pos, emp in assignments.items():
            state = states.get(emp, {})
            new_states[emp] = {
                'last_pos': pos,
                'time_in_pos': (state.get('time_in_pos',0)+1 if state.get('last_pos')==pos else 1)
            }
        
        schedule[time_idx] = assignments
        is_solved, final_schedule = solve_classic_limited_breaks_recursive(time_idx + 1, time_slots, availability, schedule, new_states, conductor_breaks_count + current_breaks)
        if is_solved: return True, final_schedule

    return False, None

def create_schedule_classic_limited(store_open_time_obj, store_close_time_obj, employee_data_list):
    df_long = preprocess_employee_data(employee_data_list)
    if df_long.empty: return "No employee data to process."
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    availability = {t: list(g['EmployeeNameFML']) for t, g in df_long[~df_long['IsOnBreak'] & ~df_long['IsOnToffTL']].groupby('Time')}
    
    is_solved, final_assignments = solve_classic_limited_breaks_recursive(0, time_slots, availability, [{} for _ in time_slots], {}, 0)

    if not is_solved: return "Could not find a valid schedule, even with up to 2 breaks of the Conductor start-time rule."
    
    note = "NOTE: The Conductor start time rule was broken to generate this schedule.\n\n"
    
    rows = []
    for i, slot_str in enumerate(time_slots):
        row = {"Time": slot_str, **final_assignments[i]}
        breaks = df_long[(df_long['Time'] == slot_str) & df_long['IsOnBreak']]['EmployeeNameFML'].tolist()
        tofftl = df_long[(df_long['Time'] == slot_str) & df_long['IsOnToffTL']]['EmployeeNameFML'].tolist()
        row["Break"] = ", ".join(sorted(list(set(breaks))))
        row["ToffTL"] = ", ".join(sorted(list(set(tofftl))))
        rows.append(row)
    out_df = pd.DataFrame(rows, columns=["Time"] + FINAL_SCHEDULE_ROW_ORDER).set_index("Time").fillna("").transpose().reset_index().rename(columns={'index':'Position'})
    return note + out_df.to_csv(index=False)

# ==============================================================================
# SECTION 7: PHOENIX (DIVERSE) - OPTIMIZED
# ==============================================================================
def is_swap_safe(df, time_idx, emp1_name, emp2_name, pos1, pos2, employee_schedule_map):
    # OPTIMIZATION: Uses the pre-computed employee_schedule_map for faster lookups
    def check_employee_validity(emp_name, new_pos, current_time_idx):
        # Check previous position
        if current_time_idx > 0:
            # Find the assignment for the previous time slot
            prev_assignment = next((item for item in reversed(employee_schedule_map[emp_name]) if item['time_idx'] < current_time_idx), None)
            if prev_assignment:
                last_pos = prev_assignment['pos']
                if new_pos in LINE_BUSTER_ROLES and new_pos == last_pos:
                    return False
                if new_pos not in LINE_BUSTER_ROLES and new_pos != 'Conductor' and new_pos == last_pos:
                    if current_time_idx > 1:
                        # Find the assignment before the previous one
                        prev_prev_assignment = next((item for item in reversed(employee_schedule_map[emp_name]) if item['time_idx'] < prev_assignment['time_idx']), None)
                        if prev_prev_assignment and prev_prev_assignment['pos'] == new_pos:
                            return False
        # Check next position
        if current_time_idx < len(df.columns) - 1:
            next_assignment = next((item for item in employee_schedule_map[emp_name] if item['time_idx'] > current_time_idx), None)
            if next_assignment:
                next_pos = next_assignment['pos']
                if next_pos in LINE_BUSTER_ROLES and next_pos == new_pos:
                    return False
        return True
    
    return check_employee_validity(emp1_name, pos2, time_idx) and check_employee_validity(emp2_name, pos1, time_idx)

def create_schedule_phoenix_diverse(store_open_time_obj, store_close_time_obj, employee_data_list):
    initial_schedule_csv = create_schedule_phoenix(store_open_time_obj, store_close_time_obj, employee_data_list)
    if "Could not find" in initial_schedule_csv or not employee_data_list:
        return initial_schedule_csv

    note, csv_data = (initial_schedule_csv.split('\n\n', 1) if "NOTE:" in initial_schedule_csv else ("", initial_schedule_csv))
    
    df = pd.read_csv(StringIO(csv_data)).set_index('Position')

    # OPTIMIZATION: Pre-process the schedule into an employee-centric map for fast lookups.
    employee_schedule_map = {emp: [] for emp in pd.unique(df.values.ravel()) if isinstance(emp, str) and emp}
    time_slot_map = {name: i for i, name in enumerate(df.columns)}
    for time_slot, i in time_slot_map.items():
        for pos in df.index:
            emp = df.loc[pos, time_slot]
            if isinstance(emp, str) and emp:
                employee_schedule_map[emp].append({'time_idx': i, 'time_str': time_slot, 'pos': pos})

    swaps_made = 0
    for _ in range(5): # Limit passes to prevent excessive processing
        made_a_swap_this_pass = False
        for time_idx, time_slot in enumerate(df.columns):
            for current_pos in df.index:
                if current_pos in ['Break', 'ToffTL', 'Conductor']: continue
                
                emp_name = df.loc[current_pos, time_slot]
                if not isinstance(emp_name, str) or not emp_name: continue
                
                # OPTIMIZATION: Use the map for faster pattern checking
                emp_history = [item for item in employee_schedule_map[emp_name] if item['time_idx'] <= time_idx]
                
                is_repetitive = False
                # Check for "on-off-on" pattern: e.g., Handout -> (off) -> Handout
                if len(emp_history) >= 2 and emp_history[-1]['pos'] == current_pos:
                    # Find last time they worked
                    last_work_idx = emp_history[-2]['time_idx']
                    if time_idx - last_work_idx > 1: # They had a gap
                       is_repetitive = True

                # Check for simple repetition in a recent window
                if not is_repetitive:
                    recent_positions = [h['pos'] for h in emp_history[-3:]]
                    if recent_positions.count(current_pos) > 1:
                        is_repetitive = True
                
                if is_repetitive:
                    for other_pos in df.index:
                        if other_pos == current_pos or other_pos in ['Break', 'ToffTL']: continue
                        
                        other_emp = df.loc[other_pos, time_slot]
                        if isinstance(other_emp, str) and other_emp and other_emp != emp_name:
                            if is_swap_safe(df, time_idx, emp_name, other_emp, current_pos, other_pos, employee_schedule_map):
                                # Perform the swap
                                df.loc[current_pos, time_slot], df.loc[other_pos, time_slot] = other_emp, emp_name
                                
                                # Update the map to reflect the swap
                                for item in employee_schedule_map[emp_name]:
                                    if item['time_idx'] == time_idx: item['pos'] = other_pos
                                for item in employee_schedule_map[other_emp]:
                                    if item['time_idx'] == time_idx: item['pos'] = current_pos
                                
                                swaps_made += 1
                                made_a_swap_this_pass = True
                                break 
                    if made_a_swap_this_pass: break
            if made_a_swap_this_pass: break
    
    if swaps_made > 0:
        note += f"{swaps_made} diversity swap(s) made. "

    return note.strip() + "\n\n" + df.reset_index().to_csv(index=False)
