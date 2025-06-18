# File: scheduler_logic.py
import pandas as pd
from io import StringIO
from datetime import datetime, time
import copy
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
# SECTION 2: ROTATIONAL SCHEDULER
# ==============================================================================
def create_schedule_rotational(store_open_time_obj, store_close_time_obj, employee_data_list):
    df_long = preprocess_employee_data(employee_data_list)
    if df_long.empty: return "No data."
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    availability = {t: sorted(list(g['EmployeeNameFML'])) for t, g in df_long[~df_long['IsOnBreak'] & ~df_long['IsOnToffTL']].groupby('Time')}
    schedule = {t: {p: ("" if p not in ["Break", "ToffTL"] else []) for p in FINAL_SCHEDULE_ROW_ORDER} for t in time_slots}
    employee_states = {emp: {'last_pos_idx': -1} for emp in df_long['EmployeeNameFML'].unique()}
    for i, slot_str in enumerate(time_slots):
        for _, row in df_long[df_long['Time'] == slot_str].iterrows():
            if row['IsOnBreak']: schedule[slot_str]['Break'].append(row['EmployeeNameFML'])
            if row['IsOnToffTL']: schedule[slot_str]['ToffTL'].append(row['EmployeeNameFML'])
        if i > 0:
            prev_conductor = schedule[time_slots[i-1]]['Conductor']
            is_new_conductor_block = (i < 2 or schedule[time_slots[i-2]]['Conductor'] != prev_conductor)
            if prev_conductor and is_new_conductor_block and prev_conductor in availability.get(slot_str, []):
                schedule[slot_str]['Conductor'] = prev_conductor
        assigned_in_slot = {v for v in schedule[slot_str].values() if isinstance(v, str) and v}
        available_and_unassigned = [emp for emp in availability.get(slot_str, []) if emp not in assigned_in_slot]
        if not available_and_unassigned: continue
        for pos_idx, pos in enumerate(WORK_POSITIONS):
            if not available_and_unassigned: break
            if schedule[slot_str][pos]: continue
            best_candidate, ideal_candidate = None, None
            target_prev_idx = (pos_idx - 1 + len(WORK_POSITIONS)) % len(WORK_POSITIONS)
            for emp in available_and_unassigned:
                if employee_states.get(emp, {}).get('last_pos_idx') == target_prev_idx:
                    ideal_candidate = emp
                    break
            best_candidate = ideal_candidate if ideal_candidate else available_and_unassigned[0]
            schedule[slot_str][pos] = best_candidate
            available_and_unassigned.remove(best_candidate)
            if pos == 'Conductor' and i + 1 < len(time_slots):
                next_slot_str = time_slots[i+1]
                if best_candidate in availability.get(next_slot_str, []) and not schedule[next_slot_str]['Conductor']:
                    schedule[next_slot_str]['Conductor'] = best_candidate
        for p_idx, p_name in enumerate(WORK_POSITIONS):
            emp = schedule[slot_str].get(p_name)
            if emp: employee_states[emp]['last_pos_idx'] = p_idx
    schedule_rows = [{"Time": time, **positions} for time, positions in schedule.items()]
    out_df = pd.DataFrame(schedule_rows, columns=["Time"] + FINAL_SCHEDULE_ROW_ORDER).fillna("")
    for col in ["Break", "ToffTL"]: out_df[col] = out_df[col].apply(lambda x: ", ".join(sorted(list(set(x)))) if isinstance(x, list) else x)
    final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
    return final_df.to_csv(index=False)

# ==============================================================================
# SECTION 3: HEURISTIC (CONDUCTOR FIRST) SCHEDULER
# ==============================================================================
def create_schedule_heuristic(store_open_time_obj, store_close_time_obj, employee_data_list):
    # This logic remains unchanged
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
                if (pos in LINE_BUSTER_ROLES and last_pos == pos and time_in_pos >= 1) or \
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
# SECTION 4: BACKTRACKING (MOST STRICT) SCHEDULER (IMPROVED)
# ==============================================================================
def is_assignment_valid_backtracking(assignments, time_slot_obj, prev_states):
    # This function now only checks the absolute hard rules
    for pos, emp in assignments.items():
        state = prev_states.get(emp, {})
        last_pos, time_in_pos = state.get('last_pos'), state.get('time_in_pos', 0)
        
        if (pos in LINE_BUSTER_ROLES and last_pos == pos and time_in_pos >= 1) or \
           (pos == 'Conductor' and last_pos == 'Conductor' and time_in_pos >= 2) or \
           (pos not in LINE_BUSTER_ROLES and pos != 'Conductor' and last_pos == pos and time_in_pos >= 2):
            return False
        if pos == 'Conductor' and last_pos != 'Conductor' and time_slot_obj.minute != 0:
            return False
    return True

def solve_recursive(time_idx, time_slots, availability, schedule, states):
    if time_idx >= len(time_slots): return True, schedule
    slot_str, slot_obj = time_slots[time_idx], parse_time_input(time_slots[time_idx], datetime(1970,1,1).date())
    avail_emps = list(availability.get(slot_str, []))
    positions_to_fill = WORK_POSITIONS[:len(avail_emps)]
    if len(positions_to_fill) != len(avail_emps): return False, None
    
    # --- NEW: Separate permutations into preferred (no ABAB) and last resort (allows ABAB) ---
    preferred_perms, last_resort_perms = [], []
    for p in permutations(avail_emps):
        assignments = {pos: emp for pos, emp in zip(positions_to_fill, p)}
        is_abab = False
        for pos, emp in assignments.items():
            history = states.get(emp, {}).get('history', [])
            if len(history) == 4 and history[0] == history[2] and history[1] == history[3] and history[2] == pos:
                is_abab = True
                break
        if is_abab:
            last_resort_perms.append(p)
        else:
            preferred_perms.append(p)

    # --- Try preferred permutations first, then the last resort ones ---
    for p in preferred_perms + last_resort_perms:
        assignments = {pos: emp for pos, emp in zip(positions_to_fill, p)}
        if is_assignment_valid_backtracking(assignments, slot_obj, states):
            new_states = copy.deepcopy(states)
            for pos, emp in assignments.items():
                history = new_states.get(emp, {}).get('history', [])
                new_history = (history + [pos])[-4:]
                new_states[emp] = {
                    'last_pos': pos, 
                    'time_in_pos': (states.get(emp,{}).get('time_in_pos',0)+1 if states.get(emp,{}).get('last_pos')==pos else 1),
                    'history': new_history
                }
            schedule[time_idx] = assignments
            is_solved, final_schedule = solve_recursive(time_idx + 1, time_slots, availability, schedule, new_states)
            if is_solved: return True, final_schedule
            
    return False, None

def create_schedule_backtracking(store_open_time_obj, store_close_time_obj, employee_data_list):
    df_long = preprocess_employee_data(employee_data_list)
    if df_long.empty: return "No employee data to process."
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    availability = {t: list(g['EmployeeNameFML']) for t, g in df_long[~df_long['IsOnBreak'] & ~df_long['IsOnToffTL']].groupby('Time')}
    is_solved, final_assignments = solve_recursive(0, time_slots, availability, [{} for _ in time_slots], {})
    if not is_solved: return "Could not find a valid schedule that meets all hard rules."
    note = ""
    # Check if the final solution contains an ABAB pattern to notify the user
    final_states = {}
    for i in range(len(time_slots)):
        slot_assignments = final_assignments[i]
        for pos, emp in slot_assignments.items():
            history = final_states.get(emp, {}).get('history', [])
            if len(history) == 4 and history[0] == history[2] and history[1] == history[3] and history[2] == pos:
                note = "NOTE: A valid schedule was only found by allowing some employees to alternate between two positions for over 2 hours.\n\n"
            new_history = (history + [pos])[-4:]
            final_states[emp] = {'history': new_history}
    
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
# SECTION 5: SIMPLE (GREEDY) SCHEDULER
# ==============================================================================
def create_schedule_simple(store_open_time_obj, store_close_time_obj, employee_data_list):
    # This logic remains unchanged
    df = preprocess_employee_data(employee_data_list)
    if df.empty: return "No employee slots generated from input."
    time_map = {ts: parse_time_input(ts, datetime(1970, 1, 1).date()) for ts in df['Time'].unique()}
    all_slots_str = sorted(df['Time'].unique(), key=lambda t: time_map.get(t))
    emp_info_map = {t: g.to_dict('records') for t, g in df.groupby('Time')}
    schedule_rows, emp_lb_last, emp_cur_pos, emp_last_time_spec_pos, g_time_step = [], {}, {}, {}, 0
    for time_slot in all_slots_str:
        g_time_step += 1
        cur_assigns = {p: "" for p in FINAL_SCHEDULE_ROW_ORDER}
        cur_assigns["Break"], cur_assigns["ToffTL"] = [], []
        active_emps_details = sorted(emp_info_map.get(time_slot, []), key=lambda x: x['EmployeeNameFML'])
        processed_this_slot, avail_for_work = set(), []
        for emp_d in active_emps_details:
            emp_n = emp_d["EmployeeNameFML"]
            if emp_d["IsOnBreak"]:
                processed_this_slot.add(emp_n); cur_assigns["Break"].append(emp_n)
                emp_lb_last[emp_n]=False; emp_cur_pos[emp_n]=None
            elif emp_d["IsOnToffTL"]:
                processed_this_slot.add(emp_n); cur_assigns["ToffTL"].append(emp_n)
                emp_lb_last[emp_n]=False; emp_cur_pos[emp_n]=None
            else: avail_for_work.append(emp_n)
        for pos in WORK_POSITIONS:
            best_candidate, min_last_time = None, float('inf')
            for emp in [e for e in avail_for_work if e not in processed_this_slot]:
                if pos in LINE_BUSTER_ROLES and emp_lb_last.get(emp, False): continue
                last_time = emp_last_time_spec_pos.get(emp, {}).get(pos, -1)
                if last_time < min_last_time:
                    min_last_time, best_candidate = last_time, emp
            if best_candidate:
                cur_assigns[pos] = best_candidate
                processed_this_slot.add(best_candidate)
                emp_lb_last[best_candidate] = (pos in LINE_BUSTER_ROLES)
                emp_cur_pos[best_candidate] = pos
                emp_last_time_spec_pos.setdefault(best_candidate, {})[pos] = g_time_step
        schedule_rows.append({"Time": time_slot, **cur_assigns})
    out_df = pd.DataFrame(schedule_rows, columns=["Time"] + FINAL_SCHEDULE_ROW_ORDER)
    final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
    return final_df.to_csv(index=False)
