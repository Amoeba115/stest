# File: scheduler_logic.py
import pandas as pd
from io import StringIO
from datetime import datetime
from itertools import permutations
import copy

# ==============================================================================
# SECTION 1: SHARED HELPER FUNCTIONS
# ==============================================================================

def parse_time_input(time_val, ref_date):
    """Parses various time string formats into a datetime object."""
    if pd.isna(time_val) or str(time_val).strip().upper() in ['N/A', '']:
        return pd.NaT
    try:
        return pd.to_datetime(f"{ref_date.strftime('%Y-%m-%d')} {str(time_val).strip()}")
    except ValueError:
        return pd.NaT

def preprocess_employee_data(employee_data_list):
    """A robust, shared function to convert employee data into a long-format DataFrame."""
    all_slots = []
    ref_date = datetime(1970, 1, 1).date()
    for emp_data in employee_data_list:
        name_parts = emp_data.get('Name', '').split(' ', 1)
        name = f"{name_parts[0]} {name_parts[1][0] if len(name_parts) > 1 and name_parts[1] else ''}.".strip()
        
        s_start = parse_time_input(emp_data.get('Shift Start'), ref_date)
        s_end = parse_time_input(emp_data.get('Shift End'), ref_date)
        b_start = parse_time_input(emp_data.get('Break'), ref_date)
        b_end = b_start + pd.Timedelta(minutes=30) if pd.notna(b_start) else None
        t_start = parse_time_input(emp_data.get('ToffTL Start'), ref_date)
        t_end = parse_time_input(emp_data.get('ToffTL End'), ref_date)
        
        if pd.notna(s_start) and pd.notna(s_end):
            curr = s_start
            while curr < s_end:
                on_break = pd.notna(b_start) and b_start <= curr < b_end
                on_tofftl = pd.notna(t_start) and t_start <= curr < t_end
                
                position_scheduled_as = "ToffTL" if on_tofftl else "Available"
                is_unpaid_break_str = "TRUE" if on_break else "FALSE"

                all_slots.append({
                    'Time': curr.strftime('%I:%M %p').lstrip('0'),
                    'EmployeeNameFML': name,
                    'IsOnBreak': on_break,
                    'IsOnToffTL': on_tofftl,
                    'Position Scheduled As': position_scheduled_as,
                    'Unpaid Break': is_unpaid_break_str
                })
                curr += pd.Timedelta(minutes=30)
    return pd.DataFrame(all_slots) if all_slots else pd.DataFrame()


# ==============================================================================
# SECTION 2: COMPLEX (BACKTRACKING) SCHEDULER
# ==============================================================================
COMPLEX_WORK_POSITIONS = ["Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Greeter", "Drink Maker 1", "Drink Maker 2", "Line Buster 3"]
COMPLEX_ALL_POSITIONS = COMPLEX_WORK_POSITIONS + ["Expo", "Break", "ToffTL"]

def is_assignment_valid_complex(assignments, time_slot_obj, prev_states, relaxation_level):
    """Checks if a proposed set of assignments is valid based on the relaxation level."""
    # --- Hard Rules (Enforced on all Tiers) ---
    for pos, emp in assignments.items():
        emp_last_pos = prev_states.get(emp, {}).get('last_pos')
        emp_time_in_pos = prev_states.get(emp, {}).get('time_in_pos', 0)
        
        # HARD RULE: Conductor hour and start time.
        if pos == 'Conductor':
            if emp_last_pos == 'Conductor' and emp_time_in_pos >= 2: return False # Max 1 hour
            if emp_last_pos != 'Conductor' and time_slot_obj.minute != 0: return False # Must start on hour
        
        # HARD RULE: Line Buster 30-minute limit.
        if pos in ["Line Buster 1", "Line Buster 2", "Line Buster 3"]:
            if emp_last_pos == pos and emp_time_in_pos >= 1: return False # Max 30 mins

    # --- Relaxable Rules ---
    # RELAXABLE RULE (Tier 2+): Max duration for other positions (e.g., Drink Maker)
    if relaxation_level < 2:
        for pos, emp in assignments.items():
            # This rule applies to positions not covered by the hard rules above
            if pos not in ["Conductor", "Line Buster 1", "Line Buster 2", "Line Buster 3"]:
                emp_last_pos = prev_states.get(emp, {}).get('last_pos')
                emp_time_in_pos = prev_states.get(emp, {}).get('time_in_pos', 0)
                if pos == emp_last_pos and emp_time_in_pos >= 2: # 1-hour max
                    return False

    # RELAXABLE RULE (Tier 1+): Paired position swapping
    if relaxation_level < 1:
        # Placeholder for strict pair-swapping logic.
        pass

    return True

def solve_schedule_recursive(time_slot_index, time_slots, employee_info, schedule, employee_states, relaxation_level):
    if time_slot_index >= len(time_slots): return True, schedule
    current_slot_str = time_slots[time_slot_index]
    current_slot_obj = parse_time_input(current_slot_str, datetime(1970, 1, 1).date())
    available_for_work, current_assignments = [], {"Break": [], "ToffTL": []}
    for emp_details in employee_info[current_slot_str]:
        if emp_details['IsOnBreak']: current_assignments["Break"].append(emp_details['EmployeeNameFML'])
        elif emp_details['IsOnToffTL']: current_assignments["ToffTL"].append(emp_details['EmployeeNameFML'])
        else: available_for_work.append(emp_details['EmployeeNameFML'])
    num_positions_to_fill = len(available_for_work)
    positions_to_fill = COMPLEX_WORK_POSITIONS[:num_positions_to_fill]
    if len(positions_to_fill) != len(available_for_work): return False, None
    for p in permutations(available_for_work):
        tentative_assignments = {pos: emp for pos, emp in zip(positions_to_fill, p)}
        if is_assignment_valid_complex(tentative_assignments, current_slot_obj, employee_states, relaxation_level):
            new_states = copy.deepcopy(employee_states)
            final_assignments = {**current_assignments, **tentative_assignments}
            for pos, emp in tentative_assignments.items():
                time_in_pos = (new_states.get(emp, {}).get('time_in_pos', 0) + 1) if new_states.get(emp, {}).get('last_pos') == pos else 1
                new_states[emp] = {'last_pos': pos, 'time_in_pos': time_in_pos}
            schedule[time_slot_index] = {"Time": current_slot_str, **final_assignments}
            is_solved, final_schedule = solve_schedule_recursive(time_slot_index + 1, time_slots, employee_info, schedule, new_states, relaxation_level)
            if is_solved: return True, final_schedule
    return False, None

def create_schedule_complex(store_open_time_obj, store_close_time_obj, employee_data_list):
    df_long = preprocess_employee_data(employee_data_list)
    if df_long.empty: return "No employee data to process."
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    employee_info = {t: g.to_dict('records') for t, g in df_long.groupby('Time')}
    
    # 3-Tiered relaxation approach
    for relaxation_level in range(3):
        is_solved, final_schedule_rows = solve_schedule_recursive(0, time_slots, employee_info, [{} for _ in time_slots], {}, relaxation_level)
        if is_solved:
            note = ""
            if relaxation_level == 1:
                note = "NOTE: A valid schedule was found by relaxing the paired position swapping rule.\n\n"
            elif relaxation_level == 2:
                note = "NOTE: A valid schedule was found by relaxing swapping rules AND general position time limits.\n\n"
            
            out_df = pd.DataFrame(final_schedule_rows, columns=["Time"] + COMPLEX_ALL_POSITIONS).fillna("")
            for col in ["Break", "ToffTL"]:
                out_df[col] = out_df[col].apply(lambda x: ", ".join(sorted(x)) if isinstance(x, list) else x)
            final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
            return note + final_df.to_csv(index=False)
            
    return "Could not find a valid schedule, even after relaxing all possible soft rules."


# ==============================================================================
# SECTION 3: SIMPLE (GREEDY) SCHEDULER
# ==============================================================================
def create_schedule_simple(store_open_time_obj, store_close_time_obj, employee_data_list):
    df = preprocess_employee_data(employee_data_list)
    if df.empty: return "No employee slots generated from input."

    positions_ordered = ["Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Expo", "Drink Maker 1", "Drink Maker 2", "Line Buster 3", "Break", "ToffTL"]
    work_positions_priority_order = ["Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Expo", "Drink Maker 1", "Drink Maker 2", "Line Buster 3"]
    line_buster_roles = ["Line Buster 1", "Line Buster 2", "Line Buster 3"]
    
    time_map = {ts: parse_time_input(ts, datetime(1970, 1, 1).date()) for ts in df['Time'].unique()}
    all_slots_str = sorted(df['Time'].unique(), key=lambda t: time_map.get(t))

    emp_info_map = {t: g.to_dict('records') for t, g in df.groupby('Time')}
    schedule_rows, emp_lb_last, emp_cur_pos, emp_last_time_spec_pos, g_time_step = [], {}, {}, {}, 0

    for time_slot in all_slots_str:
        g_time_step += 1
        cur_assigns = {p: "" for p in positions_ordered}
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
            else:
                avail_for_work.append(emp_n)
        
        for pos in work_positions_priority_order:
            best_candidate = None
            eligible = [e for e in avail_for_work if e not in processed_this_slot]
            min_last_time = float('inf')
            for emp in eligible:
                if pos in line_buster_roles and emp_lb_last.get(emp, False): continue
                last_time = emp_last_time_spec_pos.get(emp, {}).get(pos, -1)
                if last_time < min_last_time:
                    min_last_time = last_time
                    best_candidate = emp
            
            if best_candidate:
                cur_assigns[pos] = best_candidate
                processed_this_slot.add(best_candidate)
                emp_lb_last[best_candidate] = (pos in line_buster_roles)
                emp_cur_pos[best_candidate] = pos
                emp_last_time_spec_pos.setdefault(best_candidate, {})[pos] = g_time_step

        schedule_rows.append({"Time": time_slot, **cur_assigns})

    if not schedule_rows: return "No schedule data."
    out_df = pd.DataFrame(schedule_rows, columns=["Time"] + positions_ordered)
    final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
    return final_df.to_csv(index=False)
