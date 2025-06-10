# File: scheduler_logic.py
import pandas as pd
from io import StringIO
from datetime import datetime
from itertools import permutations
import copy

# ==============================================================================
# SECTION 1: COMPLEX (BACKTRACKING) SCHEDULER
# ==============================================================================

# --- CONFIGURATION FOR COMPLEX SCHEDULER ---
COMPLEX_WORK_POSITIONS = [
    "Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Greeter",
    "Drink Maker 1", "Drink Maker 2", "Line Buster 3"
]
COMPLEX_ALL_POSITIONS = COMPLEX_WORK_POSITIONS + ["Expo", "Break", "ToffTL"]
COMPLEX_LINE_BUSTER_ROLES = ["Line Buster 1", "Line Buster 2", "Line Buster 3"]
COMPLEX_POSITION_MAX_BLOCKS = {
    "default": 2, "Line Buster 1": 1, "Line Buster 2": 1, "Line Buster 3": 1
}

# --- HELPER: VALIDATION FOR COMPLEX SCHEDULER ---
def is_assignment_valid_complex(assignments, time_slot_obj, prev_states, relaxation_level):
    for pos, emp in assignments.items():
        emp_last_pos = prev_states.get(emp, {}).get('last_pos')
        emp_time_in_pos = prev_states.get(emp, {}).get('time_in_pos', 0)
        
        max_blocks = COMPLEX_POSITION_MAX_BLOCKS.get(pos, COMPLEX_POSITION_MAX_BLOCKS['default'])
        if pos == emp_last_pos and emp_time_in_pos >= max_blocks:
            return False

    if relaxation_level < 2:
        for pos, emp in assignments.items():
            if pos == 'Conductor' and prev_states.get(emp, {}).get('last_pos') != 'Conductor':
                if time_slot_obj.minute != 0: return False
    return True

# --- HELPER: RECURSIVE SOLVER FOR COMPLEX SCHEDULER ---
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

# --- MAIN FUNCTION FOR COMPLEX SCHEDULER ---
def create_schedule_complex(store_open_time_obj, store_close_time_obj, employee_data_list):
    df_long = preprocess_employee_data_to_long_format_generic(employee_data_list)
    if df_long.empty: return "No employee data to process."
    
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    employee_info = {t: [] for t in time_slots}
    for _, group in df_long.groupby('Time'):
        employee_info[group.name] = group.to_dict('records')

    for relaxation_level in range(3):
        is_solved, final_schedule_rows = solve_schedule_recursive(0, time_slots, employee_info, [{} for _ in time_slots], {}, relaxation_level)
        if is_solved:
            note = ""
            if relaxation_level == 1: note = "NOTE: A valid schedule was found by relaxing the paired position swapping rule.\n\n"
            elif relaxation_level == 2: note = "NOTE: A valid schedule was found by relaxing paired swapping AND the Conductor start time rule.\n\n"
            
            out_df = pd.DataFrame(final_schedule_rows, columns=["Time"] + COMPLEX_ALL_POSITIONS).fillna("")
            out_df["Break"] = out_df["Break"].apply(lambda x: ", ".join(sorted(x)) if isinstance(x, list) else x)
            out_df["ToffTL"] = out_df["ToffTL"].apply(lambda x: ", ".join(sorted(x)) if isinstance(x, list) else x)

            final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
            return note + final_df.to_csv(index=False)

    return "Could not find a valid schedule, even after relaxing all possible rules."


# ==============================================================================
# SECTION 2: SIMPLE (GREEDY) SCHEDULER
# This is the logic you provided in the last prompt.
# ==============================================================================

def create_schedule_simple(store_open_time_obj, store_close_time_obj, employee_data_list):
    REF_DATE_FOR_PARSING = datetime(1970, 1, 1).date()
    df = preprocess_employee_data_to_long_format_simple(employee_data_list, REF_DATE_FOR_PARSING)
    if df.empty: return "No employee slots generated from input."

    positions_ordered = ["Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Expo", "Drink Maker 1", "Drink Maker 2", "Line Buster 3", "Break", "ToffTL"]
    work_positions_priority_order = ["Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Expo", "Drink Maker 1", "Drink Maker 2", "Line Buster 3"]
    line_buster_roles = ["Line Buster 1", "Line Buster 2", "Line Buster 3"]
    paired_position_defs = {
        "HLB1": {"pos1": "Handout", "pos2": "Line Buster 1", "emps": (None, None), "emp1_is_pos1_in_first_half": True, "slots_done_this_hour": 0, "is_broken_this_hour": False},
        "LB2E": {"pos1": "Line Buster 2", "pos2": "Expo", "emps": (None, None), "emp1_is_pos1_in_first_half": True, "slots_done_this_hour": 0, "is_broken_this_hour": False}
    }
    
    time_map = {ts: parse_time_input(ts, REF_DATE_FOR_PARSING) for ts in df['Time'].unique()}
    all_slots_str = sorted(df['Time'].unique(), key=lambda t: time_map.get(t))

    emp_info_map = {t: [] for t in all_slots_str}
    for _, r in df.iterrows(): 
        emp_info_map[r['Time']].append({"name":r['EmployeeNameFML'], "role_scheduled_as":str(r['Position Scheduled As']).strip(), "is_unpaid_break":str(r['Unpaid Break']).strip().upper() in ['TRUE','YES','1','X','T']})

    schedule_rows, emp_lb_last, emp_cur_pos, emp_time_cur_pos, emp_last_time_spec_pos, g_time_step = [], {}, {}, {}, {}, 0

    for time_slot in all_slots_str:
        g_time_step += 1
        cur_assigns = {p: "" for p in positions_ordered}
        cur_assigns["Break"], cur_assigns["ToffTL"] = [], []
        
        active_emps_details = sorted(emp_info_map.get(time_slot, []), key=lambda x: x['name'])
        processed_this_slot = set()
        avail_for_work = []

        for emp_d in active_emps_details:
            emp_n = emp_d["name"]
            if emp_d["is_unpaid_break"]:
                processed_this_slot.add(emp_n); cur_assigns["Break"].append(emp_n)
                emp_lb_last[emp_n]=False; emp_cur_pos[emp_n]=None; emp_time_cur_pos[emp_n]=0
            elif emp_d["role_scheduled_as"] == "ToffTL":
                processed_this_slot.add(emp_n); cur_assigns["ToffTL"].append(emp_n)
                emp_lb_last[emp_n]=False; emp_cur_pos[emp_n]=None; emp_time_cur_pos[emp_n]=0
            else:
                avail_for_work.append(emp_n)

        # This is a simplified version of the complex logic from your provided file
        for pos in work_positions_priority_order:
            best_candidate = None
            eligible = [e for e in avail_for_work if e not in processed_this_slot]
            
            # Very basic LRU (Least Recently Used) logic
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

        row_data = {"Time": time_slot, **cur_assigns}
        schedule_rows.append(row_data)

    if not schedule_rows: return "No schedule data."
    out_df = pd.DataFrame(schedule_rows, columns=["Time"] + positions_ordered)
    final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
    return final_df.to_csv(index=False)


# ==============================================================================
# SECTION 3: GENERIC PREPROCESSING FUNCTIONS
# Used by one or both schedulers.
# ==============================================================================

def parse_time_input(time_val, ref_date):
    if pd.isna(time_val) or str(time_val).strip().upper() in ['N/A', '']: return pd.NaT
    try: return pd.to_datetime(f"{ref_date.strftime('%Y-%m-%d')} {str(time_val).strip()}")
    except ValueError: return pd.NaT

def preprocess_employee_data_to_long_format_generic(employee_data_list):
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
                all_slots.append({'Time': curr.strftime('%I:%M %p').lstrip('0'), 'EmployeeNameFML': name, 'IsOnBreak': on_break, 'IsOnToffTL': on_tofftl})
                curr += pd.Timedelta(minutes=30)
    return pd.DataFrame(all_slots) if all_slots else pd.DataFrame()

def preprocess_employee_data_to_long_format_simple(employee_data_list, ref_date_for_parsing):
    # This is the preprocessing function from your provided "simple" logic file
    all_employee_slots = []
    activity_definitions = {"ToffTL": ("ToffTL Start", "ToffTL End")}
    for emp_data in employee_data_list:
        name_str = emp_data.get('Name', '')
        first_name, last_name_part = (name_str.split(" ", 1) + [""])[:2] if " " in name_str else (name_str, "")
        emp_name_fml = f"{first_name} {last_name_part[0] + '.' if last_name_part else ''}".strip()
        shift_start_dt = parse_time_input(emp_data.get('Shift Start'), ref_date_for_parsing)
        shift_end_dt = parse_time_input(emp_data.get('Shift End'), ref_date_for_parsing)
        if pd.notna(shift_start_dt) and pd.notna(shift_end_dt) and shift_end_dt < shift_start_dt:
            shift_end_dt += pd.Timedelta(days=1)
        activity_times = {}
        for internal_key, (start_col_key, end_col_key) in activity_definitions.items():
            s_dt = parse_time_input(emp_data.get(start_col_key), ref_date_for_parsing)
            e_dt = parse_time_input(emp_data.get(end_col_key), ref_date_for_parsing)
            if pd.notna(s_dt) and pd.notna(e_dt) and e_dt < s_dt: e_dt += pd.Timedelta(days=1)
            activity_times[internal_key] = (s_dt, e_dt)
        unpaid_break_start_dt = parse_time_input(emp_data.get('Break'), ref_date_for_parsing)
        unpaid_break_end_dt = None
        if pd.notna(unpaid_break_start_dt):
            unpaid_break_end_dt = unpaid_break_start_dt + pd.Timedelta(minutes=30)
        if pd.notna(shift_start_dt) and pd.notna(shift_end_dt):
            current_time_slot_start = shift_start_dt
            while current_time_slot_start < shift_end_dt:
                slot_time_str = current_time_slot_start.strftime('%I:%M %p').lstrip('0')
                position_scheduled_as = "Available" 
                is_unpaid_break_str = "FALSE" 
                if pd.notna(activity_times['ToffTL'][0]) and pd.notna(activity_times['ToffTL'][1]) and \
                activity_times['ToffTL'][0] <= current_time_slot_start < activity_times['ToffTL'][1]:
                    position_scheduled_as = "ToffTL"
                if pd.notna(unpaid_break_start_dt) and pd.notna(unpaid_break_end_dt) and \
                unpaid_break_start_dt <= current_time_slot_start < unpaid_break_end_dt:
                    is_unpaid_break_str = "TRUE"
                all_employee_slots.append({
                    'Time': slot_time_str, 'EmployeeNameFML': emp_name_fml,
                    'Position Scheduled As': position_scheduled_as, 'Unpaid Break': is_unpaid_break_str 
                })
                current_time_slot_start += pd.Timedelta(minutes=30)
    return pd.DataFrame(all_employee_slots, columns=['Time','EmployeeNameFML','Position Scheduled As','Unpaid Break']) if all_employee_slots else pd.DataFrame(columns=['Time','EmployeeNameFML','Position Scheduled As','Unpaid Break'])
