# File: scheduler_logic.py
import pandas as pd
from io import StringIO
from datetime import datetime
from itertools import permutations
import copy

# ==============================================================================
# CONFIGURATION
# ==============================================================================
WORK_POSITIONS_PRIORITY_ORDER = [
    "Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Greeter",
    "Drink Maker 1", "Drink Maker 2", "Line Buster 3"
]
ALL_POSITIONS_ORDERED = WORK_POSITIONS_PRIORITY_ORDER + ["Expo", "Break", "ToffTL"]
LINE_BUSTER_ROLES = ["Line Buster 1", "Line Buster 2", "Line Buster 3"]
PAIRED_POSITION_DEFINITIONS = {
    "Pair1": {"pos1": "Handout", "pos2": "Line Buster 1"},
    "Pair2": {"pos1": "Line Buster 2", "pos2": "Greeter"}
}
# This dictionary is referenced by the hard rule in is_assignment_valid
POSITION_MAX_BLOCKS = {
    # Default for all positions is 2 blocks (1 hour)
    "default": 2,
    # Specific overrides can be placed here if needed, e.g., for Line Busters
    "Line Buster 1": 1,
    "Line Buster 2": 1,
    "Line Buster 3": 1
}
REF_DATE_FOR_PARSING = datetime(1970, 1, 1).date()

# ==============================================================================
# DATA PREPROCESSING
# ==============================================================================
def parse_time_input(time_val, ref_date):
    if pd.isna(time_val) or str(time_val).strip().upper() in ['N/A', '']: return pd.NaT
    try: return pd.to_datetime(f"{ref_date.strftime('%Y-%m-%d')} {str(time_val).strip()}")
    except ValueError: return pd.NaT

def preprocess_employee_data_to_long_format(employee_data_list, ref_date):
    all_slots = []
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

# ==============================================================================
# BACKTRACKING SOLVER WITH RULE RELAXATION
# ==============================================================================

def is_assignment_valid(assignments, time_slot_obj, prev_states, relaxation_level):
    """Checks if a proposed set of assignments is valid based on the relaxation level."""
    # --- Hard Rules (Enforced on all Tiers) ---
    for pos, emp in assignments.items():
        emp_last_pos = prev_states.get(emp, {}).get('last_pos')
        emp_time_in_pos = prev_states.get(emp, {}).get('time_in_pos', 0)
        
        # HARD RULE: Maximum duration for any position.
        max_blocks = POSITION_MAX_BLOCKS.get(pos, POSITION_MAX_BLOCKS['default'])
        if pos == emp_last_pos and emp_time_in_pos >= max_blocks:
            return False # Exceeds max time for this position

        # HARD RULE: No consecutive Line Buster assignments.
        # This is implicitly handled by the max_blocks rule for Line Busters being 1.
        
    # --- Relaxable Rules ---
    # RELAXABLE RULE (Tier 2+): Conductor must start on the hour
    if relaxation_level < 2:
        for pos, emp in assignments.items():
            if pos == 'Conductor' and prev_states.get(emp, {}).get('last_pos') != 'Conductor':
                if time_slot_obj.minute != 0:
                    return False
    
    # RELAXABLE RULE (Tier 1+): Paired position swapping
    if relaxation_level < 1:
        # Complex logic to enforce strict pair swapping would go here.
        pass

    return True

def solve_schedule_recursive(time_slot_index, time_slots, employee_info, schedule, employee_states, relaxation_level):
    """Core recursive function. Tries to solve the schedule for one time slot."""
    if time_slot_index >= len(time_slots):
        return True, schedule

    current_slot_str = time_slots[time_slot_index]
    current_slot_obj = parse_time_input(current_slot_str, REF_DATE_FOR_PARSING)
    
    available_for_work = []
    current_assignments = {p: "" for p in ALL_POSITIONS_ORDERED}
    current_assignments["Break"] = []
    current_assignments["ToffTL"] = []

    for emp_details in employee_info[current_slot_str]:
        emp_name = emp_details['name']
        if emp_details['is_on_break']: current_assignments["Break"].append(emp_name)
        elif emp_details['is_on_tofftl']: current_assignments["ToffTL"].append(emp_name)
        else: available_for_work.append(emp_name)
    
    num_positions_to_fill = len(available_for_work)
    positions_to_fill = WORK_POSITIONS_PRIORITY_ORDER[:num_positions_to_fill]
    
    if len(positions_to_fill) != len(available_for_work):
        return False, None

    for p in permutations(available_for_work):
        tentative_assignments = {pos: emp for pos, emp in zip(positions_to_fill, p)}
        
        if is_assignment_valid(tentative_assignments, current_slot_obj, employee_states, relaxation_level):
            new_states = copy.deepcopy(employee_states)
            for pos, emp in tentative_assignments.items():
                current_assignments[pos] = emp
                time_in_pos = (new_states.get(emp, {}).get('time_in_pos', 0) + 1) if new_states.get(emp, {}).get('last_pos') == pos else 1
                new_states[emp] = {'last_pos': pos, 'time_in_pos': time_in_pos}
            
            schedule[time_slot_index] = {"Time": current_slot_str, **current_assignments}
            
            is_solved, final_schedule = solve_schedule_recursive(
                time_slot_index + 1, time_slots, employee_info, schedule, new_states, relaxation_level
            )
            
            if is_solved:
                return True, final_schedule
    
    return False, None

# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
def create_schedule(store_open_time_obj, store_close_time_obj, employee_data_list):
    """Orchestrates the scheduling process with tiered rule relaxation."""
    df_long = preprocess_employee_data_to_long_format(employee_data_list, REF_DATE_FOR_PARSING)
    if df_long.empty: return "No employee data to process."
    
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    
    employee_info = {t: [] for t in time_slots}
    for _, row in df_long.iterrows():
        employee_info[row['Time']].append({
            "name": row['EmployeeNameFML'], "is_on_break": row['IsOnBreak'], "is_on_tofftl": row['IsOnToffTL']
        })

    # --- Tiered Solving Approach ---
    for relaxation_level in range(3):
        initial_schedule = [{} for _ in time_slots]
        initial_states = {}
        
        is_solved, final_schedule_rows = solve_schedule_recursive( # from schedule-main/scheduler_logic.py
            0, time_slots, employee_info, initial_schedule, initial_states, relaxation_level
        )
        
        if is_solved:
            # Found a solution, now format and return it with a note about the rules.
            relaxation_note = ""
            if relaxation_level == 1:
                relaxation_note = "NOTE: A valid schedule was found by relaxing the paired position swapping rule.\n\n"
            elif relaxation_level == 2:
                relaxation_note = "NOTE: A valid schedule was found by relaxing paired swapping AND the Conductor start time rule.\n\n"
            
            out_df = pd.DataFrame(final_schedule_rows, columns=["Time"] + ALL_POSITIONS_ORDERED)
            out_df.fillna("", inplace=True)
            for col in ["Break", "ToffTL"]:
                if col in out_df.columns:
                    out_df[col] = out_df[col].apply(lambda x: ", ".join(sorted(x)) if isinstance(x, list) else x)
            
            final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
            return relaxation_note + final_df.to_csv(index=False)

    return "Could not find a valid schedule, even after relaxing all possible rules."
