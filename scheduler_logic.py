# File: scheduler_logic.py
import pandas as pd
from io import StringIO
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# Based on the rules provided by the user.
# ==============================================================================

# Rule 3: Position Filling Priority
WORK_POSITIONS_PRIORITY_ORDER = [
    "Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Greeter",
    "Drink Maker 1", "Drink Maker 2", "Line Buster 3"
]

# Defines the full list of columns for the output, including non-work roles.
ALL_POSITIONS_ORDERED = WORK_POSITIONS_PRIORITY_ORDER + ["Break", "ToffTL"]

# Rule 4: Line Buster Constraint
LINE_BUSTER_ROLES = ["Line Buster 1", "Line Buster 2", "Line Buster 3"]

# Rule 5 & 6: Paired Position Rotation
PAIRED_POSITION_DEFINITIONS = {
    "Pair1": {"pos1": "Handout", "pos2": "Line Buster 1"},
    "Pair2": {"pos1": "Line Buster 2", "pos2": "Greeter"}
}

# Rule 7: Individual Position Duration (in 30-min blocks)
# Note: Line Buster 3 is set to 1 block (30 mins) to align with Rule 4.
INDIVIDUAL_POSITION_MAX_BLOCKS = {
    "Conductor": 2, # 1 hour
    "Drink Maker 1": 2, # 1 hour
    "Drink Maker 2": 2, # 1 hour
    "Line Buster 3": 1 # 30 mins
}

# Consistent reference date for parsing time strings.
REF_DATE_FOR_PARSING = datetime(1970, 1, 1).date()


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def parse_time_input(time_val, ref_date):
    """Parses various time string formats into a datetime object."""
    if pd.isna(time_val) or str(time_val).strip().upper() in ['N/A', '']:
        return pd.NaT
    try:
        return pd.to_datetime(f"{ref_date.strftime('%Y-%m-%d')} {str(time_val).strip()}")
    except ValueError:
        return pd.NaT

def preprocess_employee_data_to_long_format(employee_data_list, ref_date):
    """Converts employee shift data into a long format DataFrame for scheduling."""
    # This function remains largely the same, as it correctly prepares the data.
    all_employee_slots = []
    for emp_data in employee_data_list:
        name_str = emp_data.get('Name', '')
        first_name, last_name_part = (name_str.split(" ", 1) + [""])[:2]
        emp_name_fml = f"{first_name} {last_name_part[0] + '.' if last_name_part else ''}".strip()

        shift_start_dt = parse_time_input(emp_data.get('Shift Start'), ref_date)
        shift_end_dt = parse_time_input(emp_data.get('Shift End'), ref_date)

        tofftl_s_dt = parse_time_input(emp_data.get('ToffTL Start'), ref_date)
        tofftl_e_dt = parse_time_input(emp_data.get('ToffTL End'), ref_date)

        break_start_dt = parse_time_input(emp_data.get('Break'), ref_date)
        break_end_dt = break_start_dt + pd.Timedelta(minutes=30) if pd.notna(break_start_dt) else None

        if pd.notna(shift_start_dt) and pd.notna(shift_end_dt):
            current_time = shift_start_dt
            while current_time < shift_end_dt:
                position = "Available"
                is_unpaid_break = "FALSE"
                if pd.notna(tofftl_s_dt) and tofftl_s_dt <= current_time < tofftl_e_dt:
                    position = "ToffTL"
                if pd.notna(break_start_dt) and break_start_dt <= current_time < break_end_dt:
                    is_unpaid_break = "TRUE"
                
                all_employee_slots.append({
                    'Time': current_time.strftime('%I:%M %p').lstrip('0'),
                    'EmployeeNameFML': emp_name_fml,
                    'Position Scheduled As': position,
                    'Unpaid Break': is_unpaid_break
                })
                current_time += pd.Timedelta(minutes=30)
    
    return pd.DataFrame(all_employee_slots) if all_employee_slots else pd.DataFrame()


def find_eligible_employees(candidates, assigned_employees, state, rules):
    """Filters a list of candidates based on general availability and rules."""
    eligible = []
    for emp in candidates:
        if emp in assigned_employees:
            continue
        
        # Rule 1: Shift boundaries (handled by preprocess function)
        # Additional checks can be added here if needed.
        
        eligible.append(emp)
    return eligible

# ==============================================================================
# MAIN SCHEDULING LOGIC
# ==============================================================================

def create_schedule(store_open_time_obj, store_close_time_obj, employee_data_list):
    df_long = preprocess_employee_data_to_long_format(employee_data_list, REF_DATE_FOR_PARSING)
    if df_long.empty:
        return "No employee data to process."

    time_slots_sorted = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    time_map = {ts: parse_time_input(ts, REF_DATE_FOR_PARSING) for ts in time_slots_sorted}

    employee_info_by_timeslot = {t: [] for t in time_slots_sorted}
    for _, row in df_long.iterrows():
        employee_info_by_timeslot[row['Time']].append({
            "name": row['EmployeeNameFML'],
            "is_break_or_tofftl": str(row['Unpaid Break']).strip().upper() == 'TRUE' or str(row['Position Scheduled As']).strip() == 'ToffTL'
        })

    # --- State Tracking Variables ---
    schedule_rows = []
    # Tracks {emp_name: position_name}
    employee_current_pos = {}
    # Tracks {emp_name: num_blocks_in_pos}
    employee_time_in_current_pos = {}
    # Tracks the state of the paired rotations {pair_id: {emps: (e1, e2), blocks_done: 0/1}}
    pair_state = {
        "Pair1": {"emps": (None, None), "blocks_done": 0},
        "Pair2": {"emps": (None, None), "blocks_done": 0}
    }

    # --- Main Loop: Iterate Through Each Time Slot ---
    for time_slot in time_slots_sorted:
        current_assignments = {p: "" for p in ALL_POSITIONS_ORDERED}
        current_assignments["Break"] = []
        current_assignments["ToffTL"] = []
        
        slot_time_obj = time_map[time_slot]
        
        active_emp_names = [e['name'] for e in employee_info_by_timeslot.get(time_slot, []) if not e['is_break_or_tofftl']]
        
        # Employees on break/tofftl are not available for work
        for emp_details in employee_info_by_timeslot.get(time_slot, []):
            if emp_details['is_break_or_tofftl']:
                # Reset state for employees on break
                employee_current_pos.pop(emp_details['name'], None)
                employee_time_in_current_pos.pop(emp_details['name'], None)

        assigned_this_slot = set()

        # Reset pair state every hour on the hour
        if slot_time_obj.minute == 0:
            for pair_id in pair_state:
                pair_state[pair_id] = {"emps": (None, None), "blocks_done": 0}

        # --- Position Assignment Loop (Rule 3) ---
        for pos in WORK_POSITIONS_PRIORITY_ORDER:
            
            # Find which pair this position belongs to, if any
            current_pair_id = None
            for pair_id, pair_def in PAIRED_POSITION_DEFINITIONS.items():
                if pos in [pair_def["pos1"], pair_def["pos2"]]:
                    current_pair_id = pair_id
                    break

            candidate = None
            
            # --- Rule 5 & 6: Paired Position Logic ---
            if current_pair_id:
                state = pair_state[current_pair_id]
                pair_def = PAIRED_POSITION_DEFINITIONS[current_pair_id]

                # If it's the second half-hour of the pair's rotation, swap them
                if state["blocks_done"] == 1:
                    emp1, emp2 = state["emps"]
                    if pos == pair_def["pos1"] and emp2: candidate = emp2
                    if pos == pair_def["pos2"] and emp1: candidate = emp1
                
                # If it's the start of a rotation, find two new people
                elif state["blocks_done"] == 0 and not state["emps"][0]:
                    available = [e for e in active_emp_names if e not in assigned_this_slot]
                    if len(available) >= 2:
                        emp1, emp2 = available[0], available[1]
                        state["emps"] = (emp1, emp2)
                        if pos == pair_def["pos1"]: candidate = emp1
                        if pos == pair_def["pos2"]: candidate = emp2
            
            # --- Rule 2: Conductor Logic ---
            elif pos == "Conductor":
                # Must start on the hour and last for 1 hour
                if slot_time_obj.minute == 0:
                    # Is an employee already in the Conductor role?
                    for emp, current_pos in employee_current_pos.items():
                        if current_pos == "Conductor" and employee_time_in_current_pos.get(emp, 0) == 1:
                            candidate = emp
                            break
                    if not candidate:
                        available = [e for e in active_emp_names if e not in assigned_this_slot]
                        if available: candidate = available[0]
            
            # --- Rule 7: Individual Positions (1-hour max) ---
            else: # For Drink Makers, etc.
                # Continue with the same person if they are under their max time
                for emp, current_pos in employee_current_pos.items():
                    if current_pos == pos and employee_time_in_current_pos.get(emp, 0) < INDIVIDUAL_POSITION_MAX_BLOCKS.get(pos, 1):
                         candidate = emp
                         break
                if not candidate:
                    available = [e for e in active_emp_names if e not in assigned_this_slot]
                    # Rule 4: Prevent consecutive Line Buster assignments
                    if pos in LINE_BUSTER_ROLES:
                        available = [e for e in available if employee_current_pos.get(e) not in LINE_BUSTER_ROLES]
                    
                    if available: candidate = available[0]

            # --- Assign Candidate and Update State ---
            if candidate and candidate not in assigned_this_slot:
                current_assignments[pos] = candidate
                assigned_this_slot.add(candidate)

                # Update state
                if employee_current_pos.get(candidate) == pos:
                    employee_time_in_current_pos[candidate] = employee_time_in_current_pos.get(candidate, 0) + 1
                else: # New position for this employee
                    employee_current_pos[candidate] = pos
                    employee_time_in_current_pos[candidate] = 1

        # --- Update Pair State after assignments are made ---
        for pair_id, state in pair_state.items():
            if state["emps"][0] and state["emps"][1]:
                state["blocks_done"] += 1

        schedule_rows.append({"Time": time_slot, **current_assignments})

    # --- Final Formatting ---
    if not schedule_rows: return "No schedule data could be generated."
    
    out_df = pd.DataFrame(schedule_rows, columns=["Time"] + ALL_POSITIONS_ORDERED)
    final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
    return final_df.to_csv(index=False)
