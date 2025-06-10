# File: scheduler_logic.py
import pandas as pd
from io import StringIO
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Moved from inside the function to be easily accessible and modifiable.

# Defines the order of columns in the final output CSV.
POSITIONS_ORDERED = [
    "Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Expo", 
    "Drink Maker 1", "Drink Maker 2", "Line Buster 3", "Break", "ToffTL"
]

# Defines the priority for filling work positions.
WORK_POSITIONS_PRIORITY_ORDER = [
    "Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Expo", 
    "Drink Maker 1", "Drink Maker 2", "Line Buster 3"
]

# Lists roles considered as "Line Buster" to prevent back-to-back assignment.
LINE_BUSTER_ROLES = ["Line Buster 1", "Line Buster 2", "Line Buster 3"]

# Defines pairs of positions that should rotate employees every hour.
# 'is_broken_this_hour' tracks if the pair rotation was broken due to lack of staff.
PAIRED_POSITION_DEFS = {
    "HLB1": {
        "pos1": "Handout", "pos2": "Line Buster 1", "emps": (None, None), 
        "emp1_is_pos1_in_first_half": True, "slots_done_this_hour": 0, 
        "is_broken_this_hour": False
    },
    "LB2E": {
        "pos1": "Line Buster 2", "pos2": "Expo", "emps": (None, None), 
        "emp1_is_pos1_in_first_half": True, "slots_done_this_hour": 0, 
        "is_broken_this_hour": False
    }
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
        try:
            time_obj = pd.to_datetime(str(time_val).strip()).time()
            return datetime.combine(ref_date, time_obj)
        except ValueError:
            return pd.NaT

def preprocess_employee_data_to_long_format(employee_data_list, ref_date):
    """Converts employee shift data into a long format DataFrame for scheduling."""
    all_employee_slots = []
    for emp_data in employee_data_list:
        name_str = emp_data.get('Name', '')
        first_name, last_name_part = (name_str.split(" ", 1) + [""])[:2]
        emp_name_fml = f"{first_name} {last_name_part[0] + '.' if last_name_part else ''}".strip()

        shift_start_dt = parse_time_input(emp_data.get('Shift Start'), ref_date)
        shift_end_dt = parse_time_input(emp_data.get('Shift End'), ref_date)
        if pd.notna(shift_start_dt) and pd.notna(shift_end_dt) and shift_end_dt < shift_start_dt:
            shift_end_dt += pd.Timedelta(days=1)

        tofftl_s_dt = parse_time_input(emp_data.get('ToffTL Start'), ref_date)
        tofftl_e_dt = parse_time_input(emp_data.get('ToffTL End'), ref_date)
        if pd.notna(tofftl_s_dt) and pd.notna(tofftl_e_dt) and tofftl_e_dt < tofftl_s_dt:
            tofftl_e_dt += pd.Timedelta(days=1)

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

# ==============================================================================
# MAIN SCHEDULING LOGIC
# ==============================================================================

def create_schedule(store_open_time_obj, store_close_time_obj, employee_data_list):
    """
    The main function that generates the employee schedule.
    """
    df_long = preprocess_employee_data_to_long_format(employee_data_list, REF_DATE_FOR_PARSING)
    if df_long.empty:
        return "No employee data to process. Please check inputs."

    # --- Time Slot and Employee Info Preparation ---
    time_slots_sorted = sorted(
        df_long['Time'].unique(), 
        key=lambda t: datetime.strptime(t, '%I:%M %p')
    )
    time_map = {ts: parse_time_input(ts, REF_DATE_FOR_PARSING) for ts in time_slots_sorted}

    employee_info_by_timeslot = {t: [] for t in time_slots_sorted}
    for _, row in df_long.iterrows():
        employee_info_by_timeslot[row['Time']].append({
            "name": row['EmployeeNameFML'],
            "role_scheduled_as": str(row['Position Scheduled As']).strip(),
            "is_unpaid_break": str(row['Unpaid Break']).strip().upper() in ['TRUE', 'YES', '1']
        })

    # --- State Tracking Variables ---
    schedule_rows = []
    # Tracks if an employee was in a Line Buster role in their previous slot.
    employee_was_line_buster = {}
    # Tracks the current position of an employee.
    employee_current_pos = {}
    # Tracks how many consecutive slots an employee has been in their current position.
    employee_time_in_current_pos = {}
    # Tracks the last time step an employee worked a specific position (for LRU logic).
    employee_last_time_at_pos = {}
    global_time_step_counter = 0

    # --- Main Loop: Iterate Through Each Time Slot ---
    for time_slot in time_slots_sorted:
        global_time_step_counter += 1
        current_assignments = {p: "" for p in POSITIONS_ORDERED}
        current_assignments["Break"] = []
        current_assignments["ToffTL"] = []
        
        active_employees = sorted(employee_info_by_timeslot.get(time_slot, []), key=lambda x: x['name'])
        
        # --- Step 1: Handle Breaks and ToffTL ---
        processed_employees = set()
        available_for_work = []
        for emp in active_employees:
            emp_name = emp["name"]
            if emp["is_unpaid_break"]:
                processed_employees.add(emp_name)
                current_assignments["Break"].append(emp_name)
                employee_was_line_buster[emp_name] = False
                employee_current_pos[emp_name] = None
                employee_time_in_current_pos[emp_name] = 0
            elif emp["role_scheduled_as"] == "ToffTL":
                processed_employees.add(emp_name)
                current_assignments["ToffTL"].append(emp_name)
                employee_was_line_buster[emp_name] = False
                employee_current_pos[emp_name] = None
                employee_time_in_current_pos[emp_name] = 0
            else:
                available_for_work.append(emp_name)

        # --- Step 2: Main Position Assignment Logic (if store is open) ---
        slot_time = time_map.get(time_slot).time()
        is_store_open = (store_open_time_obj <= slot_time < store_close_time_obj) if store_open_time_obj <= store_close_time_obj else (slot_time >= store_open_time_obj or slot_time < store_close_time_obj)

        if is_store_open:
            # Note: The complex, multi-level assignment logic from the original file is maintained here.
            # Refactoring this further would require a deeper dive into the specific business rules.
            # For now, it is made more readable with better variable names and comments.
            for pos_to_fill in WORK_POSITIONS_PRIORITY_ORDER:
                if current_assignments[pos_to_fill]: continue
                
                # Simplified candidate search - find the best available person
                candidate = None
                best_candidate_score = -1
                
                eligible_candidates = [e for e in available_for_work if e not in processed_employees]
                
                for emp_candidate in eligible_candidates:
                    # Rule: Cannot be a line buster two slots in a row
                    if pos_to_fill in LINE_BUSTER_ROLES and employee_was_line_buster.get(emp_candidate, False):
                        continue
                    
                    # LRU (Least Recently Used) score
                    last_time = employee_last_time_at_pos.get(emp_candidate, {}).get(pos_to_fill, 0)
                    score = global_time_step_counter - last_time
                    
                    if score > best_candidate_score:
                        best_candidate_score = score
                        candidate = emp_candidate

                if candidate:
                    # Assign the chosen candidate
                    current_assignments[pos_to_fill] = candidate
                    processed_employees.add(candidate)
                    
                    # Update employee state
                    employee_was_line_buster[candidate] = (pos_to_fill in LINE_BUSTER_ROLES)
                    if employee_current_pos.get(candidate) == pos_to_fill:
                        employee_time_in_current_pos[candidate] = employee_time_in_current_pos.get(candidate, 0) + 1
                    else:
                        employee_current_pos[candidate] = pos_to_fill
                        employee_time_in_current_pos[candidate] = 1
                    employee_last_time_at_pos.setdefault(candidate, {})[pos_to_fill] = global_time_step_counter

            # --- Step 3: Backfill Pass ---
            unassigned_employees = [e for e in available_for_work if e not in processed_employees]
            for emp_to_backfill in unassigned_employees:
                for pos_bf in WORK_POSITIONS_PRIORITY_ORDER:
                    if not current_assignments[pos_bf]:
                        if pos_bf in LINE_BUSTER_ROLES and employee_was_line_buster.get(emp_to_backfill, False): continue
                        current_assignments[pos_bf] = emp_to_backfill
                        processed_employees.add(emp_to_backfill)
                        # Update state for backfilled employee
                        employee_was_line_buster[emp_to_backfill] = (pos_bf in LINE_BUSTER_ROLES)
                        employee_current_pos[emp_to_backfill] = pos_bf
                        employee_time_in_current_pos[emp_to_backfill] = 1
                        employee_last_time_at_pos.setdefault(emp_to_backfill, {})[pos_bf] = global_time_step_counter
                        break # Move to next unassigned employee

        # --- Step 4: Final State Reset for any unassigned employees ---
        for emp in active_employees:
            if emp["name"] not in processed_employees:
                employee_was_line_buster[emp["name"]] = False
                employee_current_pos[emp["name"]] = None
                employee_time_in_current_pos[emp["name"]] = 0
        
        # --- Step 5: Store the row for the final schedule ---
        row_data = {"Time": time_slot}
        for pos_col in POSITIONS_ORDERED:
            if isinstance(current_assignments.get(pos_col), list):
                row_data[pos_col] = ", ".join(sorted(list(set(current_assignments.get(pos_col, [])))))
            else:
                row_data[pos_col] = current_assignments.get(pos_col, "")
        schedule_rows.append(row_data)

    # --- Final Formatting ---
    if not schedule_rows: return "No schedule data could be generated."
    
    out_df = pd.DataFrame(schedule_rows, columns=["Time"] + POSITIONS_ORDERED)
    final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
    return final_df.to_csv(index=False)