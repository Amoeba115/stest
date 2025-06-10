# File: scheduler_logic.py
import pandas as pd
from io import StringIO
from datetime import datetime
from itertools import permutations

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
INDIVIDUAL_POSITION_MAX_BLOCKS = {
    "Conductor": 2, "Drink Maker 1": 2, "Drink Maker 2": 2, "Line Buster 3": 1
}
REF_DATE_FOR_PARSING = datetime(1970, 1, 1).date()

# ==============================================================================
# DATA PREPROCESSING (Helper Functions)
# ==============================================================================
def parse_time_input(time_val, ref_date):
    if pd.isna(time_val) or str(time_val).strip().upper() in ['N/A', '']: return pd.NaT
    try: return pd.to_datetime(f"{ref_date.strftime('%Y-%m-%d')} {str(time_val).strip()}")
    except ValueError: return pd.NaT

def preprocess_employee_data_to_long_format(employee_data_list, ref_date):
    all_slots = []
    for emp_data in employee_data_list:
        name = f"{emp_data.get('Name', '').split(' ', 1)[0]} {emp_data.get('Name', '').split(' ', 1)[1][0] if ' ' in emp_data.get('Name', '') else ''}.".strip()
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
# BACKTRACKING SOLVER
# ==============================================================================

def is_assignment_valid(assignments, time_slot_obj, prev_states):
    """Checks if a proposed set of assignments for a SINGLE time slot is valid."""
    employee_states = prev_states.copy()
    
    for pos, emp in assignments.items():
        # Rule: No consecutive Line Buster roles
        if pos in LINE_BUSTER_ROLES and employee_states.get(emp, {}).get('last_pos') in LINE_BUSTER_ROLES:
            return False
        
        # Rule: Conductor must start on the hour (unless continuing)
        is_continuing_conductor = employee_states.get(emp, {}).get('last_pos') == 'Conductor'
        if pos == 'Conductor' and not is_continuing_conductor and time_slot_obj.minute != 0:
            return False
            
        # Rule: Individual position duration limits
        if not is_continuing_conductor and employee_states.get(emp, {}).get('time_in_pos', 0) >= INDIVIDUAL_POSITION_MAX_BLOCKS.get(pos, 1):
             return False

    # This is a simplified check. More complex pair-rotation logic would be added here.
    # For now, we focus on the core backtracking structure.
    return True

def solve_schedule_recursive(time_slot_index, time_slots, employee_info, schedule, employee_states):
    """
    The core recursive function that tries to solve the schedule.
    It works on one time slot at a time and calls itself for the next one.
    """
    # Base Case: If we've successfully filled all time slots, we're done.
    if time_slot_index >= len(time_slots):
        return True, schedule

    current_slot_str = time_slots[time_slot_index]
    current_slot_obj = parse_time_input(current_slot_str, REF_DATE_FOR_PARSING)
    
    # Determine which employees are available for work
    available_for_work = []
    current_assignments = {p: "" for p in ALL_POSITIONS_ORDERED}
    current_assignments["Break"] = []
    current_assignments["ToffTL"] = []

    for emp_details in employee_info[current_slot_str]:
        emp_name = emp_details['name']
        if emp_details['is_on_break']:
            current_assignments["Break"].append(emp_name)
        elif emp_details['is_on_tofftl']:
            current_assignments["ToffTL"].append(emp_name)
        else:
            available_for_work.append(emp_name)
    
    num_positions_to_fill = len(available_for_work)
    
    # Rule: Positions must be filled in priority order.
    positions_to_fill = WORK_POSITIONS_PRIORITY_ORDER[:num_positions_to_fill]

    # Find all possible ways to assign available employees to the required positions
    for p in permutations(available_for_work, num_positions_to_fill):
        
        # An assignment maps a position to an employee for this slot
        tentative_assignments = {pos: emp for pos, emp in zip(positions_to_fill, p)}
        
        # Check if this specific combination of assignments is valid according to the rules
        if is_assignment_valid(tentative_assignments, current_slot_obj, employee_states):
            
            # If valid, apply this assignment and prepare state for the next recursion
            new_states = employee_states.copy()
            for pos, emp in tentative_assignments.items():
                current_assignments[pos] = emp
                
                # Update employee state for the next time slot
                time_in_pos = (new_states.get(emp, {}).get('time_in_pos', 0) + 1) if new_states.get(emp, {}).get('last_pos') == pos else 1
                new_states[emp] = {'last_pos': pos, 'time_in_pos': time_in_pos}
            
            # Add the successful assignments to the schedule
            schedule[time_slot_index] = {"Time": current_slot_str, **current_assignments}
            
            # --- RECURSIVE CALL ---
            # Try to solve for the *next* time slot with the updated states.
            is_solved, final_schedule = solve_schedule_recursive(
                time_slot_index + 1, time_slots, employee_info, schedule, new_states
            )
            
            # If the recursive call succeeded, it means we found a valid path. Pass the success up.
            if is_solved:
                return True, final_schedule

            # --- BACKTRACK ---
            # If the recursive call returned False, it means the `tentative_assignments`
            # we chose led to a dead end. The loop will now continue to the next permutation.
    
    # If we've tried all permutations and none led to a solution, return False.
    # This tells the previous function call that it made a bad choice.
    return False, None


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
def create_schedule(store_open_time_obj, store_close_time_obj, employee_data_list):
    """
    Main function to orchestrate the scheduling process.
    """
    df_long = preprocess_employee_data_to_long_format(employee_data_list, REF_DATE_FOR_PARSING)
    if df_long.empty: return "No employee data to process."
    
    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    
    employee_info = {t: [] for t in time_slots}
    for _, row in df_long.iterrows():
        employee_info[row['Time']].append({
            "name": row['EmployeeNameFML'],
            "is_on_break": row['IsOnBreak'],
            "is_on_tofftl": row['IsOnToffTL']
        })

    # Initialize empty schedule and states, then start the recursive solver.
    initial_schedule = [{} for _ in time_slots]
    initial_states = {}
    
    is_solved, final_schedule_rows = solve_schedule_recursive(0, time_slots, employee_info, initial_schedule, initial_states)
    
    if not is_solved:
        return "Could not find a valid schedule that meets all constraints."

    # Format the final schedule for output
    out_df = pd.DataFrame(final_schedule_rows, columns=["Time"] + ALL_POSITIONS_ORDERED)
    out_df.fillna("", inplace=True)
    for col in ["Break", "ToffTL"]:
        if col in out_df.columns:
            out_df[col] = out_df[col].apply(lambda x: ", ".join(sorted(x)) if isinstance(x, list) else x)
            
    final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
    return final_df.to_csv(index=False)
