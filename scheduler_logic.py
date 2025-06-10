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
# SECTION 2: REWRITTEN "COMPLEX" (TIERED GREEDY) SCHEDULER
# ==============================================================================
COMPLEX_WORK_POSITIONS = ["Handout", "Line Buster 1", "Conductor", "Line Buster 2", "Expo", "Drink Maker 1", "Drink Maker 2", "Line Buster 3"]
COMPLEX_ALL_POSITIONS = COMPLEX_WORK_POSITIONS + ["Break", "ToffTL"]
COMPLEX_LINE_BUSTER_ROLES = ["Line Buster 1", "Line Buster 2", "Line Buster 3"]

def create_schedule_complex(store_open_time_obj, store_close_time_obj, employee_data_list):
    df_long = preprocess_employee_data(employee_data_list)
    if df_long.empty: return "No employee data to process."

    time_slots = sorted(df_long['Time'].unique(), key=lambda t: datetime.strptime(t, '%I:%M %p'))
    employee_info = {t: g.to_dict('records') for t, g in df_long.groupby('Time')}

    # State tracking for employees
    employee_states = {} # {emp: {'last_pos': str, 'time_in_pos': int, 'history': list}}

    schedule_rows = []

    for time_slot_str in time_slots:
        current_assignments = {p: "" for p in COMPLEX_ALL_POSITIONS}
        current_assignments["Break"] = []
        current_assignments["ToffTL"] = []
        
        time_slot_obj = parse_time_input(time_slot_str, datetime(1970,1,1).date())

        # --- Step 1: Handle Breaks/ToffTL (Hard Rule) ---
        available_for_work = []
        for emp_details in employee_info[time_slot_str]:
            emp_name = emp_details['EmployeeNameFML']
            if emp_details['IsOnBreak']:
                current_assignments["Break"].append(emp_name)
                employee_states[emp_name] = {'last_pos': 'Break', 'time_in_pos': 1, 'history': []}
            elif emp_details['IsOnToffTL']:
                current_assignments["ToffTL"].append(emp_name)
                # Don't reset state for ToffTL to track duration
                if employee_states.get(emp_name, {}).get('last_pos') == 'ToffTL':
                    employee_states[emp_name]['time_in_pos'] += 1
                else:
                    employee_states[emp_name] = {'last_pos': 'ToffTL', 'time_in_pos': 1, 'history': []}
            else:
                available_for_work.append(emp_name)
        
        # --- Step 2: Assign available workers to positions based on priority ---
        assigned_this_slot = set()
        for pos in COMPLEX_WORK_POSITIONS:
            
            # --- Candidate Selection ---
            best_candidate = None
            best_candidate_score = -1

            eligible_candidates = [e for e in available_for_work if e not in assigned_this_slot]

            for emp in eligible_candidates:
                state = employee_states.get(emp, {})
                last_pos = state.get('last_pos')
                time_in_pos = state.get('time_in_pos', 0)
                
                # --- Hard Rule Checks ---
                if pos in COMPLEX_LINE_BUSTER_ROLES and time_in_pos >= 1 and last_pos == pos: continue
                if pos not in COMPLEX_LINE_BUSTER_ROLES and time_in_pos >= 2 and last_pos == pos: continue

                # --- Scoring based on priority rules ---
                score = 10 # Base score for being eligible
                
                # Second-Priority Rules
                if pos == 'Conductor':
                    if last_pos == 'Conductor' and time_in_pos == 1: score += 50 # High score for continuing Conductor
                    elif time_slot_obj.minute == 0: score += 20 # Good score for starting on the hour
                
                # Third-Priority Rules (lower score boosts)
                # Simplified check to avoid direct back-and-forth
                if len(state.get('history', [])) > 1 and state['history'][-2] == pos:
                    score -= 5 # Penalize for ABAB pattern

                if score > best_candidate_score:
                    best_candidate = emp
                    best_candidate_score = score
            
            if best_candidate:
                current_assignments[pos] = best_candidate
                assigned_this_slot.add(best_candidate)

                # Update state for the chosen employee
                state = employee_states.get(best_candidate, {})
                if state.get('last_pos') == pos:
                    state['time_in_pos'] += 1
                else:
                    state['time_in_pos'] = 1
                state['last_pos'] = pos
                state.setdefault('history', []).append(pos)
                employee_states[best_candidate] = state
        
        schedule_rows.append({"Time": time_slot_str, **current_assignments})

    # --- Final Formatting ---
    out_df = pd.DataFrame(schedule_rows, columns=["Time"] + COMPLEX_ALL_POSITIONS).fillna("")
    out_df["Break"] = out_df["Break"].apply(lambda x: ", ".join(sorted(x)) if isinstance(x, list) else x)
    out_df["ToffTL"] = out_df["ToffTL"].apply(lambda x: ", ".join(sorted(x)) if isinstance(x, list) else x)
    final_df = out_df.set_index("Time").transpose().reset_index().rename(columns={'index': 'Position'})
    return final_df.to_csv(index=False)


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
