# File: scheduler_app.py
import streamlit as st
import pandas as pd
from datetime import time, datetime
from io import StringIO
from scheduler_logic import create_schedule, parse_time_input # from schedule-main/scheduler_logic.py

# --- Page Configuration ---
st.set_page_config(page_title="Employee Scheduler", layout="wide") # from schedule-main/scheduler_app.py

# --- Custom Styling ---
st.markdown("""
<style>
    /* Style for the main action button */
    div.stButton > button {
        background-color: #f03c4c;
        color: white;
        font-size: 16px;
        font-weight: bold;
        border-radius: 8px;
        border: 2px solid #f03c4c;
        width: 100%;
    }
    div.stButton > button:hover {
        background-color: #d93644;
        border-color: #d93644;
        color: white;
    }
</style>
""", unsafe_allow_html=True)


# --- Page Title ---
st.markdown('<h1 style="color: #f03c4c;">Employee Schedule Generator</h1>', unsafe_allow_html=True)
st.write("Fill in the store hours and employee details in the sidebar to generate the schedule. This generator uses the rules for Swig daily schedules as a guideline.") # from schedule-main/scheduler_app.py

# --- Consistent Reference Date for Time Parsing ---
REF_DATE_FOR_PARSING = datetime(1970, 1, 1).date() # from schedule-main/scheduler_app.py

# --- Input Sections in Sidebar ---
st.sidebar.markdown('<h1 style="color: #f03c4c; font-size: 24px;">Configuration</h1>', unsafe_allow_html=True)

# Store Hours
st.sidebar.markdown('<h3 style="color: #f03c4c;">Store Hours</h3>', unsafe_allow_html=True)
store_open_time_str = st.sidebar.text_input("Store Open Time (e.g., 08:00 AM)", "7:30 AM") # from schedule-main/scheduler_app.py
store_close_time_str = st.sidebar.text_input("Store Close Time (e.g., 11:00 PM)", "10:00 PM") # from schedule-main/scheduler_app.py

# Parse store times early for validation
store_open_dt = parse_time_input(store_open_time_str, REF_DATE_FOR_PARSING) # from schedule-main/scheduler_app.py
store_close_dt = parse_time_input(store_close_time_str, REF_DATE_FOR_PARSING) # from schedule-main/scheduler_app.py

# Number of Employees
st.sidebar.markdown('<h3 style="color: #f03c4c;">Employees</h3>', unsafe_allow_html=True)
num_employees = st.sidebar.number_input("Number of Employees Working", min_value=1, value=2, step=1) # from schedule-main/scheduler_app.py

employee_data_list = [] # from schedule-main/scheduler_app.py
for i in range(num_employees): # from schedule-main/scheduler_app.py
    st.sidebar.markdown(f"--- **Employee {i+1}** ---") # from schedule-main/scheduler_app.py
    emp_name = st.sidebar.text_input(f"Name (Employee {i+1})", key=f"name_{i}") # from schedule-main/scheduler_app.py
    shift_start_str = st.sidebar.text_input(f"Shift Start (Employee {i+1})", " ", key=f"s_start_{i}") # from schedule-main/scheduler_app.py
    shift_end_str = st.sidebar.text_input(f"Shift End (Employee {i+1})", " ", key=f"s_end_{i}") # from schedule-main/scheduler_app.py
    break_start_str = st.sidebar.text_input(f"Break Start (Employee {i+1})", " ", key=f"break_{i}") # from schedule-main/scheduler_app.py

    # --- Real-time validation for employee shift times ---
    shift_start_dt = parse_time_input(shift_start_str, REF_DATE_FOR_PARSING) # from schedule-main/scheduler_app.py
    shift_end_dt = parse_time_input(shift_end_str, REF_DATE_FOR_PARSING) # from schedule-main/scheduler_app.py

    if pd.notna(shift_start_dt) and pd.notna(store_open_dt) and shift_start_dt < store_open_dt: # from schedule-main/scheduler_app.py
        st.sidebar.warning(f"Employee {i+1}'s shift starts before the store opens.") # from schedule-main/scheduler_app.py
    if pd.notna(shift_end_dt) and pd.notna(store_close_dt) and shift_end_dt > store_close_dt: # from schedule-main/scheduler_app.py
        st.sidebar.warning(f"Employee {i+1}'s shift ends after the store closes.") # from schedule-main/scheduler_app.py
    # --- End validation ---
    
    has_tofftl = st.sidebar.checkbox(f"Training Off The Line (ToffTL) for Employee {i+1}?", key=f"has_tofftl_{i}") # from schedule-main/scheduler_app.py
    tofftl_start_str = None # from schedule-main/scheduler_app.py
    tofftl_end_str = None # from schedule-main/scheduler_app.py
    if has_tofftl: # from schedule-main/scheduler_app.py
        tofftl_start_str = st.sidebar.text_input(f"ToffTL Start (Employee {i+1})", "11:00 AM", key=f"tofftl_s_{i}") # from schedule-main/scheduler_app.py
        tofftl_end_str = st.sidebar.text_input(f"ToffTL End (Employee {i+1})", "12:00 PM", key=f"tofftl_e_{i}") # from schedule-main/scheduler_app.py

    if emp_name: # from schedule-main/scheduler_app.py
        employee_data_list.append({ # from schedule-main/scheduler_app.py
            "Name": emp_name, # from schedule-main/scheduler_app.py
            "Shift Start": shift_start_str, "Shift End": shift_end_str, # from schedule-main/scheduler_app.py
            "Break": break_start_str, # from schedule-main/scheduler_app.py
            "ToffTL Start": tofftl_start_str, "ToffTL End": tofftl_end_str # from schedule-main/scheduler_app.py
        })

# --- Generate Schedule Button ---
if st.sidebar.button("Generate Schedule"): # from schedule-main/scheduler_app.py
    if not employee_data_list: # from schedule-main/scheduler_app.py
        st.error("Please add at least one employee.") # from schedule-main/scheduler_app.py
    elif pd.isna(store_open_dt) or pd.isna(store_close_dt): # from schedule-main/scheduler_app.py
        st.error("Invalid store open or close time format. Please use HH:MM AM/PM or HH:MM.") # from schedule-main/scheduler_app.py
    else:
        store_open_time_obj = store_open_dt.time() # from schedule-main/scheduler_app.py
        store_close_time_obj = store_close_dt.time() # from schedule-main/scheduler_app.py
        
        valid_employee_data = True # from schedule-main/scheduler_app.py
        for emp_idx, emp_d in enumerate(employee_data_list): # from schedule-main/scheduler_app.py
            if not emp_d["Name"].strip(): # from schedule-main/scheduler_app.py
                st.error(f"Employee {emp_idx+1} name is missing.") # from schedule-main/scheduler_app.py
                valid_employee_data = False # from schedule-main/scheduler_app.py
                break # from schedule-main/scheduler_app.py
        
        if valid_employee_data: # from schedule-main/scheduler_app.py
            with st.spinner("Generating schedule... Please wait."):
                try:
                    # Call the main scheduling logic
                    schedule_csv_string = create_schedule(store_open_time_obj, store_close_time_obj, employee_data_list) # from schedule-main/scheduler_app.py
                    
                    st.success("Schedule Generated Successfully!") # from schedule-main/scheduler_app.py
                    
                    # --- Display the schedule as a DataFrame and provide download ---
                    st.subheader("Generated Schedule") # from schedule-main/scheduler_app.py
                    
                    # Check for relaxation notes in the output
                    if "NOTE:" in schedule_csv_string:
                        note, csv_data = schedule_csv_string.split('\n\n', 1)
                        st.info(note)
                    else:
                        csv_data = schedule_csv_string

                    schedule_df = pd.read_csv(StringIO(csv_data)) # from schedule-main/scheduler_app.py
                    st.dataframe(schedule_df) # from schedule-main/scheduler_app.py
                    
                    st.download_button( # from schedule-main/scheduler_app.py
                        label="Download Schedule as CSV", # from schedule-main/scheduler_app.py
                        data=csv_data, # from schedule-main/scheduler_app.py
                        file_name="schedule.csv", # from schedule-main/scheduler_app.py
                        mime="text/csv", # from schedule-main/scheduler_app.py
                    )
                    
                except Exception as e: # from schedule-main/scheduler_app.py
                    st.error(f"An error occurred during schedule generation: {e}") # from schedule-main/scheduler_app.py
        else:
            st.warning("Please correct the employee data errors in the sidebar.") # from schedule-main/scheduler_app.py

st.sidebar.markdown("---") # from schedule-main/scheduler_app.py
st.sidebar.info("Ensure all time inputs are in a recognizable format (e.g., '9:00 AM', '14:30').")
