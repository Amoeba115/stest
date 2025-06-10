# File: scheduler_app.py
import streamlit as st
import pandas as pd
from datetime import time, datetime
from io import StringIO
from scheduler_logic import create_schedule, parse_time_input

# --- Page Configuration ---
st.set_page_config(page_title="Employee Scheduler", layout="wide")

st.title("Employee Schedule Generator")
st.write("Fill in the store hours and employee details in the sidebar to generate the schedule.")

# --- Consistent Reference Date for Time Parsing ---
REF_DATE_FOR_PARSING = datetime(1970, 1, 1).date()

# --- Input Sections in Sidebar ---
st.sidebar.header("Configuration")

# Store Hours
st.sidebar.subheader("Store Hours")
store_open_time_str = st.sidebar.text_input("Store Open Time (e.g., 08:00 AM)", "7:30 AM")
store_close_time_str = st.sidebar.text_input("Store Close Time (e.g., 11:00 PM)", "10:00 PM")

# Parse store times early for validation
store_open_dt = parse_time_input(store_open_time_str, REF_DATE_FOR_PARSING)
store_close_dt = parse_time_input(store_close_time_str, REF_DATE_FOR_PARSING)

# Number of Employees
st.sidebar.subheader("Employees")
num_employees = st.sidebar.number_input("Number of Employees Working", min_value=1, value=2, step=1)

employee_data_list = []
for i in range(num_employees):
    st.sidebar.markdown(f"--- **Employee {i+1}** ---")
    emp_name = st.sidebar.text_input(f"Name (Employee {i+1})", key=f"name_{i}")
    shift_start_str = st.sidebar.text_input(f"Shift Start (Employee {i+1})", " ", key=f"s_start_{i}")
    shift_end_str = st.sidebar.text_input(f"Shift End (Employee {i+1})", " ", key=f"s_end_{i}")
    break_start_str = st.sidebar.text_input(f"Break Start (Employee {i+1})", " ", key=f"break_{i}")

    # --- Real-time validation for employee shift times ---
    shift_start_dt = parse_time_input(shift_start_str, REF_DATE_FOR_PARSING)
    shift_end_dt = parse_time_input(shift_end_str, REF_DATE_FOR_PARSING)

    if pd.notna(shift_start_dt) and pd.notna(store_open_dt) and shift_start_dt < store_open_dt:
        st.sidebar.warning(f"Employee {i+1}'s shift starts before the store opens.")
    if pd.notna(shift_end_dt) and pd.notna(store_close_dt) and shift_end_dt > store_close_dt:
        st.sidebar.warning(f"Employee {i+1}'s shift ends after the store closes.")
    # --- End validation ---
    
    has_tofftl = st.sidebar.checkbox(f"Training Off The Line (ToffTL) for Employee {i+1}?", key=f"has_tofftl_{i}")
    tofftl_start_str = None
    tofftl_end_str = None
    if has_tofftl:
        tofftl_start_str = st.sidebar.text_input(f"ToffTL Start (Employee {i+1})", "11:00 AM", key=f"tofftl_s_{i}")
        tofftl_end_str = st.sidebar.text_input(f"ToffTL End (Employee {i+1})", "12:00 PM", key=f"tofftl_e_{i}")

    if emp_name: # Only add if name is provided
        employee_data_list.append({
            "Name": emp_name,
            "Shift Start": shift_start_str, "Shift End": shift_end_str,
            "Break": break_start_str,
            "ToffTL Start": tofftl_start_str, "ToffTL End": tofftl_end_str
        })

# --- Generate Schedule Button ---
if st.sidebar.button("Generate Schedule"):
    if not employee_data_list:
        st.error("Please add at least one employee.")
    elif pd.isna(store_open_dt) or pd.isna(store_close_dt):
        st.error("Invalid store open or close time format. Please use HH:MM AM/PM or HH:MM.")
    else:
        store_open_time_obj = store_open_dt.time()
        store_close_time_obj = store_close_dt.time()
        
        valid_employee_data = True
        for emp_idx, emp_d in enumerate(employee_data_list):
            if not emp_d["Name"].strip():
                st.error(f"Employee {emp_idx+1} name is missing.")
                valid_employee_data = False
                break
        
        if valid_employee_data:
            with st.spinner("Generating schedule... Please wait."):
                try:
                    # Call the main scheduling logic
                    schedule_csv_string = create_schedule(store_open_time_obj, store_close_time_obj, employee_data_list)
                    
                    st.success("Schedule Generated Successfully!")
                    
                    # --- Display the schedule as a DataFrame and provide download ---
                    st.subheader("Generated Schedule")
                    schedule_df = pd.read_csv(StringIO(schedule_csv_string))
                    st.dataframe(schedule_df)
                    
                    st.download_button(
                        label="Download Schedule as CSV",
                        data=schedule_csv_string,
                        file_name="schedule.csv",
                        mime="text/csv",
                    )
                    
                    st.subheader("Raw CSV Output")
                    st.text_area("CSV Data", schedule_csv_string, height=300)
                    
                except Exception as e:
                    st.error(f"An error occurred during schedule generation: {e}")
        else:
            st.warning("Please correct the employee data errors in the sidebar.")

st.sidebar.markdown("---")
st.sidebar.info("Ensure all time inputs are in a recognizable format (e.g., '9:00 AM', '14:30').")
