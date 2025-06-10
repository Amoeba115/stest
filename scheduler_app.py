# File: scheduler_app.py
import streamlit as st
import pandas as pd
from datetime import time, datetime
from io import StringIO
# Import all necessary functions from the logic file
from scheduler_logic import create_schedule_complex, create_schedule_simple, parse_time_input

# --- Page Configuration ---
st.set_page_config(page_title="Employee Scheduler", layout="wide")

# --- Custom Styling ---
st.markdown("""
<style>
    div.stButton > button {
        background-color: #f03c4c; color: white; font-size: 16px; font-weight: bold;
        border-radius: 8px; border: 2px solid #f03c4c; width: 100%;
    }
    div.stButton > button:hover {
        background-color: #d93644; border-color: #d93644; color: white;
    }
</style>
""", unsafe_allow_html=True)

# --- Page Title ---
st.markdown('<h1 style="color: #f03c4c;">Employee Schedule Generator</h1>', unsafe_allow_html=True)
st.write("Fill in the store hours and employee details in the sidebar to generate the schedule.")

# --- Consistent Reference Date for Time Parsing ---
REF_DATE_FOR_PARSING = datetime(1970, 1, 1).date()

# --- Input Sections in Sidebar ---
st.sidebar.markdown('<h1 style="color: #f03c4c; font-size: 24px;">Configuration</h1>', unsafe_allow_html=True)

# Algorithm Selector
st.sidebar.markdown('<h3 style="color: #f03c4c;">Algorithm</h3>', unsafe_allow_html=True)
algorithm_choice = st.sidebar.radio(
    "Select the scheduling logic:",
    ('Complex (Rule-Based)', 'Simple (v1)'),
    help="Complex logic uses backtracking to meet all rules. Simple logic is faster and uses a basic assignment strategy."
)

# Store Hours
st.sidebar.markdown('<h3 style="color: #f03c4c;">Store Hours</h3>', unsafe_allow_html=True)
store_open_time_str = st.sidebar.text_input("Store Open Time (e.g., 08:00 AM)", "8:00 AM")
store_close_time_str = st.sidebar.text_input("Store Close Time (e.g., 11:00 PM)", "11:00 PM")

store_open_dt = parse_time_input(store_open_time_str, REF_DATE_FOR_PARSING)
store_close_dt = parse_time_input(store_close_time_str, REF_DATE_FOR_PARSING)

# Employees
st.sidebar.markdown('<h3 style="color: #f03c4c;">Employees</h3>', unsafe_allow_html=True)
num_employees = st.sidebar.number_input("Number of Employees Working", min_value=1, value=2, step=1)

employee_data_list = []
for i in range(num_employees):
    st.sidebar.markdown(f"--- **Employee {i+1}** ---")
    emp_name = st.sidebar.text_input(f"Name (Employee {i+1})", key=f"name_{i}")
    shift_start_str = st.sidebar.text_input(f"Shift Start (Employee {i+1})", "9:00 AM", key=f"s_start_{i}")
    shift_end_str = st.sidebar.text_input(f"Shift End (Employee {i+1})", "5:00 PM", key=f"s_end_{i}")
    break_start_str = st.sidebar.text_input(f"Break Start (Employee {i+1})", "1:00 PM", key=f"break_{i}")
    
    has_tofftl = st.sidebar.checkbox(f"Training Off The Line for Employee {i+1}?", key=f"has_tofftl_{i}")
    tofftl_start_str = None
    tofftl_end_str = None
    if has_tofftl:
        tofftl_start_str = st.sidebar.text_input(f"ToffTL Start (Employee {i+1})", "11:00 AM", key=f"tofftl_s_{i}")
        tofftl_end_str = st.sidebar.text_input(f"ToffTL End (Employee {i+1})", "12:00 PM", key=f"tofftl_e_{i}")

    if emp_name:
        employee_data_list.append({
            "Name": emp_name, "Shift Start": shift_start_str, "Shift End": shift_end_str,
            "Break": break_start_str, "ToffTL Start": tofftl_start_str, "ToffTL End": tofftl_end_str
        })

# --- Generate Schedule Button ---
if st.sidebar.button("Generate Schedule"):
    if not employee_data_list:
        st.error("Please add at least one employee.")
    elif pd.isna(store_open_dt) or pd.isna(store_close_dt):
        st.error("Invalid store open or close time format.")
    else:
        store_open_time_obj = store_open_dt.time()
        store_close_time_obj = store_close_dt.time()
        
        with st.spinner(f"Generating schedule with {algorithm_choice.split(' ')[0]} logic..."):
            try:
                # Call the chosen scheduling function
                if algorithm_choice == 'Complex (Rule-Based)':
                    schedule_output = create_schedule_complex(store_open_time_obj, store_close_time_obj, employee_data_list)
                else: # 'Simple (Greedy)'
                    schedule_output = create_schedule_simple(store_open_time_obj, store_close_time_obj, employee_data_list)
                
                st.success("Schedule Generated Successfully!")
                st.subheader("Generated Schedule")
                
                note, csv_data = (schedule_output.split('\n\n', 1) if "NOTE:" in schedule_output else ("", schedule_output))
                if note: st.info(note)

                schedule_df = pd.read_csv(StringIO(csv_data))
                st.dataframe(schedule_df)
                
                st.download_button(label="Download Schedule as CSV", data=csv_data, file_name="schedule.csv", mime="text/csv")
                
            except Exception as e:
                st.error(f"An error occurred: {e}")

st.sidebar.markdown("---")
