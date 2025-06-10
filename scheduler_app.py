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
st.write("Fill in the store hours and employee details in the sidebar to generate the schedule.") # from schedule-main/scheduler_app.py

# --- Consistent Reference Date for Time Parsing ---
REF_DATE_FOR_PARSING = datetime(1970, 1, 1).date() # from schedule-main/scheduler_app.py

# --- Input Sections in Sidebar ---
st.sidebar.markdown('<h1 style="color: #f03c4c; font-size: 24px;">Configuration</h1>', unsafe_allow_html=True)

# Store Hours
st.sidebar.markdown('<h3 style="color: #f03c4c;">Store Hours</h3>', unsafe_allow_html=True)
store_open_time_str = st.sidebar.text_input("Store Open Time (e.g., 08:00 AM)", "8:00 AM") # from schedule
