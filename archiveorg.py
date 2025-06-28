import streamlit as st
import pandas as pd
import re
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime

# Authorize Google Sheets API

def authorize_gspread():
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client

# Consolidate canister entries by getting the most recent non-update entry,
# then applying updates that occur after it

def consolidate_canister_entries(df):
    df = df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df.sort_values("Timestamp")

    consolidated = []
    for can_id, group in df.groupby("Canister ID"):
        group = group.sort_values("Timestamp")
        sample_rows = group[group["Type of Entry"].str.lower() != "update existing"]
        update_rows = group[group["Type of Entry"].str.lower() == "update existing"]

        if sample_rows.empty:
            base = update_rows.iloc[-1].copy()
        else:
            base = sample_rows.iloc[-1].copy()
            updates_after_base = update_rows[update_rows["Timestamp"] > base["Timestamp"]]
            for _, update in updates_after_base.iterrows():
                for col in df.columns:
                    val = update[col]
                    if pd.notna(val) and str(val).strip():
                        base[col] = val

        consolidated.append(base)

    return pd.DataFrame(consolidated)

# Load spreadsheet data
@st.cache_data(show_spinner="Loading canister data...")
def load_data():
    def load_sheet_df(sheet_name, worksheet_title="Sheet1"):
        client = authorize_gspread()
        worksheet = client.open(sheet_name).worksheet(worksheet_title)
        df = get_as_dataframe(worksheet, dtype=str)
        df = df.dropna(how="all")
        return df

    df = load_sheet_df("Canister Notes", worksheet_title="Form Responses 1")

    shelved_df = df[df["Storage Location"].str.contains(":", na=False)].copy()

    def parse_location(loc):
        match = re.match(r"SRTC\\s+([^:]+):([A-Ja-j])([1-9])", loc)
        if match:
            room, row, col = match.groups()
            return room.upper(), row.upper(), int(col)
        return None, None, None

    shelved_df[["Room", "Row", "Col"]] = shelved_df["Storage Location"].apply(
        lambda x: pd.Series(parse_location(x))
    )
    return df, shelved_df

# Create shelf matrix to display canister IDs
def create_shelf_matrix(rows, cols, data):
    matrix = pd.DataFrame("", index=rows, columns=cols)
    for _, row in data.iterrows():
        r, c = row["Row"], row["Col"]
        if r in rows and c in cols:
            matrix.at[r, c] = str(row["Canister ID"])
    return matrix

# Load and consolidate data
df, shelved_df = load_data()
consolidated_df = consolidate_canister_entries(df)
shelved_df = consolidate_canister_entries(shelved_df)

st.title("Canister Shelf Map")

# Display room selection and shelf matrix
rooms = sorted(shelved_df["Room"].dropna().unique())
selected_room = st.selectbox("Select a Room", rooms)
room_df = shelved_df[shelved_df["Room"] == selected_room]

shelf1_rows = list("ABCDE")
shelf1_cols = list(range(1, 10))
shelf1_df = room_df[room_df["Row"].isin(shelf1_rows)]
shelf1 = create_shelf_matrix(shelf1_rows, shelf1_cols, shelf1_df)

shelf2_rows = list("FGHIJ")
shelf2_cols = list(range(1, 6))
shelf2_df = room_df[room_df["Row"].isin(shelf2_rows)]
shelf2 = create_shelf_matrix(shelf2_rows, shelf2_cols, shelf2_df)

col1, col2 = st.columns(2)
with col1:
    st.subheader("Shelf A–E (Cols 1–9)")
    st.dataframe(shelf1, height=300)

with col2:
    st.subheader("Shelf F–J (Cols 1–5)")
    st.dataframe(shelf2, height=300)

# Define desired display order for search results
DISPLAY_ORDER = [
    "Canister ID", "Pressure", "Sample Date", "Timezone", "Location", "Latitude", "Longitude",
    "Storage Location", "Type of Entry", "Temperature", "Wind Speed", "Wind Direction",
    "Container Size", "Container Type", "New Sample or Measured", "Notes"
]

search_query = st.sidebar.text_input("Search (Canister ID, Location, or Year)", value="")

if search_query:
    query = search_query.strip().lower()

    def match_row(row):
        can_id = str(row.get("Canister ID", "")).lower()
        location = str(row.get("Storage Location", "")).lower()
        size = str(row.get("Container Size (L)", "")).lower()
        notes = str(row.get("Notes", "")).lower()
        entry_type = str(row.get("Type of Entry", "")).lower()
        sample_date = row.get("Sample Date", "")
        year = ""
        if pd.notna(sample_date):
            try:
                year = pd.to_datetime(sample_date).year
            except Exception:
                pass
        return (
            (query in can_id)
            or (query in location)
            or (query == str(year))
            or (query in size)
            or (query in notes)
            or (query in entry_type)
        )

    result = df_all[df_all.apply(match_row, axis=1)]

    if not result.empty:
        st.sidebar.success(f"Found {len(result)} match{'es' if len(result) > 1 else ''}")
        for i, row in result.iterrows():
            with st.sidebar.expander(f"Edit Canister {row['Canister ID']}"):
                updated_row = {}
                for col in DISPLAY_ORDER:
                    if col in row:
                        val = row[col] if pd.notna(row[col]) else ""
                        updated_val = st.text_input(f"{col}", value=str(val), key=f"{col}_{i}")
                        updated_row[col] = updated_val
