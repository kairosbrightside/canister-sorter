import streamlit as st
import pandas as pd
import re
import gspread
from gspread_dataframe import get_as_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from google.oauth2.service_account import Credentials



def authorize_gspread():
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict)
    client = gspread.authorize(creds)
    return client

def consolidate_canister_entries(df):
    df = df.sort_values("Timestamp") 
    df = df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

    samples = df[df["Type of Entry"].str.lower() != "update existing"]
    updates = df[df["Type of Entry"].str.lower() == "update existing"]

    recent_samples = (
        samples.sort_values("Timestamp")
        .groupby("Canister ID")
        .last()
        .reset_index()
    )

    recent_updates = (
        updates.sort_values("Timestamp")
        .groupby("Canister ID")
        .last()
        .reset_index()
    )

    combined = pd.merge(
        recent_samples,
        recent_updates,
        on="Canister ID",
        how="outer",
        suffixes=("_sample", "_update"),
    )

    def merge_fields(row, col):
        update_val = row.get(f"{col}_update")
        sample_val = row.get(f"{col}_sample")
        return update_val if pd.notna(update_val) and str(update_val).strip() else sample_val

    all_fields = set(col.replace("_sample", "") for col in combined.columns if "_sample" in col)
    merged_data = pd.DataFrame()

    for field in all_fields:
        merged_data[field] = combined.apply(lambda row: merge_fields(row, field), axis=1)

    merged_data["Canister ID"] = combined["Canister ID"]
    merged_data["Type of Entry"] = combined["Type of Entry_sample"]
    return merged_data

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

def create_shelf_matrix(rows, cols, data):
    matrix = pd.DataFrame("", index=rows, columns=cols)
    for _, row in data.iterrows():
        r, c = row["Row"], row["Col"]
        if r in rows and c in cols:
            matrix.at[r, c] = str(row["Canister ID"])
    return matrix

df, shelved_df = load_data()
consolidated_df = consolidate_canister_entries(df)
shelved_df = consolidate_canister_entries(shelved_df)

st.title("Canister Shelf Map")

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

search_query = st.sidebar.text_input("Search (Canister ID, Location, or Year)", value="")

df_all = consolidated_df.copy()

if search_query:
    query = search_query.strip().lower()

    def match_row(row):
        can_id = str(row.get("Canister ID", "")).lower()
        location = str(row.get("Storage Location", "")).lower()
        sample_date = row.get("Sample Date", "")
        year = ""
        if pd.notna(sample_date):
            try:
                year = pd.to_datetime(sample_date).year
            except Exception:
                pass
        return (query in can_id) or (query in location) or (query == str(year))

    result = df_all[df_all.apply(match_row, axis=1)]

    if not result.empty:
        st.sidebar.success(f"Found {len(result)} match{'es' if len(result) > 1 else ''}")
        for i, row in result.iterrows():
            with st.sidebar.expander(f"Edit Canister {row['Canister ID']}"):
                updated_row = {}
                for col in result.columns:
                    val = row[col] if pd.notna(row[col]) else ""
                    updated_val = st.text_input(f"{col}", value=str(val), key=f"{col}_{i}")
                    updated_row[col] = updated_val
                if st.button(f"Update {row['Canister ID']}", key=f"save_{i}"):
                    try:
                        updated_row["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        updated_row["Type of Entry"] = "Update Existing"

                        for field in ["Sample Date", "Sample Type", "Site"]:
                            updated_row[field] = ""

                        client = authorize_gspread()
                        sheet = client.open("Canister Notes").worksheet("Form Responses 1")
                        form_columns = sheet.row_values(1)

                        new_row = [updated_row.get(col, "") for col in form_columns]
                        sheet.append_row(new_row)

                        st.success(f"Appended update for Canister {updated_row['Canister ID']}")
                    except Exception as e:
                        st.error(f"Failed to append update: {e}")

    else:
        st.sidebar.error("No matches found.")

    new_entry = {}
    form_columns = df.columns.tolist()
    for col in form_columns:
        if col.lower() == "sample date":
            new_val = st.text_input(f"{col} (new)", value=datetime.now().strftime("%Y-%m-%d"), key=f"new_{col}")
        else:
            new_val = st.text_input(f"{col} (new)", key=f"new_{col}")
        new_entry[col] = new_val

    if st.button("Submit New Entry"):
        if not new_entry.get("Canister ID"):
            st.warning("Canister ID is required.")
        else:
            try:
                client = authorize_gspread()
                sheet = client.open("Canister Notes").worksheet("Form Responses 1")
                sheet.append_row([new_entry.get(col, "") for col in form_columns])
                st.success(f"New entry for Canister {new_entry['Canister ID']} added!")
            except Exception as eee:
                st.error(f"Failed to add new entry: {eee}")
