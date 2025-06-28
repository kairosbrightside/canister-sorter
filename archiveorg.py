import streamlit as st
import pandas as pd
import re
import gspread
from gspread_dataframe import get_as_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from google.oauth2.service_account import Credentials

def authorize_gspread():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)  # ✅ Add scopes
    client = gspread.authorize(creds)
    return client

def consolidate_canister_entries(df):
    df = df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df.sort_values("Timestamp")

    # Split into updates and non-updates
    is_update = df["Type of Entry"].str.lower() == "update existing"
    updates = df[is_update]
    non_updates = df[~is_update]

    final_rows = []

    for can_id, group in df.groupby("Canister ID"):
        group = group.sort_values("Timestamp")

        # Get the last non-update entry
        recent_non_update = group[group["Type of Entry"].str.lower() != "update existing"]
        if recent_non_update.empty:
            continue  # skip if no valid base entry

        base_row = recent_non_update.iloc[-1]
        base_time = base_row["Timestamp"]

        # All updates *after* the base time
        post_updates = group[
            (group["Type of Entry"].str.lower() == "update existing") &
            (group["Timestamp"] > base_time)
        ]
        # Merge all updates onto base row
        merged = base_row.to_dict()
        for _, update_row in post_updates.iterrows():
            for col, val in update_row.items():
                if pd.notna(val) and str(val).strip():
                    merged[col] = val

        final_rows.append(merged)
    return pd.DataFrame(final_rows)
    
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

    # Load the full dataset
    df = load_sheet_df("Canister Notes", worksheet_title="Form Responses 1")
    return df

# Room / Row / Col for matrix placement
def parse_location(loc):
    match = re.match(r"SRTC\s+([^:]+):([A-Ja-j])([1-9])", loc)
    if match:
        room, row, col = match.groups()
        return room.upper(), row.upper(), int(col)
    return None, None, None

def create_shelf_matrix(rows, cols, data):
    matrix = pd.DataFrame("", index=rows, columns=cols)
    for _, row in data.iterrows():
        r, c = row["Row"], row["Col"]
        if r in rows and c in cols:
            matrix.at[r, c] = str(row["Canister ID"])
    return matrix

df = load_data()

consolidated_df = consolidate_canister_entries(df)

shelved_df_raw = consolidated_df[consolidated_df["Storage Location"].str.contains(":", na=False)].copy()

shelved_df_raw[["Room", "Row", "Col"]] = shelved_df_raw["Storage Location"].apply(
    lambda x: pd.Series(parse_location(x))
)

shelved_df = shelved_df_raw

# 🔹 Add this block here
if st.button("Average pressure of Archive samples"):
    archive_samples = df[df["Type of Entry"].str.lower() == "archive"].copy()

    # Only keep valid timestamps
    archive_samples["Timestamp"] = pd.to_datetime(archive_samples["Timestamp"], errors="coerce")
    archive_samples = archive_samples.dropna(subset=["Timestamp"])

    # Most recent 'update existing' for each canister
    latest_updates = (
        archive_samples[archive_samples["Type of Entry"].str.lower() == "update existing"]
        .sort_values("Timestamp")
        .groupby("Canister ID")
        .tail(1)
    )

    # Try converting pressure to numeric, if not already
    latest_updates["Pressure (psig)"] = pd.to_numeric(latest_updates["Pressure (psig)"], errors="coerce")
    valid_pressures = latest_updates["Pressure (psig)"].dropna()

    if not valid_pressures.empty:
        avg_pressure = valid_pressures.mean()
        st.success(f"Average pressure of Archive samples (latest updates): {avg_pressure:.2f}")
    else:
        st.warning("No valid pressures found in recent 'Update Existing' entries for Archive samples.")

# 🔹 Then continue with the rest of your app
st.title("Canister Shelf Map")

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

PREFERRED_COLUMN_ORDER = [
    "Canister ID", "Pressure (psig)", "Sample Date", "Timezone", "Location",
    "Latitude", "Longitude", "Storage Location", "Type of Entry",
    "Ambient Temperature (°C)", "Wind Speed (mph)", "Wind Direction",
    "Container Size", "Container Type", "New Sample or Measured",
    # Additional fields you might have go here
    "Notes"
]
if search_query:
    query = search_query.strip().lower()

    def match_row(row):
        can_id = str(row.get("Canister ID", "")).lower()
        location = str(row.get("Storage Location", "")).lower()
        size = str(row.get("Container Size (L)", "")).lower()
        notes = str(row.get("Notes", "")).lower()
        type = str(row.get("Type of Entry", "")).lower()
        sample_date = row.get("Sample Date", "")
        year = ""
        if pd.notna(sample_date):
            try:
                year = pd.to_datetime(sample_date).year
            except Exception:
                pass
        return (query in can_id) or (query in location) or (query == str(year)) or (query in size) or (query in notes) or (query in type)

    result = df_all[df_all.apply(match_row, axis=1)]


    if not result.empty:
        st.sidebar.success(f"Found {len(result)} match{'es' if len(result) > 1 else ''}")
        for i, row in result.iterrows():
            with st.sidebar.expander(f"Edit Canister {row['Canister ID']}"):
                updated_row = {}
    
                # Input fields in order
                for col in result:
                    val = row[col] if col in row and pd.notna(row[col]) else ""
                    updated_val = st.text_input(f"{col}", value=str(val), key=f"{col}_{i}")
                    updated_row[col] = updated_val
    
                # Update button
                if st.button(f"Update {row['Canister ID']}", key=f"save_{i}"):
                    try:
                        updated_row["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        updated_row["Type of Entry"] = f"{row['Type of Entry']}"  # Preserve entry type
    
                        client = authorize_gspread()
                        sheet = client.open("Canister Notes").worksheet("Form Responses 1")
                        form_columns = sheet.row_values(1)
    
                        full_row = [updated_row.get(col, "") for col in form_columns]
                        sheet.append_row(full_row)
    
                        st.success(f"Appended update for Canister {updated_row['Canister ID']}")
                    except Exception as e:
                        st.error(f"Failed to append update: {e}")


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
