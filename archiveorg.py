import streamlit as st
import pandas as pd
import re
import gspread
from gspread_dataframe import get_as_dataframe
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
from oauth2client.tools import run_flow
from datetime import datetime

### gspread function that checks to see if the user should be able to access data
### let me know if you have any problems with it!!!!
def authorize_gspread():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    flow = flow_from_clientsecrets("labcredentials.json", scope)
    storage = Storage("labtoken.json")
    creds = run_flow(flow, storage)
    client = gspread.authorize(creds)
    return client

# since entries are stored in a silly way, this keeps the location of the most recent canister update
# but gets all the other information from elsewhere
# when you're entering a new sample for an existing canister, enter "new sample" for the type
# "update existing" is only for samples where the content is unchanged but the location is altered
# if you do it through the streamlit interface it'll do that for you 
def consolidate_canister_entries(df):
    df = df.sort_values("Timestamp") 
    df = df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

    # finds the sample entries
    samples = df[df["Type of Entry"].str.lower() != "update existing"]
    
    # finds the update entries
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
    # keeps the og entry type from the sample entry
    merged_data["Type of Entry"] = combined["Type of Entry_sample"]
    return merged_data

@st.cache_data(show_spinner="Loading canister data...")
# loads data from the sheet
def load_data():
    def load_sheet_df(sheet_name, worksheet_title="Sheet1"):
        client = authorize_gspread()
        worksheet = client.open(sheet_name).worksheet(worksheet_title)
        df = get_as_dataframe(worksheet, dtype=str)  
        df = df.dropna(how="all")  
        return df

    df = load_sheet_df("Canister Notes", worksheet_title="Form Responses 1")

    # samples on shelves
    shelved_df = df[df["Storage Location"].str.contains(":", na=False)].copy()

    # identifies matrix locations
    # there were some inconsistencies in the original dataset
    # i typed dashes instead of colons for a couples rows of samples for... some reason...?
    # but hopefully they're all fixed now
    def parse_location(loc):
        match = re.match(r"SRTC\s+([^:]+):([A-Ja-j])([1-9])", loc)
        if match:
            room, row, col = match.groups()
            return room.upper(), row.upper(), int(col)
        return None, None, None
    
    # adds matrix locations as columns to the shelved dataframe
    shelved_df[["Room", "Row", "Col"]] = shelved_df["Storage Location"].apply(
        lambda x: pd.Series(parse_location(x))
    )
    return df, shelved_df

# makes our cute little shelf display
# i had a nightmare of a time trying to make the cans searchable by pressing buttons
# but i do not remember enough from high school CSS to really implement that
# it was so bad
# not even chatgpt could save me
# mistakes were made
# so they're just tables (for now?)
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

# lets you see the shelves of each room separveately
# was thinking about expanding this to include the floor canisters
# but the documentation for that is super inconsistent...
# I need to do a lot of restructuring to enable that...
# also was thinking about adding the cans in 409
rooms = sorted(shelved_df["Room"].dropna().unique())
selected_room = st.selectbox("Select a Room", rooms)

room_df = shelved_df[shelved_df["Room"] == selected_room]

# shelf matrices (see fig 1 of paper)
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

# searchbar for samples
# in my testing, these fields worked but lmk if not
search_query = st.sidebar.text_input("Search (Canister ID, Location, or Year)", value="")

df_all = consolidated_df.copy()

if search_query:
    query = search_query.strip().lower()

    # figures out if your query matches any of the listed fields
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
            # was thinking of having this not take people to the edit immediately
            # but also i think that's mainly what it would be used for anyways so it's fine
            with st.sidebar.expander(f"Edit Canister {row['Canister ID']}"):
                updated_row = {}
                for col in result.columns:
                    val = row[col] if pd.notna(row[col]) else ""
                    updated_val = st.text_input(f"{col}", value=str(val), key=f"{col}_{i}")
                    updated_row[col] = updated_val
                # not sureee if i like that it just updates the existing row instead of making a new one
                #  but since most of our "update existing" stuff just ends up being super basic location and pressure updates
                #I think it's probably okay for now
                ### i changed it actually
                ### it was set up that way to provide canister history and i have a way to get around that anyways
                #### this part is kinda slow so you won't see changes immediately
                ### and you'll have to reload the app sadly
                ### and it might take a bit even if you reload (i had to do it twice)
                ### it's annoying i know </3
                ### that's what i get for using a depreciated library i guess
                if st.button(f"Update {row['Canister ID']}", key=f"save_{i}"):
                    try:
                        # updates update time and entry type
                        # thought about also having it prefill the user email likw the form does?
                        updated_row["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        updated_row["Type of Entry"] = "Update Existing"

                        # clear fields that should only be filled on initial sample
                        # so people don't accidentally mess with the sample date
                        for field in ["Sample Date", "Sample Type", "Site"]:
                            updated_row[field] = ""

                        client = authorize_gspread()
                        sheet = client.open("Canister Notes").worksheet("Form Responses 1")
                        form_columns = sheet.row_values(1)  # use column order from the sheet header

                        new_row = [updated_row.get(col, "") for col in form_columns]
                        sheet.append_row(new_row)

                        st.success(f"Appended update for Canister {updated_row['Canister ID']}")
                    except Exception as e:
                        st.error(f"Failed to append update: {e}")

    else:
        st.sidebar.error("No matches found.")

    # add a new canister
    # I might restructure this a bit to have more dropdowns like the Google Form so we're less vulnerable to typos
    new_entry = {}
    form_columns = df.columns.tolist()
    for col in form_columns:
        if col.lower() == "sample date":
            new_val = st.text_input(f"{col} (new)", value=datetime.now().strftime("%Y-%m-%d"), key=f"new_{col}")
        else:
            new_val = st.text_input(f"{col} (new)", key=f"new_{col}")
        new_entry[col] = new_val
        
    # lets users put in new cans
    # like the update one, this one also takes a hot minute to load
    # so beware
    if st.button("Submit New Entry"):
        if not new_entry.get("Canister ID"):
            st.warning("Canister ID is required.")
        else:
            try:
                # client authorization
                # sorry it's annoying
                # they wanted my money to make a google cloud service account and i didn't wanna pay
                # maybe if it gets too bad i'll figure it out
                client = authorize_gspread()
                sheet = client.open("Canister Notes").worksheet("Form Responses 1")
                sheet.append_row([new_entry.get(col, "") for col in form_columns])
                st.success(f"New entry for Canister {new_entry['Canister ID']} added!")
            except Exception as eee:
                st.error(f"Failed to add new entry: {eee}")
