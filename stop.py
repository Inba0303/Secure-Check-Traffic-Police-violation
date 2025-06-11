import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras # Import this for DictCursor
import plotly.express as px
from streamlit_option_menu import option_menu

def create_connection():
    try:
        connection = psycopg2.connect(
            host='localhost',
            port=5432,
            user='postgres',
            password='root123',
            database='lone_db'
        )
        return connection
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# This function is not used in the current display logic, but kept for reference
def get_data_as_dicts():
    conn = create_connection()
    if conn:
        try:
            # Create a cursor with DictCursor factory
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("SELECT * FROM check_post_logs LIMIT 10;") # Example query
            data = cursor.fetchall()
            return data
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return []
        finally:
            if conn:
                conn.close() # Always close the connection when you're done
    return []

# Set page configuration once at the top
st.set_page_config(page_title="Traffic Stop Records", layout="wide")

st.markdown(
    """
    <h1 style="text-align:center; color: red;">Welcome to SecureCheck Traffic Records</h1>
    <h6 style="text-align:center;color:green;">police  traffic recording details</h6>
    """,
    unsafe_allow_html=True
)

# Sidebar menu
with st.sidebar:
    selected = option_menu(
        menu_title="Traffic Stop records",
        options=["Home", "Traffic records Table", "Create format table", "Queries"],
        icons=["house", "book-fill", "pencil", "floppy"],
        default_index=0
    )

# --- Data Fetching Function (moved outside specific 'selected' block) ---
def fetch_data(query):
    conn = create_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                data_dicts = [dict(row) for row in result]
                df = pd.DataFrame(data_dicts)
                return df
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()
    return pd.DataFrame()

# Fetch data once when the app starts or 'Traffic records Table' is selected
# We will pass this 'data' DataFrame to different sections
data = pd.DataFrame() # Initialize an empty DataFrame
if selected == "Traffic records Table":
    query = "SELECT * FROM traffic_stops LIMIT 100;" # Assuming 'traffic_stops' is the correct table
    data = fetch_data(query) # Fetch data here

# --- Conditional Rendering based on sidebar selection ---
if selected == "Home":
    st.image("C:/Users/inbak/Downloads/Traffic-Fines-List-India-1.jpg", caption="Traffic Enforcement in India")
    st.markdown("""
    ---
    This application provides a comprehensive overview of traffic stop records.
    Navigate through the sidebar to explore different aspects of the data,
    including detailed tables, analytics, and custom queries.
    """)

elif selected == "Traffic records Table":
    st.title("Traffic Stop Records")
    st.markdown("Real-time traffic stop records from SecureCheck database")

    st.header("Traffic Stop Records Table")
    if not data.empty:
        st.dataframe(data) # Display the DataFrame

        st.header("Traffic Incident Overview")
        # Define 4 columns to accommodate all metrics
        col1, col2, col3, col4= st.columns(4)

        with col1:
            total_stops = data.shape[0]
            st.metric("Total Police Stops", total_stops)

        with col2:
            if 'vehicle_number' in data.columns:
                total_vehicles = data['vehicle_number'].nunique()
                st.metric("Total Vehicles", total_vehicles)
            else:
                st.warning("'vehicle_number' column not found.")
                st.metric("Total Vehicles", "N/A")

        with col3:
            if 'stop_outcome' in data.columns:
                arrests = data[data['stop_outcome'].str.contains("arrest", case=False, na=False)].shape[0]
                st.metric("Total Arrests", arrests)
            else:
                st.warning("'stop_outcome' column not found for arrests.")
                st.metric("Total Arrests", "N/A")

        with col4:
            if 'stop_outcome' in data.columns:
                warnings = data[data['stop_outcome'].str.contains("warning", case=False, na=False)].shape[0]
                st.metric("Total Warnings", warnings)
            else:
                st.warning("'stop_outcome' column not found for warnings.")
                st.metric("Total Warnings", "N/A")

        st.header("Traffic Stop Records Visualization")
        # Use numerical values for column ratios, e.g., [1, 1] for equal width
        tabl1_col, tabl2_col = st.columns([1, 1])

        with tabl1_col:
            st.subheader("Stops by Violation") # Add subheader for clarity
            if 'violation' in data.columns:
                violation_data = data['violation'].value_counts().reset_index()
                violation_data.columns = ['Violation', 'Count']
                fig = px.bar(violation_data, x='Violation', y='Count', title='Traffic Stops by Violation', color='Violation') # Better color by violation
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No data available for violations or 'violation' column not found.")

        with tabl2_col:
            st.subheader("Driver Gender Distribution") # Add subheader for clarity
            if 'driver_gender' in data.columns:
                gender_data = data['driver_gender'].value_counts().reset_index()
                gender_data.columns = ['Gender', 'Count']
                fig = px.pie(gender_data, names='Gender', values='Count', title='Driver Gender Distribution', color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No data available for driver gender chart or 'driver_gender' column not found.")
    else:
        st.info("No data available to display for the Traffic Records Table. Please ensure the database connection is active and the table 'traffic_stops' exists.")

elif selected == "Queries":
    st.header("Medium level queries")

    # Define query_map including the new queries
    query_map = {
        "Total Number of police stops": "SELECT COUNT(*) FROM traffic_stops;",
        "count of stops by violation Type": "SELECT violation, COUNT(*) FROM traffic_stops GROUP BY violation ORDER BY COUNT(*) DESC;",
        "Number of Arrests vs Warnings": "SELECT stop_outcome, COUNT(*) FROM traffic_stops GROUP BY stop_outcome ORDER BY COUNT(*) DESC;",
        "Average age of drivers stopped": "SELECT AVG(driver_age) FROM traffic_stops WHERE driver_age IS NOT NULL;",
        "Top 5 most frequest search Types": "SELECT search_type, COUNT(*) FROM traffic_stops GROUP BY search_type ORDER BY COUNT(*) DESC LIMIT 5;",
        "count of stops by Gender": "SELECT driver_gender,count(*) FROM traffic_stops GROUP BY driver_gender ORDER BY count(*) DESC;",
        "Most common Violation for Arrests": "SELECT violation, COUNT(*) FROM traffic_stops WHERE stop_outcome ILIKE '%arrest%' GROUP BY violation ORDER BY COUNT(*) DESC LIMIT 1;",

        # New Queries for your questions:

        # Which driver age group had the highest arrest rate?
        # Assuming 'driver_age' and 'stop_outcome' columns exist.
        # This query creates age bins (0-19, 20-29, etc.) and calculates arrest rate for each.
        "Arrest Rate by Driver Age Group": """
            SELECT
                CASE
                    WHEN driver_age BETWEEN 0 AND 19 THEN '0-19'
                    WHEN driver_age BETWEEN 20 AND 29 THEN '20-29'
                    WHEN driver_age BETWEEN 30 AND 39 THEN '30-39'
                    WHEN driver_age BETWEEN 40 AND 49 THEN '40-49'
                    WHEN driver_age BETWEEN 50 AND 59 THEN '50-59'
                    WHEN driver_age >= 60 THEN '60+'
                    ELSE 'Unknown'
                END AS age_group,
                CAST(COUNT(CASE WHEN stop_outcome ILIKE '%arrest%' THEN 1 END) AS REAL) * 100.0 / COUNT(*) AS arrest_rate_percentage
            FROM traffic_stops
            WHERE driver_age IS NOT NULL
            GROUP BY age_group
            ORDER BY arrest_rate_percentage DESC;
        """,

        
        # Which race and gender combination has the highest search rate?
        # Requires 'driver_race', 'driver_gender', and 'search_conducted' (boolean) columns.
        "Search Rate by Race and Gender Combination": """
            SELECT
                COALESCE(driver_race, 'Unknown') AS driver_race,
                COALESCE(driver_gender, 'Unknown') AS driver_gender,
                CAST(COUNT(CASE WHEN search_conducted = TRUE THEN 1 END) AS REAL) * 100.0 / COUNT(*) AS search_rate_percentage
            FROM traffic_stops
            WHERE driver_race IS NOT NULL AND driver_gender IS NOT NULL
            GROUP BY driver_race, driver_gender
            ORDER BY search_rate_percentage DESC;
        """,   
     
    }

    # Use a single selectbox for query selection
    selected_query = st.selectbox("Select a Query to Run", list(query_map.keys()))

    if st.button("Run Query"):
        result_df = fetch_data(query_map[selected_query])
        if not result_df.empty:
            st.subheader(f"Results for: {selected_query}")
            st.dataframe(result_df)
            # Optional: Add visualizations for some queries if appropriate
            if selected_query == "Most Traffic Stops by Time of Day (Hour)":
                fig = px.bar(result_df, x='hour_of_day', y='total_stops', title='Traffic Stops by Hour of Day',
                             labels={'hour_of_day': 'Hour of Day (24-hour)', 'total_stops': 'Total Stops'},
                             color='total_stops', color_continuous_scale=px.colors.sequential.Plasma)
                st.plotly_chart(fig, use_container_width=True)
            elif selected_query == "Arrest Rate by Driver Age Group":
                fig = px.bar(result_df, x='age_group', y='arrest_rate_percentage', title='Arrest Rate by Driver Age Group',
                             labels={'age_group': 'Age Group', 'arrest_rate_percentage': 'Arrest Rate (%)'},
                             color='arrest_rate_percentage', color_continuous_scale=px.colors.sequential.Viridis)
                st.plotly_chart(fig, use_container_width=True)
            elif selected_query == "Arrest Rate for Night vs. Day Stops":
                fig = px.bar(result_df, x='time_of_day_category', y='arrest_rate_percentage', title='Arrest Rate: Night vs. Day Stops',
                             labels={'time_of_day_category': 'Time Category', 'arrest_rate_percentage': 'Arrest Rate (%)'},
                             color='arrest_rate_percentage', color_continuous_scale=px.colors.sequential.Magma)
                st.plotly_chart(fig, use_container_width=True)


        else:
            st.warning("No results found for the selected query. Please ensure the necessary columns exist in your 'traffic_stops' table.")
            st.info("Note: Queries for 'country', 'driver_race', 'search_conducted', 'stop_datetime', and 'stop_duration_minutes' require these columns to be present in your 'traffic_stops' table.")




elif selected == "Create format table":
    st.header("Traffic police Data Entry Record Format")
    st.markdown("""For creating a custom format table, please ensure you have the necessary columns in your 'traffic_stops' table required for the queries you want to run.""")

    st.header("Add New Police Stop Record & Predict Outcome and Violation")
    # Set up the app title
    with st.form("add_record_form"):
        stop_date = st.date_input("Stop Date")
        stop_time = st.time_input("Stop Time", value=pd.to_datetime('14:30:00').time())
        country_name = st.text_input("Country Name")
        driver_gender = st.selectbox("Driver Gender", ["Male", "Female", "Other"], index=0)  # Male selected
        driver_age = st.number_input("Driver Age", min_value=18, max_value=100, value=27)  # 27 years old
        vehicle_number = st.text_input("Vehicle Number")
        car_type = st.selectbox("Car Type", ["Sedan", "SUV", "Truck", "Motorcycle", "Other"])
        search_conducted = st.selectbox("Search Conducted", ["Yes", "No"], index=1)  # No selected
        search_type = st.selectbox("Search Type", ["Frisk", "Vehicle", "Consent", "Probable Cause", "Inventory", "Other"])
        stop_duration_minutes = st.number_input("Stop Duration (minutes)", min_value=1, max_value=120, value=10)
        is_arrest = st.selectbox("Is Arrest Made?", ["Yes", "No"], index=1)  # No selected (citation given)
        drug_related_stop_input = st.selectbox("Is Drug Related Stop?", ["Yes", "No"], index=1)  # No selected
        violation = st.selectbox("violation",["Speeding" ,"Normal checking","Traffic rules violation"]) # Known violation
        stop_outcome = "Citation"  # Known outcome
    
        submitted = st.form_submit_button("Submit Traffic Stop Data")
    
    if submitted:
        # Format the data as a narrative
        narrative = f"🚗 A {driver_age}-year-old {driver_gender.lower()} driver was stopped for {violation} at {stop_time.strftime('%I:%M %p')}. "
        narrative += f"No search was conducted, and he received a {stop_outcome.lower()}. "
        narrative += f"The stop lasted {stop_duration_minutes} minutes and was not drug-related."
        narrative +=f" The vehicle was a {car_type.lower()} with the number plate {vehicle_number}."
        narrative +=f" The is_arrest was {is_arrest.lower()} and the driver was of {drug_related_stop_input.lower()} nature."
        
        # Display the narrative
        st.write(narrative)
        
