import streamlit as st
import anthropic
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="CitiBike Data Explorer",
    page_icon=":bike:",
    layout="centered",
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_QUERIES = 20

SYSTEM_PROMPT = """You are a data analyst answering questions about NYC CitiBike ridership data (Jan 2024 - present).
You have access to three summary tables in BigQuery. Use the run_sql_query tool to query them.

IMPORTANT RULES:
- Always use fully-qualified table names: `citibike-portfolio.citibike.<table_name>`
- Only query these three tables: daily_summary, hourly_summary, station_stats
- NEVER query the trips_cleaned table (it has 90M+ rows and is too expensive)
- Write efficient SQL with appropriate filters and aggregations
- If a query returns an error, read the error and fix the SQL
- Give clear, concise answers with key numbers
- Do not show SQL unless the user asks to see it

TABLE SCHEMAS:

1. `citibike-portfolio.citibike.daily_summary`
   Best for: daily trends, monthly/yearly comparisons, overall statistics
   Columns:
   - trip_date (DATE): date
   - avg_temperature_f (FLOAT64): average temperature in Fahrenheit
   - num_trips (INT64): total trips that day
   - member_trips (INT64): trips by annual members
   - casual_trips (INT64): trips by casual/day-pass users
   - electric_trips (INT64): electric bike trips
   - classic_trips (INT64): classic bike trips
   - total_minutes (FLOAT64): total ride duration in minutes
   - member_minutes (FLOAT64): member ride minutes
   - casual_minutes (FLOAT64): casual ride minutes
   - electric_minutes (FLOAT64): electric bike ride minutes
   - classic_minutes (FLOAT64): classic bike ride minutes

2. `citibike-portfolio.citibike.hourly_summary`
   Best for: hour-of-day patterns, weather impact, day-of-week analysis
   Columns:
   - trip_date (DATE): date
   - hour_of_day (INT64): 0-23
   - day_of_week (INT64): 1=Sunday, 7=Saturday
   - day_name (STRING): e.g. "Monday"
   - temperature_f (FLOAT64): temperature in Fahrenheit
   - precipitation_mm (FLOAT64): precipitation in millimeters
   - conditions (STRING): "Sunny", "Partly Cloudy", "Cloudy", or "Unknown"
   - num_trips (INT64): total trips
   - member_trips (INT64): member trips
   - casual_trips (INT64): casual trips
   - electric_trips (INT64): electric bike trips
   - classic_trips (INT64): classic bike trips
   - total_minutes (FLOAT64): total ride minutes
   - member_minutes (FLOAT64): member ride minutes
   - casual_minutes (FLOAT64): casual ride minutes
   - electric_minutes (FLOAT64): electric bike ride minutes
   - classic_minutes (FLOAT64): classic bike ride minutes

3. `citibike-portfolio.citibike.station_stats`
   Best for: station popularity, geographic analysis, flow imbalances
   Columns:
   - station_name (STRING): station name
   - latitude (FLOAT64): station latitude
   - longitude (FLOAT64): station longitude
   - trips_started (INT64): trips originating from this station
   - trips_ended (INT64): trips ending at this station
   - total_activity (INT64): trips_started + trips_ended
   - net_flow (INT64): trips_started - trips_ended (positive = net source of bikes, negative = net sink)
"""

TOOLS = [
    {
        "name": "run_sql_query",
        "description": "Run a read-only SQL query against the CitiBike BigQuery dataset and return results.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SELECT query to execute",
                }
            },
            "required": ["sql"],
        },
    }
]

BLOCKED_KEYWORDS = [
    "DELETE", "DROP", "INSERT", "UPDATE", "CREATE",
    "ALTER", "TRUNCATE", "MERGE", "GRANT", "REVOKE",
]

# ---------------------------------------------------------------------------
# Clients (cached so they're created once per app session)
# ---------------------------------------------------------------------------

@st.cache_resource
def get_bq_client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=creds, project=creds.project_id)


@st.cache_resource
def get_claude_client():
    return anthropic.Anthropic(api_key=st.secrets["claude"]["api_key"])


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------

def run_sql_query(sql: str) -> str:
    """Execute a SQL query against BigQuery and return results as a string."""
    sql_upper = sql.upper()

    for keyword in BLOCKED_KEYWORDS:
        if keyword in sql_upper:
            return f"Error: {keyword} statements are not allowed. Only SELECT queries are permitted."

    if "TRIPS_CLEANED" in sql_upper:
        return "Error: Querying trips_cleaned is not allowed (90M+ rows). Use daily_summary, hourly_summary, or station_stats instead."

    try:
        client = get_bq_client()
        result = client.query(sql).result()
        rows = [dict(row) for row in result]

        if not rows:
            return "Query returned no results."

        # Format as a readable table string
        columns = list(rows[0].keys())
        header = " | ".join(columns)
        lines = [header, "-" * len(header)]

        for row in rows[:50]:
            lines.append(" | ".join(str(row[c]) for c in columns))

        output = "\n".join(lines)

        if len(rows) > 50:
            output += f"\n... ({len(rows)} total rows, showing first 50)"

        return output
    except Exception as e:
        return f"SQL Error: {e}"


# ---------------------------------------------------------------------------
# Claude agentic loop
# ---------------------------------------------------------------------------

def ask_claude(question: str, chat_history: list) -> str:
    """Send a question to Claude with tool use, returning the final answer."""
    client = get_claude_client()

    # Build messages from chat history plus new question
    messages = []
    for msg in chat_history:
        messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": question})

    # Agentic loop — Claude may call run_sql_query multiple times
    max_iterations = 5
    for _ in range(max_iterations):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        # If Claude is done, extract and return the text
        if response.stop_reason == "end_turn":
            text_parts = [
                block.text for block in response.content if block.type == "text"
            ]
            return "\n".join(text_parts)

        # Process tool calls
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = run_sql_query(block.input["sql"])
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    }
                )

        # Add assistant response and tool results for next iteration
        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})

    return "I wasn't able to find an answer within the allowed number of steps. Please try rephrasing your question."


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title("CitiBike Data Explorer")
st.markdown(
    "Ask natural language questions about NYC CitiBike ridership data "
    "(Jan 2024 -- present). Data sourced from "
    "[CitiBike's public S3 bucket](https://s3.amazonaws.com/tripdata/). "
    "Powered by Claude and BigQuery."
)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "query_count" not in st.session_state:
    st.session_state.query_count = 0

# Render chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Chat input
if st.session_state.query_count >= MAX_QUERIES:
    st.info("You've reached the question limit for this session. Refresh the page to start a new session.")
else:
    if prompt := st.chat_input("Ask a question about CitiBike data..."):
        # Display user message
        with st.chat_message("user"):
            st.markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        # Get and display assistant response
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                response = ask_claude(prompt, st.session_state.messages[:-1])
            st.markdown(response)

        st.session_state.messages.append({"role": "assistant", "content": response})
        st.session_state.query_count += 1
