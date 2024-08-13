import os
import pandas as pd
import redshift_connector
from dotenv import load_dotenv
import warnings
from openai import OpenAI
import json
from prompts import prompt_list

warnings.filterwarnings('ignore')

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def prepare_warehouse_connection():
    warehouse_host = os.getenv("FELLOW_DATA_WAREHOUSE_HOST")
    warehouse_port = os.getenv("FELLOW_DATA_WAREHOUSE_PORT")
    warehouse_username = os.getenv("FELLOW_DATA_WAREHOUSE_USERNAME")
    warehouse_password = os.getenv("FELLOW_DATA_WAREHOUSE_PASSWORD")
    warehouse_db = "warehouse"

    conn = redshift_connector.connect(
        host=warehouse_host,
        database=warehouse_db,
        port=warehouse_port,
        user=warehouse_username,
        password=warehouse_password
    )
    return conn

def execute(query):
    conn = prepare_warehouse_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

information_tables = pd.read_csv('information_tables.csv')
information_columns = pd.read_csv('information_schema_no_desc.csv')

def get_relevant_tables(data_request):
    prompt = f"""
                ## Task
                Please identify the most relevant tables based on the user's request.

                ## Instructions
                1. The available tables and their columns are provided in the dictionary format: {information_tables.to_json(orient="records")}.
                2. List the relevant tables, avoiding any redundancy.
                3. Output only the list of table names, separated by commas, without any comments or extra information.

                ## Example Output
                table_name1, table_name2, table_name3
            """
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "system", "content": prompt},
                #   {"role": "user", "content": "I want to know every Shopify user's last active date"},
                #   {"role": "assistant", "content": "fct_user_activity, dim_fellow_user, dim_fellow_workspace"},
                  {"role": "user", "content": data_request}],
        temperature=0.2,
        max_tokens=100
    )
    suggested_tables = response.choices[0].message.content
    return suggested_tables.split(",")

def get_columns(suggested_tables, data_request):
    suggested_tables = [table.strip() for table in suggested_tables]
    all_columns = information_columns[information_columns['table_name'].isin(suggested_tables)]
    all_columns = all_columns.groupby(['schema_name', 'table_name']).apply(
    lambda x: {
        'column_name': x['column_name'].tolist(),
        'data_type': x['data_type'].tolist(),
        'description': x['description'].tolist()
        }
    ).to_dict()
    prompt ="""
                ## Task
                Given the user's request and the available tables and columns, provide a shorter list of columns that are most relevant to the request. 

                ## Instructions
                1. Format the answer as a Python dictionary with schema and table names as tuple keys and lists of column names as values, only output the dictionary, nothing else.
                2. Scan through all the available tables and columns before providing the output.
                3. Include enough columns to fulfill the requested information but only use the columns and tables available.
                4. Some requested columns may not be available and need to be derived from other available columns or have different names. Include the available columns that should be used.
                5. Do not invent new information. Use the exact schema names, table names, and column names provided. Do not modify or create new information.

                ## Example Output
                {('schema_name1', 'table_name1'): ['column1', 'column2', 'column3'],('schema_name2', 'table_name2'): ['columnA', 'columnB', 'columnC']}
            """
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "system", "content": prompt},
                  {"role": "user", "content": f"Available tables and columns are {all_columns} and request is {data_request}"}],
        temperature=0.2,
        max_tokens=100
    )
    suggested_columns = response.choices[0].message.content
    return suggested_columns

def generate_sql_query(table_columns):
    prompt="""
            ## Task
            Construct a complete PostgreSQL query based on the user's request and the available tables and columns.

            ## Instructions
            1. Scan through all available tables and columns before proceeding, read the column data type and description carefully.
            2. Clearly match each requested column with its corresponding schema and table in the database.
            3. If some columns need to be derived or calculated, specify how this should be done using the existing data.
            4. Write a straightforward and efficient SQL query, avoiding complex joins or subqueries unless absolutely necessary, do not use alias for tables or ctes.
            5. Ensure that all column references include their respective schema and table names for clarity.
            6. Output only the SQL query, with no additional comments or explanations.
            7. Ensure that the query is clear and directly executable in a standard PostgreSQL environment.

            ## Example Output
            SELECT schema1.table1.column1, schema2.table2.column2
            FROM schema1.table1
            JOIN schema2.table2 ON schema1.table1.id = schema2.table2.id
            WHERE schema1.table1.column3 = 'value'
            """
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "system", "content": prompt},
                #   {"role": "user", 
                #    "content": """Given \{('engagement', 'fct_user_activity'): ['user_id', 'is_active', 'snapshot_date', 'job_title'],  
                #    ('product_core', 'dim_fellow_user'): ['user_id', 'full_name', 'dob', 'workspace'] \} I want to know every Shopify user's last active date"""},
                #   {"role": "assistant", "content": """SELECT u.user_id, u.full_name, MAX(f.snapshot_date) AS last_active_date
                #                                       FROM product_core.dim_fellow_user AS u
                #                                       LEFT JOIN engagement.fct_user_activity AS f ON f.user_id = u.user_id
                #                                       WHERE u.workspace = 'Shopify'
                #                                         AND u.is_enabled AND u.is_configured AND NOT u.is_deleted
                #                                         AND f.active_this_day
                #                                       GROUP BY 1, 2"""},
                  {"role": "user", "content": f"Given {table_columns}, write the query for this request: {data_request}"}],
        max_tokens=1000,
        temperature=0.2)

    sql_query = response.choices[0].message.content
    return sql_query


data_request = prompt_list[2]
suggested_tables = get_relevant_tables(data_request)
print("******************************************************************************************************************************************************************")
print(suggested_tables)
print("******************************************************************************************************************************************************************")
table_columns = get_columns(suggested_tables, data_request)
print(table_columns)
print("******************************************************************************************************************************************************************")
sql_query = generate_sql_query(table_columns)
print("Generated SQL Query:", sql_query)
print("******************************************************************************************************************************************************************")
print(execute(sql_query))
print("******************************************************************************************************************************************************************")
