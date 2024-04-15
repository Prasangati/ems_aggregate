
"""
Name: Prasanga Tiwari
Email: prasanga.tiwari99@myhunter.cuny.edu
Resources: https://scikit-learn.org
"""
import pandas as pd
import pandasql as ps

def make_df(file_name):
    """
    Rows that are have null values for the type description, incident date, 
    incident time, borough name are dropped 
    """
    df = pd.read_csv(file_name)
    df = df.dropna(subset=['TYP_DESC', 'INCIDENT_DATE','INCIDENT_TIME','BORO_NM'])
    return df

def compute_time_delta(start, stop):
    """
    The function converts the input strings into datetime objects and 
    returns a whole number that is the difference in time in seconds. 
    """
    start = pd.to_datetime(start, format='%m/%d/%Y %I:%M:%S %p')
    stop = pd.to_datetime(stop, format='%m/%d/%Y %I:%M:%S %p')
    difference_seconds_pandas = (stop-start).total_seconds()
    return difference_seconds_pandas

def select_boro_column(_df):
    """
    Selects, using SQL, the BORO_NM column from _df. 
    Returns the resulting DataFrame from the SQL query.
    """
    query = "SELECT BORO_NM FROM _df;"

    # Execute the query and return the result
    return ps.sqldf(query)

def select_by_boro(_df, boro_name):
    """
    Selects, using SQL, all rows from the DataFrame, 
    _df, where the borough is boro_name. Returns the 
    resulting DataFrame from the SQL query. 

    """
    upper_boro_name = boro_name.upper()

    query = f"SELECT * FROM _df WHERE UPPER(BORO_NM) = '{upper_boro_name}';"

    return ps.sqldf(query)

def new_years_count(_df, boro_name):
    """
    Selects, using SQL, the number of incidents from _df, 
    called in on New Year's Day (Jan 1, 2021) in the specified borough, boro_name. 
    Returns the resulting DataFrame from the SQL query. 
    """
    # Convert boro_name to upper case
    upper_boro_name = boro_name.upper()

    # SQL query to count incidents on New Year's Day in the specified borough
    query = f"""
    SELECT COUNT(*) 
    FROM _df
    WHERE UPPER(BORO_NM) = '{upper_boro_name}'
    AND INCIDENT_DATE = '01/01/2021';
    """

    # Execute the query and return the results
    return ps.sqldf(query)

def incident_counts(_df):
    """
    Selects, using SQL, the incident counts per radio code (TYP_DESC), 
    sorted alphabetically by radio code (TYP_DESC). 
    Returns the resulting DataFrame from the SQL query.
    """

    query = """
    SELECT TYP_DESC, COUNT(*) 
    FROM _df
    GROUP BY TYP_DESC
    ORDER BY TYP_DESC;
    """
    # Execute the query and return the result
    return ps.sqldf(query)

def top_10(_df, boro_name):
    """
    using SQL, the top 10 most commonly occurring 
    incidence by radio code, and the number of incident occurrences,
    in specified borough
    """
    # Convert boro_name to upper case
    upper_boro_name = boro_name.upper()

    # SQL query to find the top 10 most common incidents by radio code in the specified borough
    query = f"""
    SELECT TYP_DESC, COUNT(*) as 'COUNT(*)'
    FROM _df
    WHERE UPPER(BORO_NM) = '{upper_boro_name}'
    GROUP BY TYP_DESC
    ORDER BY 'COUNT(*)' DESC
    LIMIT 10;
    """

    # Execute the query and return the result
    return ps.sqldf(query)
