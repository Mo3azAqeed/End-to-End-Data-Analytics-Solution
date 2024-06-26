import pandas as pd


def get_uniques_and_indexing(file_name, col1, col2, col3="", col4="",col5=""):
    """
  This function efficiently extracts unique rows based on up to four columns, sorts them, and assigns numerical indices.

  Args:
      file_name (str): The path to the CSV file.
      col1 (str): The first column name for duplicate removal.
      col2 (str): The second column name (optional) for duplicate removal and sorting.
      col3 (str, optional): The third column name (optional) for duplicate removal.
      col4 (str, optional): The fourth column name (optional) for duplicate removal.
      col5 (str, optional): The fifth column name (optional) for duplicate removal.

  Returns:
      pandas.DataFrame: A new DataFrame containing unique rows, sorted, and with a new "ID" column for indexing.
    """
    df = pd.read_csv(file_name, index_col=False)
  # Select relevant columns based on provided arguments
    selected_cols = [col1, col2]
    if col3:
        selected_cols.append(col3)
    if col4:
        selected_cols.append(col4)
    if col5:
        selected_cols.append(col5)
    df_new = df[selected_cols]
    # Remove duplicate rows based on col1 (and optionally col2,col3,col4,col5)
    df_new_unique = df_new.drop_duplicates(subset=selected_cols)
    # Sort and add "ID" column
    df_new_sorted = df_new_unique.sort_values(by=selected_cols, ascending=True)
    df_new_sorted["ID"] = range(1, len(df_new_sorted) + 1)
    # Return desired columns
    return df_new_sorted[["ID"] + selected_cols].reset_index(drop=True)

def get_unique_Dates(file_paths,date_columns):
    """
  This function efficiently extracts unique Dates From Varuios files With each one Has More Than One Dates Column .
   And Best Of that with Diffrent dates formate

  Args:
      file_paths (list):A list Contains The path to the CSV files.
      date_columns (list):A Nested List contain A lists of Date Columns with Date Columns in Eaach CSV file Respectively
  Returns:
      pandas.DataFrame: A new DataFrame containing unique rows, sorted, and with a new "ID" column for indexing.
    """
    all_dates = []
    # Extract dates from each file and column
    for file_path, columns in zip(file_paths, date_columns):
        df = pd.read_csv(file_path, usecols=columns)
        for column in columns:
            all_dates.extend(df[column].dropna().tolist())
    unique_dates = set(all_dates)
    sorted_unique_dates = list(unique_dates)
    # Create a DataFrame with the unique, sorted dates
    dates_df = pd.DataFrame({'Date': sorted_unique_dates})
    dates_df['Date'] = pd.to_datetime(dates_df['Date'],format='mixed')
    # Sort the DataFrame by the Date column (should already be sorted)
    dates_df = dates_df.sort_values(by='Date').reset_index(drop=True)
    dates_df['ID'] = range(1, len(dates_df) + 1)
    dates_df.drop_duplicates(subset="Date",inplace=True)
    dates_df=dates_df[['ID','Date']]
    dates_df['DayOfWeek']=dates_df['Date'].dt.day_name()
    dates_df['Month']=dates_df['Date'].dt.month
    dates_df['Quarter']=dates_df['Date'].dt.quarter
    dates_df['Year']=dates_df['Date'].dt.year
    dates_df["IsWeekend"] = dates_df['Date'].dt.day_of_week > 5
    return dates_df[['ID', 'Date', 'DayOfWeek', 'Month', 'Quarter', 'Year', 'IsWeekend']]


def normalized_dated_df(file_paths, date_columns):
    """ 
    This function normalizes date columns in multiple CSV files by replacing each date with a corresponding ID from a unified date dimension table.
    It ensures that all date columns are consistently represented across different files.
    And Save the new Data Framee into CSV After Being Normalized
    Args:
        file_paths (list): A list containing the paths to the CSV files.
        date_columns (list): A nested list containing lists of date columns in each CSV file respectively.
    Returns:
        dict: A dictionary where keys are file paths and values are pandas DataFrames with normalized date columns.
    """
    # Create the Time_df containing unique dates and their IDs
    Time_df = get_unique_Dates(file_paths, date_columns)
    Time_df.to_csv("Dim_Time_keys.csv",index=False)
    
    normalized_dataframes = {}
    for file_path, columns in zip(file_paths, date_columns):
        df = pd.read_csv(file_path)
        df_normalized = df.copy()
        for col in columns:
            df_normalized[col] = pd.to_datetime(df_normalized[col], errors='coerce')
            Time_df['Date'] = pd.to_datetime(Time_df['Date'], errors='coerce')
            df_normalized = pd.merge(df_normalized, Time_df[['ID', 'Date']], left_on=col, right_on='Date', how='left')
            df_normalized[col] = df_normalized['ID']
            df_normalized.rename(columns={'ID':f'{col}_ID'},inplace=True)
            df_normalized.drop(columns=['Date',col], inplace=True)
        print(f"{file_path} is Succefully normalized and saved")
        normalized_dataframes[file_path] = df_normalized
    return normalized_dataframes



# List of file paths
file_paths = [
    'PurchasesFINAL12312016.csv',
    'InvoicePurchases12312016.csv',
    'SalesFINAL12312016.csv'
]


# List of date columns in each file
date_columns =[
    ["PayDate","PODate","ReceivingDate","InvoiceDate"], 
    ['InvoiceDate',	'PODate','PayDate'],
    ['SalesDate']
    ]


