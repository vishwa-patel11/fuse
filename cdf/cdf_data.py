# Databricks notebook source
def fix_dictionary(data):
    # Check if 'Acquisitions' is None
    if data.get('Acquisitions') is None:
        # Create a new 'Acquisitions' dictionary with the correct keys but all values set to zeros
        data['Acquisitions'] = {}
        for key in data['Appeals']:
            # Replace 'Appeals' with 'Acquisitions' in the key
            new_key = key.replace('Appeals', 'Acquisitions')
            # Assign (0.0, 0.0, 0.0) for the new key in 'Acquisitions'
            # We maintain the structure but replace actual values with zeros
            data['Acquisitions'][new_key] = (1, 1, 1)
    return data
    
def calculate_cdf_dataframe(paramDict, start_day=0, end_day=340, num_points=341):
    """
    This function calculates the Cumulative Distribution Function (CDF) for each campaign type based on gamma distribution parameters provided in 'paramDict'. It generates a DataFrame that lists the percentage of completion (as determined by the CDF) for each campaign type across a specified range of days.

    Dependencies:
    - Numpy for generating a range of days using linspace.
    - Pandas for creating and manipulating the DataFrame.
    - Scipy.stats for computing the gamma CDF.

    Parameters:
    - paramDict (dict): A dictionary where keys are campaign types (e.g., 'ACQ', 'APP') and values are tuples containing the parameters (shape, loc, scale) for the gamma distribution associated with each campaign type.
    - start_day (int, optional): The starting day number for the CDF calculation. Defaults to 0.
    - end_day (int, optional): The ending day number for the CDF calculation. Defaults to 340.
    - num_points (int, optional): The number of points to calculate between 'start_day' and 'end_day', inclusive. This determines the resolution of the CDF calculation. Defaults to 341.

    Returns:
    - pd.DataFrame: A DataFrame where each column represents a different campaign type, and each row represents the percentage of completion for that campaign type at a given day. The first column lists the day numbers.

    Example:
    >>> paramDict = {'ACQ': (1.753, 0, 22), 'APP': (1.843, 0, 24)}
    >>> df_cdf = calculate_cdf_dataframe(paramDict)
    >>> print(df_cdf.head())
        Number of Days  ACQ Percent Complete  APP Percent Complete
    0              0.0                   0.0                   0.0
    1              1.0                   x.x                   y.y
    2              2.0                   x.x                   y.y
    ...
    
    Note: 'x.x' and 'y.y' represent calculated percentage values based on the gamma CDF for 'ACQ' and 'APP' respectively.
    """
    # Define the range of days for the CDF calculation
    days_range = np.linspace(start_day, end_day, num_points)

    # Initialize a dictionary to hold CDF results
    cdf_results = {'Number of Days': days_range}

    # Calculate the CDF for each campaign type using their parameters
    for campaign_type, params in paramDict.items():
        # Unpack the parameters for the gamma distribution
        shape, loc, scale = params
        # Calculate the CDF for the given range of days
        cdf = gamma.cdf(days_range, a=shape, loc=loc, scale=scale)
        # Store the CDF results
        cdf_results[f'{campaign_type} Percent Complete'] = cdf * 100  # Convert to percentage

    # Convert the results into a DataFrame
    df_cdf = pd.DataFrame(cdf_results)

    return df_cdf

def generate_end_years(df, clientMap):
    """
    Updates the clientMap with end years for campaign types for each client.
    Calculates the end years based on the maximum fiscal year in 'df', 
    counting back 3 years. This tuple of years is then appended to each 
    campaign type for every client in the clientMap.

    Dependencies:
    - Pandas library for DataFrame manipulation.

    Parameters:
    df (DataFrame): A pandas DataFrame containing at least a 'FiscalYear' column.
    clientMap (dict): A nested dictionary containing client and their campaign type details.

    Returns:
    dict: The updated clientMap with end years added to each campaign type for each client.

    Example:
    # >>> df = pd.DataFrame({'FiscalYear': [2018, 2019, 2020, 2021]})
    # >>> clientMap = {"Client1": {"CampaignTypes": {"TypeA": [], "TypeB": []}}}
    # >>> updated_clientMap = generate_end_years(df, clientMap)
    # >>> updated_clientMap["Client1"]["CampaignTypes"]["TypeA"]
    [(18, 19, 20)]

    Operations:
    - Finds the maximum fiscal year from the 'FiscalYear' column of the DataFrame.
    - Calculates the start fiscal year, which is three years prior to the maximum fiscal year.
    - Creates a tuple of end years, consisting of the last two digits of each year from the 
    start fiscal year to the year before the maximum fiscal year.
    - Iterates through each client and campaign type in the clientMap, appending the tuple 
    of end years to the campaign type details.
    """
    # Find the maximum fiscal year and calculate the start year (3 years back)
    max_fiscal_year = df['FiscalYear'].max()
    start_fiscal_year = max_fiscal_year - 3

    # Create a tuple of end years (last two digits), from start_fiscal_year to max_fiscal_year - 1
    end_years = tuple(str(year)[-2:] for year in range(start_fiscal_year, max_fiscal_year))

    # Update each campaign type in clientMap with separate tuples for start patterns and end years
    for campaign_type in clientMap['CampaignTypes']:
        start_patterns = tuple(clientMap['CampaignTypes'][campaign_type])

        # Update the campaign type with two tuples: start patterns and end years
        clientMap['CampaignTypes'][campaign_type] = [start_patterns, end_years]

    return clientMap

def vectorized_subtract_dates(df, result_dict):
    """
    The function takes a Pandas DataFrame and a result_dict mapping CampaignCode to dates.
    It returns a new DataFrame with an additional column 'DateDelta' representing the number of days
    between the gift date and mail date.

    Dependencies:
    - Pandas library for DataFrame manipulation and datetime operations.

    Parameters:
    - df (DataFrame): A Pandas DataFrame with 'CampaignCode' and 'GiftDate' columns.
    - result_dict (dict): A dictionary mapping 'CampaignCode' to mail dates.
    
    Returns:
    - DataFrame: The input DataFrame with an additional 'DateDelta' column.

    Example:
    # >>> df = pd.DataFrame({'CampaignCode': ['C1', 'C2'], 'GiftDate': ['2021-01-01', '2021-01-10']})
    # >>> result_dict = {'C1': '2020-12-25', 'C2': '2021-01-05'}
    # >>> updated_df = vectorized_subtract_dates(df, result_dict)
    # >>> updated_df[['CampaignCode', 'DateDelta']]
    [DataFrame with 'DateDelta' column showing the difference in days]

    Operations:
    - Maps 'CampaignCode' to 'MailDate' using the 'result_dict' and converts it to datetime.
    - Converts 'GiftDate' to datetime format.
    - Calculates 'DateDelta' as the difference in days between 'GiftDate' and 'MailDate'.
    - Handles invalid dates by setting the 'DateDelta' for those records to None.
    """

    # Convert CampaignCode to mail_date using result_dict
    df['MailDate'] = df['CampaignCode'].map(result_dict).astype('datetime64[ns]')

    # Convert GiftDate to datetime
    df['GiftDate'] = pd.to_datetime(df['GiftDate'])

    # Subtract dates and get the difference in days
    df['DateDelta'] = (df['GiftDate'] - df['MailDate']).dt.days

    # Handle invalid dates
    df.loc[~df['GiftDate'].notna() | ~df['MailDate'].notna(), 'DateDelta'] = None

    return df

def get_params(df, Campaign, end_year):
    """
    Fits a gamma distribution to the 'DateDelta' values of a specified campaign and fiscal year in a DataFrame. 
    The function filters data based on CampaignCode and FiscalYear, handles edge cases like empty data frames or 
    non-finite values, and checks the validity of fitted parameters.

    Dependencies:
    - numpy for numerical operations and checks.
    - scipy.stats.gamma for fitting and evaluating the gamma distribution.
    - matplotlib.pyplot for plotting histograms and distribution curves.

    Parameters:
    - df (DataFrame): DataFrame with campaign data, including 'CampaignCode', 'FiscalYear', and 'DateDelta' columns.
    - Campaign (str): String specifying the campaign code.
    - end_year (int): Integer specifying the fiscal year.

    Returns:
    - params (tuple): Tuple (shape, loc, scale) of the fitted gamma distribution, or None if checks fail or an exception occurs.

    Example:
    # >>> data = {'CampaignCode': ['C1', 'C1'], 'FiscalYear': [2020, 2020], 'DateDelta': [10, 15]}
    # >>> df = pd.DataFrame(data)
    # >>> params = get_params(df, 'C1', 2020)
    # >>> params
    (shape, loc, scale values of fitted gamma distribution)

    Operations:
    - Filters the DataFrame for the specified campaign and fiscal year.
    - Handles cases with empty data frames or non-finite 'DateDelta' values.
    - Fits a gamma distribution to the 'DateDelta' values.
    - Checks for valid fitted parameters, including checking for NaN values and bounds on the shape parameter.
    - Plots a histogram of 'DateDelta' with the fitted gamma distribution curve.
    - Prints campaign details and fitted parameters.
    """

    mask = (df.CampaignCode == Campaign) & (df.FiscalYear == end_year) 
    _df = df.loc[mask]

    # Handle empty data frames
    if _df.empty:
        print(f"Skipping {Campaign} due to empty data frame.")
        return None

    # Handle non-finite values in the 'DateDelta' column
    if not np.isfinite(_df['DateDelta']).all():
        print(f"Skipping {Campaign} due to non-finite values in 'DateDelta'.")
        return None

    try:
        # Fit gamma distribution
        params = stats.gamma.fit(_df['DateDelta'])
    except Exception as e:
        print(f"Skipping {Campaign} due to an exception during gamma fit: {e}")
        return None

    # Check for NaN values in params
    if any(np.isnan(param) for param in params):
        print(f"Skipping {Campaign} due to NaN values in parameters.")
        return None

    # Skip if shape parameter is less than 0.3 or greater than 5
    shape_param = params[0]
    if shape_param < 0.3 or shape_param > 5:
        print(f"Skipping {Campaign} due to shape parameter being out of bounds (less than 0.3 or greater than 5).")
        return None

    gift_sum = _df.groupby('DateDelta')['GiftAmount'].sum()
    gift_count = _df.groupby('DateDelta').size()

    # Plotting sum of GiftAmount for each DateDelta
    plt.figure(figsize=(14, 6))
    gift_sum.plot(kind='bar', color='b')
    plt.title(f"Campaign: {Campaign} - Total Gift Amount by DateDelta")
    plt.xlabel('DateDelta')
    plt.ylabel('Total Gift Amount')
    plt.show()

    # Plotting count of gifts for each DateDelta
    plt.figure(figsize=(14, 6))
    gift_count.plot(kind='bar', color='g')
    plt.title(f"Campaign: {Campaign} - Gift Count by DateDelta")
    plt.xlabel('DateDelta')
    plt.ylabel('Gift Count')
    plt.show()

    print(f"Campaign: {Campaign}")
    print(f"Shape: {shape_param}, Loc: {params[1]}, Scale: {params[2]}")

    return params


def calculate_campaign_average_from_campaigns(df, Campaigns, curve_type, client_map):
    """
    Calculates the average gamma distribution parameters for a set of campaigns from a DataFrame, 
    based on a specific client and curve type. The function relies on 'get_params' to fit gamma 
    distributions for each campaign and uses 'client_map' to extract campaign years. It then averages 
    the parameters of successful gamma fits.

    Dependencies:
    - get_params(df, Campaign, end_year): Fits a gamma distribution to 'DateDelta' values for a campaign.

    Parameters:
    - df (DataFrame): DataFrame with campaign data, including 'CampaignCode', 'FiscalYear', and 'DateDelta' columns.
    - Campaigns (dict): Dictionary of campaign codes.
    - curve_type (str): String specifying the type of curve (e.g., 'Linear', 'Exponential').
    - client_map (dict): Dictionary mapping client names to properties (like 'YearPosition').
    - client_name (str): String specifying the client name.

    Returns:
    - CampAVG (dict): Dictionary with the average gamma distribution parameters for the specified campaigns,
    or None if no valid campaigns are found.

    Example:
    # >>> df = pd.DataFrame({...})
    # >>> Campaigns = {"Campaign1": ..., "Campaign2": ...}
    # >>> curve_type = "Linear"
    # >>> client_map = {"Client1": {"YearPosition": ...}}
    # >>> client_name = "Client1"
    # >>> CampAVG = calculate_campaign_average_from_campaigns(df, Campaigns, curve_type, client_map, client_name)
    # >>> CampAVG
    {'Linear Average Response Curve': (average_shape, average_loc, average_scale)}

    Operations:
    - Extracts the fiscal year and campaign year position based on the client.
    - Loops through the specified campaigns to calculate gamma distribution parameters.
    - Skips campaigns with invalid parameters.
    - Calculates and stores the average of the valid gamma distribution parameters.
    - Returns the dictionary of averaged parameters or None if no valid campaigns are found.
"""

    CampaignDict = {}

    # Extract the end year - 1
    end_year = df.FiscalYear.max() - 2

    # Retrieve the year position directly from client_map
    year_position = client_map['YearPosition']

    # Loop through the specified campaigns in Campaigns dictionary
    for Campaign in Campaigns:
        # Extract the year part of the campaign based on year_position
        campaign_year = Campaign[year_position: year_position + 2]

        # Check if the campaign year matches the last two characters of end_year - 1
        if campaign_year == str(end_year)[2:]:
            params = get_params(df, Campaign, end_year)

            # Skip the campaign if parameters are not valid
            if params is None:
                print(f"Skipping {Campaign} due to negative parameter, shape less than 0.3, or None parameter.")
                continue

            print(f"Campaign: {Campaign}")

            # Check if params has at least three elements before accessing properties
            if len(params) >= 3:
                print(f"Shape: {params[0]}, Loc: {params[1]}, Scale: {params[2]}")
                CampaignDict[Campaign] = params
                print("*" * 50)
            else:
                print(f"Invalid params for {Campaign}: {params}")

    # Calculate and return average parameters
    if CampaignDict:
        total_sum = (0, 0, 0)

        # Calculate the sum of values in CampaignDict
        for key in CampaignDict:
            values = CampaignDict[key]
            total_sum = tuple(sum(x) for x in zip(total_sum, values))

        # Calculate the average
        average_values = tuple(x / len(CampaignDict) for x in total_sum)

        # Determine the curve type for the key name
        curve_type_name = f"{curve_type} " if curve_type else ""
        # Create the CampAVG dictionary
        CampAVG = {f"{curve_type_name}Average Response Curve": average_values}
        return CampAVG
    else:
        print("No campaigns found for averaging.")
        return None

def process_campaign_data(data_frame):
    """
    Processes campaign data from a DataFrame to clean and analyze campaign dates and counts. 
    The function cleans 'MailDate' values, converts them to datetime format, calculates the 
    count of each 'CampaignCode', determines the minimum 'MailDate' for each 'CampaignCode', 
    and creates a sorted DataFrame and a dictionary based on these values.

    Dependencies:
    - Pandas library for DataFrame manipulation and datetime operations.

    Parameters:
    - data_frame (DataFrame): DataFrame containing campaign data with columns 'MailDate' and 'CampaignCode'.

    Returns:
    - Tuple containing two elements:
      1. A sorted DataFrame with columns 'CampaignCode', 'Count', and 'MinMailDate'.
      2. A dictionary with 'CampaignCode' as keys and 'MailDate' as values.

    Example:
    # >>> data = {'CampaignCode': ['C1', 'C2', 'C1'], 'MailDate': ['2021-01-01', '2021-01-02', '2021-01-03']}
    # >>> df = pd.DataFrame(data)
    # >>> sorted_campaigns, result_dict = process_campaign_data(df)
    # >>> sorted_campaigns
    [DataFrame showing sorted campaign data]
    # >>> result_dict
    {'C1': '2021-01-03', 'C2': '2021-01-02'}

    Operations:
    - Cleans 'MailDate' values by stripping whitespace and removing non-printable characters.
    - Converts 'MailDate' to datetime format, handling errors by coercion.
    - Calculates the count of each 'CampaignCode'.
    - Determines the minimum 'MailDate' for each 'CampaignCode'.
    - Creates a DataFrame with 'CampaignCode', its count, and minimum 'MailDate'.
    - Sorts this DataFrame by count (descending) and then by 'MinMailDate'.
    - Generates a dictionary with 'CampaignCode' as keys and 'MailDate' as values.

"""

    # Check if 'MailDate' is a string type and then apply string methods
    if data_frame['MailDate'].dtype == object:
        data_frame['MailDate'] = data_frame['MailDate'].str.strip()
        data_frame['MailDate'] = data_frame['MailDate'].apply(lambda x: ''.join(filter(str.isprintable, x)) if x is not None else None)

    # Convert 'MailDate' to datetime
    data_frame['MailDate'] = pd.to_datetime(data_frame['MailDate'], errors='coerce')

    # Calculate campaign counts
    campaign_counts = data_frame['CampaignCode'].value_counts()

    # Calculate the minimum MailDate for each CampaignCode
    min_mail_dates = data_frame.groupby('CampaignCode')['MailDate'].min()

    # Create a DataFrame with counts and minimum MailDates
    result_df = pd.DataFrame({
        'CampaignCode': campaign_counts.index, 
        'Count': campaign_counts.values, 
        'MinMailDate': min_mail_dates.values
    })

    # Sort the DataFrame first by count (in descending order), then by MinMailDate
    sorted_campaigns = result_df.sort_values(by=['Count', 'MinMailDate'], ascending=[False, True])

    # Create a dictionary with CampaignCode as keys and MailDate as values
    result_dict = data_frame.set_index('CampaignCode')['MailDate'].to_dict()

    return sorted_campaigns, result_dict

def extract_and_process_campaigns(df, sc, unique_campaign_codes, fiscal_start_month):
    """
    Extracts and processes specific campaign data from a DataFrame based on campaign types and fiscal year criteria. 
    The function verifies required campaign types, calculates fiscal years for gifts, extracts campaign codes within 
    the current fiscal year, and creates dictionaries for the maximum 'GiftDate' and 'MailDate' for each extracted 
    campaign code.

    Dependencies:
    - Pandas library for DataFrame manipulation and datetime operations.
    - 'fiscal_from_column' function to calculate the fiscal year from a date column.

    Parameters:
    - df (DataFrame): DataFrame with campaign data including columns 'CampaignCode' and 'GiftDate'.
    - sc (DataFrame): DataFrame containing mail campaign data with columns 'CampaignCode' and 'MailDate'.
    - unique_campaign_codes (dict): Dictionary with campaign types ('Appeals', 'Acquisitions', etc.) as keys 
      and lists of campaign codes as values.
    - fiscal_start_month (int): Integer representing the start month of the fiscal year.

    Returns:
    - Tuple of two dictionaries:
      1. max_gift_dates_campaigns: Dictionary with campaign codes as keys and the maximum 'GiftDate' as values.
      2. max_mail_dates_campaigns: Dictionary with campaign codes as keys and the maximum 'MailDate' as values.
      Returns (None, None) if required campaign types are missing or no valid campaigns are found.

    Example:
    # >>> df = pd.DataFrame({...})
    # >>> sc = pd.DataFrame({...})
    # >>> unique_campaign_codes = {'Appeals': ['A1', 'A2'], 'Acquisitions': ['B1', 'B2']}
    # >>> fiscal_start_month = 7
    # >>> max_gift_dates, max_mail_dates = extract_and_process_campaigns(df, sc, unique_campaign_codes, fiscal_start_month)
    # >>> max_gift_dates
    {'A1': '2021-07-15', 'B1': '2021-08-20'}
    # >>> max_mail_dates
    {'A1': '2021-07-10', 'B1': '2021-08-15'}

    Operations:
    - Verifies the presence of 'Appeals' and 'Acquisitions' in unique_campaign_codes.
    - Calculates the fiscal year for each gift based on 'GiftDate' and fiscal_start_month.
    - Determines the current fiscal year considering the fiscal start month.
    - Extracts campaign codes within the current fiscal year that meet a 365-day criterion.
    - Creates dictionaries for the maximum 'GiftDate' and 'MailDate' for each extracted campaign code.
    """

    if 'Appeals' not in unique_campaign_codes or 'Acquisitions' not in unique_campaign_codes:
        print("Missing 'Appeals' or 'Acquisitions' in unique_campaign_codes.")
        return None, None

    # Determine fiscal year for each gift
    df['FiscalYear'] = fiscal_from_column(df, 'GiftDate', fiscal_start_month) 

    # Find the current fiscal year
    current_date = pd.to_datetime('today')
    current_fiscal_year = current_date.year + (current_date.month >= fiscal_start_month)

    print("Current Fiscal Year: FY" + str(current_fiscal_year))  # Debugging statement

    # Extract campaign codes within the current fiscal year and meeting the 365-day criterion
    keys_to_extract = {}
    for campaign_type, codes in unique_campaign_codes.items():
        keys_to_extract[campaign_type] = []
        for code in codes:
            campaign_data = df[df['CampaignCode'] == code]
            if not campaign_data.empty:
                campaign_fiscal_years = campaign_data['FiscalYear'].unique()
                if current_fiscal_year in campaign_fiscal_years:
                    min_date = campaign_data['GiftDate'].min()
                    max_date = campaign_data['GiftDate'].max()
                    if pd.notna(min_date) and pd.notna(max_date) and (max_date - min_date).days < 340:
                        keys_to_extract[campaign_type].append(code)

    print("Keys to Extract:", keys_to_extract)  # Debugging statement

    if not any(keys_to_extract.values()):
        print("No keys to extract. Check unique_campaign_codes and fiscal year.")

    # Create dictionaries for max_gift_dates_campaigns and max_mail_dates_campaigns
    max_gift_dates_campaigns = {key: df[df['CampaignCode'] == key]['GiftDate'].max() for ct, codes in keys_to_extract.items() for key in codes}
    max_mail_dates_campaigns = {key: sc[sc['CampaignCode'] == key]['MailDate'].max() for ct, codes in keys_to_extract.items() for key in codes}

    print("Max Gift Dates Campaigns:", max_gift_dates_campaigns)  # Debugging statement
    print("Max Mail Dates Campaigns:", max_mail_dates_campaigns)  # Debugging statement

    return max_gift_dates_campaigns, max_mail_dates_campaigns

def days_in_subsequent_months(input_date):

    """
    Calculates the number of days remaining in the input month and in the subsequent eleven months.
    The function computes the days remaining in the month of the input date and the total days in each 
    of the next eleven months, considering leap years if applicable. It handles year transitions and 
    leap years correctly.

    Dependencies:
    - calendar.monthrange() from the calendar module to determine the number of days in a month.

    Parameters:
    - input_date (datetime.date or datetime.datetime): A datetime object representing the date for 
      which the calculation is to be done.

    Returns:
    - List[int]: A list of integers where the first element is the number of days remaining in the 
      month of the input date and the subsequent elements are the total days in each of the next 
      eleven months.

    Example:
    # >>> from datetime import date
    # >>> input_date = date(2021, 1, 15)
    # >>> remaining_days = days_in_subsequent_months(input_date)
    # >>> remaining_days
    [16, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    Operations:
    - Determines the number of days in the month of the input date.
    - Calculates the remaining days in the input month.
    - Iterates over the next eleven months, calculating the number of days in each month.
    - Accounts for year changes and leap years in the calculation.
    """

    _, days_in_start_month = monthrange(input_date.year, input_date.month)
    remaining_days = [days_in_start_month - input_date.day]

    for i in range(1, 12):
        next_month = (input_date.month + i) % 12
        if next_month == 0:
            next_month = 12
        _, days_in_month = monthrange(input_date.year + (input_date.month + i - 1) // 12, next_month)
        remaining_days.append(days_in_month)

    return remaining_days

def apply_cdf_to_result_dict(result_dict, shape, loc, scale):
    """
    Applies the Cumulative Distribution Function (CDF) of the gamma distribution to a dictionary of results. 
    The function calculates the CDF values of the gamma distribution for the given shape, loc, and scale parameters 
    for each key in the input dictionary. The dictionary values are assumed to be lists of days, representing time 
    intervals. The function calculates the cumulative probability for each interval and stores the results in a new 
    dictionary.

    Dependencies:
    - scipy.stats.gamma for calculating the CDF of the gamma distribution.

    Parameters:
    - result_dict (dict): Dictionary with keys representing identifiers and values as lists of days (intervals).
    - shape (float): The shape parameter of the gamma distribution.
    - loc (float): The location parameter of the gamma distribution.
    - scale (float): The scale parameter of the gamma distribution.

    Returns:
    - dict: A dictionary with the same keys as 'result_dict'. Each key is associated with a list of CDF values 
      corresponding to the intervals specified in the input dictionary.

    Example:
    # >>> result_dict = {'Campaign1': [10, 20, 30], 'Campaign2': [15, 25, 35]}
    # >>> shape, loc, scale = 2.0, 0.0, 1.0
    # >>> cdf_result = apply_cdf_to_result_dict(result_dict, shape, loc, scale)
    # >>> cdf_result
    {'Campaign1': [CDF values], 'Campaign2': [CDF values]}

    Operations:
    - Iterates over each key-value pair in the input dictionary.
    - For each list of days (value), computes the CDF value for the cumulative time up to that day and subtracts 
      the CDF value of the previous cumulative time.
    - Stores the computed CDF values in a new dictionary with the same keys.
    """
    cdf_values = {}
    for key, value in result_dict.items():
        cdf_val = []
        prev_day = 0
        for days in value:
            cdf_val.append(stats.gamma.cdf(prev_day + days, shape, loc, scale) - stats.gamma.cdf(prev_day, shape, loc, scale))
            prev_day += days
        cdf_values[key] = cdf_val
    return cdf_values

def create_result_dict(MailDateDict):

    """
    Creates a dictionary that maps campaign codes to lists of the number of days remaining in the current month 
    and in the subsequent eleven months. The function parses each date string in the input dictionary and calculates 
    the days remaining using the 'days_in_subsequent_months' function.

    Dependencies:
    - 'dateutil.parser.parse' for parsing date strings.
    - 'days_in_subsequent_months' function to calculate the number of days in subsequent months.

    Parameters:
    - MailDateDict (dict): Dictionary with campaign codes as keys and date strings as values.

    Returns:
    - dict: A dictionary with campaign codes as keys. Each key is associated with a list of integers where the 
      first element is the number of days remaining in the month of the input date and the subsequent elements 
      are the total days in each of the next eleven months.

    Example:
    # >>> MailDateDict = {'Campaign1': '2021-01-15', 'Campaign2': '2021-02-20'}
    # >>> result_dict = create_result_dict(MailDateDict)
    # >>> result_dict
    {'Campaign1': [16, 28, 31, 30, ...], 'Campaign2': [8, 31, 30, 31, ...]}

    Operations:
    - Iterates over each key-value pair in the input dictionary.
    - Parses each date string to a datetime object.
    - Calculates the days remaining in the current month and in the subsequent eleven months for each parsed date.
    - Stores the calculated days in a new dictionary with the same keys.
    """

    result_dict = {}
    for campaign, date_str in MailDateDict.items():
        try:
            # Parse the date string
            input_date = parser.parse(date_str)
        except ValueError:
            # Handle the case where the date string cannot be parsed
            print(f"Unable to parse date string for campaign {campaign}: {date_str}")
            continue  # Skip this campaign

        days_remaining = days_in_subsequent_months(input_date)
        result_dict[campaign] = days_remaining

    return result_dict

def calculate_dollar_values(campaigns, campaign_budgets, cdf_values):

    """
    Calculates the dollar values for campaigns based on their budgets and CDF values from a gamma distribution.

    This function multiplies the CDF values of each campaign by its corresponding budget to calculate the expected 
    dollar values. It processes a list of campaign identifiers, their associated budgets, and a dictionary of CDF 
    values for each campaign. The function handles exceptions and prints relevant information for each campaign 
    during processing.

    Dependencies:
    - Basic Python operations. No external dependencies.

    Parameters:
    - campaigns (list): A list of campaign identifiers (strings).
    - campaign_budgets (dict): A dictionary with campaign identifiers as keys and their respective budgets as values.
    - cdf_values (dict): A dictionary with campaign identifiers as keys and lists of CDF values as values.

    Returns:
    - dict: A dictionary with campaign identifiers as keys and lists of calculated dollar values as values. If an 
      error occurs, it prints the error message and returns the dictionary with processed data up to the point of the error.

    Example:
    # >>> campaigns = ['Campaign1', 'Campaign2']
    # >>> campaign_budgets = {'Campaign1': 1000, 'Campaign2': 2000}
    # >>> cdf_values = {'Campaign1': [0.1, 0.2, 0.3], 'Campaign2': [0.2, 0.4, 0.6]}
    # >>> dollar_values = calculate_dollar_values(campaigns, campaign_budgets, cdf_values)
    # >>> dollar_values
    {'Campaign1': [100.0, 200.0, 300.0], 'Campaign2': [400.0, 800.0, 1200.0]}

    Operations:
    - Iterates over each campaign in the campaigns list.
    - Retrieves the CDF values and budget for the current campaign.
    - Calculates the dollar value for each interval by multiplying the CDF value by the campaign's budget.
    - Stores the calculated dollar values in a new dictionary.
    - Handles any exceptions that occur during the process and prints error messages.
    """

    dollar_values_with_budgets = {}
    try:
        for i, campaign in enumerate(campaigns):
            print(f"Processing campaign: {campaign}")
            print(f"CDF Values: {cdf_values[campaign]}")
            print(f"Campaign Budget: {campaign_budgets[campaign]}")

            campaign_dollar_values = [float(val) * float(campaign_budgets[campaign]) for val in cdf_values[campaign]]
            dollar_values_with_budgets[campaign] = campaign_dollar_values

            print(f"Dollar Values: {campaign_dollar_values}")
    except Exception as e:
        print(f"Error: {e}")

    return dollar_values_with_budgets

def merge_dates_with_values(dates_dict, values_dict):

    """
    Merges two dictionaries: one with dates and another with values, based on common keys.

    This function creates a new dictionary that combines the dates and values associated with the same keys from two 
    input dictionaries. For each key in the values dictionary, it checks if the key exists in the dates dictionary. 
    If so, it creates a new entry in the merged dictionary with the key, the corresponding date from the dates 
    dictionary, and the list of values from the values dictionary.

    Dependencies:
    - Basic Python operations. No external dependencies.

    Parameters:
    - dates_dict (dict): Dictionary with keys representing identifiers and values as dates.
    - values_dict (dict): Dictionary with the same identifiers as keys and lists of values associated with these 
      identifiers.

    Returns:
    - dict: A dictionary where each key corresponds to an identifier found in both input dictionaries. Each key is 
      associated with a list containing the date from the dates dictionary followed by the list of values from the 
      values dictionary.

    Example:
    # >>> dates_dict = {'Campaign1': '2021-01-01', 'Campaign2': '2021-02-01'}
    # >>> values_dict = {'Campaign1': [100, 200, 300], 'Campaign2': [400, 500, 600]}
    # >>> merged_dict = merge_dates_with_values(dates_dict, values_dict)
    # >>> merged_dict
    {'Campaign1': ['2021-01-01', [100, 200, 300]], 'Campaign2': ['2021-02-01', [400, 500, 600]]}

    Operations:
    - Iterates over each key-value pair in the values dictionary.
    - Checks if the key is present in the dates dictionary.
    - If the key is found, creates a new entry in the merged dictionary with the date and the values associated with 
      the key.
    - Continues this process for all keys in the values dictionary.

    """

    merged_dict = {}
    for key, values in values_dict.items():
        if key in dates_dict:
            merged_dict[key] = [dates_dict[key]] + [values]
    return merged_dict

def create_MailDateDict(dataframe, key_column, value_column):
    """
    Creates a dictionary from two columns of a DataFrame.

    This function takes a DataFrame and two column names as inputs and creates a dictionary where the keys are values 
    from the specified key column and the values are from the specified value column. It's particularly useful for 
    quickly converting DataFrame columns to a dictionary format for easier data manipulation or lookup.

    Dependencies:
    - Pandas library for DataFrame manipulation.
    - Python's built-in 'zip' function for pairing elements.

    Parameters:
    - dataframe (DataFrame): A pandas DataFrame from which the data is extracted.
    - key_column (str): The name of the column in the DataFrame to be used as keys in the dictionary.
    - value_column (str): The name of the column in the DataFrame to be used as values in the dictionary.

    Returns:
    - dict: A dictionary with keys and values corresponding to the specified columns in the DataFrame.

    Example:
    # >>> df = pd.DataFrame({'CampaignCode': ['C1', 'C2'], 'MailDate': ['2021-01-01', '2021-02-01']})
    # >>> MailDateDict = create_MailDateDict(df, 'CampaignCode', 'MailDate')
    # >>> MailDateDict
    {'C1': '2021-01-01', 'C2': '2021-02-01'}

    Operations:
    - Extracts the specified key and value columns from the DataFrame.
    - Uses the 'zip' function to pair elements from the key and value columns.
    - Converts the zipped pairs into a dictionary.
    """

    MailDateDict = dict(zip(dataframe[key_column], dataframe[value_column]))
    return MailDateDict

def create_campaign_budgets(dataframe, key_column, value_column):

    """
    Creates a dictionary of campaign budgets from two columns of a DataFrame.

    This function generates a dictionary where the keys are campaign identifiers and the values are their corresponding 
    budgets. It takes a DataFrame and two column names as inputs, with one column representing the campaign identifiers 
    and the other representing the budgets. The function is useful for converting DataFrame columns into a dictionary 
    format for convenient access and manipulation of campaign budget data.

    Dependencies:
    - Pandas library for DataFrame manipulation.
    - Python's built-in 'zip' function for pairing elements.

    Parameters:
    - dataframe (DataFrame): A pandas DataFrame from which the data is extracted.
    - key_column (str): The name of the column in the DataFrame to be used as keys in the dictionary, representing 
      campaign identifiers.
    - value_column (str): The name of the column in the DataFrame to be used as values in the dictionary, representing 
      campaign budgets.

    Returns:
    - dict: A dictionary where each key is a campaign identifier and each value is the corresponding budget for that 
      campaign.

    Example:
    # >>> df = pd.DataFrame({'CampaignCode': ['C1', 'C2'], 'Budget': [1000, 2000]})
    # >>> campaign_budgets = create_campaign_budgets(df, 'CampaignCode', 'Budget')
    # >>> campaign_budgets
    {'C1': 1000, 'C2': 2000}

    Operations:
    - Extracts the specified key and value columns from the DataFrame, corresponding to campaign identifiers and budgets.
    - Pairs elements from the key and value columns using the 'zip' function.
    - Converts these paired elements into a dictionary.
    """

    campaign_budgets = dict(zip(dataframe[key_column], dataframe[value_column]))
    return campaign_budgets

def populate_dataframe(mail_date_dict, months):

    """
     Populates a DataFrame with data based on a mail date dictionary and a list of months.

    This function creates a DataFrame where each column represents a different campaign, and each row represents a month. 
    The values in the DataFrame are populated based on the mail date dictionary, which contains campaign start dates and 
    values associated with each campaign. The function formats the months, initializes the DataFrame, populates it with 
    campaign data, and calculates monthly totals.

    Dependencies:
    - Pandas library for DataFrame manipulation.
    - Python's datetime module for handling date formatting.

    Parameters:
    - mail_date_dict (dict): Dictionary with campaign names as keys and a list containing the start date and associated 
      values for each campaign as values.
    - months (list[datetime]): List of datetime objects representing the months for which the DataFrame is to be populated.

    Returns:
    - DataFrame: A pandas DataFrame with rows representing months and columns representing campaigns. Each cell contains 
      the value associated with a campaign for a given month, and there is an additional column for the total values of all 
      campaigns for each month.

    Example:
    # >>> mail_date_dict = {'Campaign1': ['2021-01-01', 100, 200], 'Campaign2': ['2021-02-01', 150, 250]}
    # >>> months = [datetime(2021, 1, 1), datetime(2021, 2, 1)]
    # >>> df = populate_dataframe(mail_date_dict, months)
    # >>> df
    [DataFrame showing campaigns and their values for each month]

    Operations:
    - Formats the 'months' list into strings representing 'Month Year'.
    - Initializes a DataFrame with columns for each campaign, a 'Month' column, and a 'Monthly Total' column.
    - Populates the DataFrame with zeros and then with values from the mail date dictionary.
    - Calculates the total for each month across all campaigns.
    """

    # Convert the datetime objects in 'months' to strings in 'Month Year' format
    formatted_months = [month.strftime('%B %Y') for month in months]

    # Initialize DataFrame with formatted 'Month' column
    df = pd.DataFrame(columns=["Month"] + list(mail_date_dict.keys()) + ["Monthly Total"])
    df['Month'] = formatted_months

    # Initialize the rest of the DataFrame
    for key in mail_date_dict:
        df[key] = 0
    df["Monthly Total"] = 0

    for key, value in mail_date_dict.items():
        # Ensure the date is a datetime object and then format it
        campaign_start_date = value[0] if isinstance(value[0], datetime) else parse_date(value[0])
        campaign_start_date_str = campaign_start_date.strftime('%B %Y')

        if campaign_start_date_str in df['Month'].values:
            start_index = df.index[df['Month'] == campaign_start_date_str][0]
            for i, val in enumerate(value[1]):
                if start_index + i < len(df):
                    df.loc[start_index + i, key] = val

    # Calculate the monthly total
    df["Monthly Total"] = df.iloc[:, 1:-1].sum(axis=1)
    return df

def get_sharepoint_context(site_url, username, password):

    """
    Establishes a context for interacting with a SharePoint site using provided credentials.

    This function attempts to authenticate a user to a SharePoint site using a username and password. If authentication 
    is successful, it creates and returns a context object that can be used for subsequent interactions with the SharePoint 
    site. If authentication fails, the function raises an exception.

    Dependencies:
    - Office365-REST-Python-Client library for SharePoint site interaction and authentication.

    Parameters:
    - site_url (str): A string representing the URL of the SharePoint site to connect to.
    - username (str): A string representing the username for authentication.
    - password (str): A string representing the password for authentication.

    Returns:
    - ClientContext: A ClientContext object that represents the SharePoint site context, allowing for further interactions 
      with the site.

    Exceptions:
    - Raises an Exception if authentication fails.

    Example:
    # >>> site_url = 'https://example.sharepoint.com'
    # >>> username = 'user@example.com'
    # >>> password = 'password'
    # >>> ctx = get_sharepoint_context(site_url, username, password)
    # [ClientContext object is returned for successful authentication]

    Operations:
    - Initializes an AuthenticationContext with the provided SharePoint site URL.
    - Attempts to acquire a token for the user using the provided username and password.
    - If authentication is successful, creates a ClientContext for the SharePoint site.
    - If authentication fails, raises an exception indicating that authentication failed.
    """

    ctx_auth = AuthenticationContext(site_url)
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, ctx_auth)
        return ctx
    else:
        raise Exception("Authentication failed")

def transfer_from_cluster(client_context, relative_url, file_name):

    """
    Transfers a file from a cluster to a specified location in a SharePoint library.

    This function uploads a file from a cluster's temporary storage to a SharePoint library at a specified relative URL. 
    It uses the ClientContext object to interact with SharePoint, creates a file creation information object, reads the 
    file content, and then uploads the file to the library. If the operation is successful, it returns a confirmation 
    message. In case of an exception, the exception is raised for further handling.

    Dependencies:
    - Office365-REST-Python-Client library for SharePoint site interaction.
    - Python's os.path and built-in file handling for reading the file content.

    Parameters:
    - client_context (ClientContext): A ClientContext object for interacting with SharePoint.
    - relative_url (str): A string representing the server-relative URL of the folder in the SharePoint library where 
      the file is to be uploaded.
    - file_name (str): A string representing the name of the file to be transferred.

    Returns:
    - str: A success message string confirming the transfer of the file to the specified SharePoint library location.

    Exceptions:
    - Raises an exception if an error occurs during the file transfer process.

    Example:
    # >>> client_context = get_sharepoint_context('https://example.sharepoint.com', 'user@example.com', 'password')
    # >>> relative_url = '/Shared Documents/folder'
    # >>> file_name = 'example.txt'
    # >>> message = transfer_from_cluster(client_context, relative_url, file_name)
    # >>> message
    'Transferred example.txt to /Shared Documents/folder'

    Operations:
    - Retrieves the folder in the SharePoint library corresponding to the given relative URL.
    - Initializes a FileCreationInformation object and reads the file content from the cluster's temporary storage.
    - Sets the URL and overwrite properties for the file creation information.
    - Uploads the file to the SharePoint library.
    - Executes the client context query to complete the upload.
    - Returns a success message upon successful transfer.
    - Raises an exception if any errors occur during the process.
    """

    try:
        libraryRoot = client_context.web.get_folder_by_server_relative_url(relative_url)
        info = FileCreationInformation()
        with open(os.path.join('/dbfs/tmp', file_name), 'rb') as content_file:
            info.content = content_file.read()
        info.url = file_name
        info.overwrite = True
        upload_file = libraryRoot.files.add(info)
        client_context.execute_query()
        return 'Transferred %s to %s' % (file_name, relative_url)
    except Exception as e:
        raise e


def is_new_structure_format(sample_campaign_info):
    # Count the number of instances with more than one pair of brackets
    multiple_brackets_count = sum(1 for item in sample_campaign_info if isinstance(item, (list, tuple)) and len(item) == 2)
    # If any campaign type has more than one pair of brackets, consider it the new structure
    return multiple_brackets_count > 0

def _extract_appeals_acquisitions(df, mapper):
    # Assuming 'CampaignTypes' contains lists of patterns for each campaign type
    # We check the first item in 'CampaignTypes' to determine the structure
    sample_campaign_info = next(iter(mapper['CampaignTypes'].values()))
    is_new_structure = is_new_structure_format(sample_campaign_info)

    # Debugging print statements
    print(f"Sample Campaign Info: {sample_campaign_info}")
    print(f"Detected Structure: {'New' if is_new_structure else 'Old'}")

    # Use the appropriate function based on the detected structure
    if is_new_structure:
        return _extract_appeals_acquisitions_new(df, mapper)
    else:
        return _extract_appeals_acquisitions_old(df, mapper)

def _extract_appeals_acquisitions_new(df, mapper):
    print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line

    d = {}
    extraction_field = mapper.get('ExtractionField', 'CampaignCode')  # Default to 'CampaignCode' if not specified
    n = mapper['YearPosition']

    if mapper['Method'] == 'starts_ends':
        _mapper = mapper['CampaignTypes']
        print(f"Using 'starts_ends' method with YearPosition at {n} and ExtractionField {extraction_field}")  # Debugging line
        
        for ct in _mapper.keys():
            print(f"Processing CampaignType: {ct}")  # Debugging line
            campaign_info = _mapper.get(ct)  # Safely get campaign info

            if not campaign_info:
                print(f"Skipping CampaignType {ct} as no data found.")  # Skip if no data is found for the campaign type
                continue

            # Determine if we're dealing with the new structure format
            if isinstance(campaign_info[0][0], list) or extraction_field == 'CampaignName':  # Adjust for 'CampaignName' extraction
                campaign_data = campaign_info if extraction_field == 'CampaignName' else [campaign_info]

                masks = []  # To store all masks and then combine them
                for campaign_patterns in campaign_data:
                    # Adjust the loop to handle more complex nesting
                    for pattern_group in campaign_patterns:
                        # Check if it's a tuple with two parts (start_patterns, end_patterns)
                        if isinstance(pattern_group, tuple) and len(pattern_group) == 2:
                            start_patterns, end_patterns = pattern_group
                        else:
                            # Handle cases where there's a different structure
                            start_patterns, end_patterns = pattern_group, []

                        # Now proceed with processing each start pattern
                        for start_pattern in start_patterns:
                            m1 = df['CampaignCode'].str.startswith(start_pattern)
                            if end_patterns:
                                end_masks = [df['CampaignCode'].str.endswith(ep) for ep in end_patterns]
                                m_end_combined = pd.Series([False] * len(df))
                                for em in end_masks:
                                    m_end_combined |= em
                                masks.append(m1 & m_end_combined)
                            else:
                                masks.append(m1)

                # Combine all masks for this campaign type with AND for year pattern
                if masks and extraction_field != 'CampaignName':  # Only for 'CampaignCode' processing
                    final_mask = masks[0]
                    for m in masks[1:]:
                        final_mask |= m
                    year_pattern_regex = '|'.join(_mapper[ct][-1])  # Assuming year data is the last element
                    m_year = df['CampaignCode'].str[n:n+2].str.contains(year_pattern_regex)
                    final_mask &= m_year
                    d[ct] = df.loc[final_mask, 'CampaignCode'].unique()

            # Only print and access `d[ct]` if `ct` was added to `d`
            if ct in d:
                print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
                if len(d[ct]) == 0:
                    print(f"No campaigns found for {ct}.")  # Debugging line

    elif mapper['Method'] == 'contains' and extraction_field == 'CampaignCode':
        _mapper = mapper['CampaignTypes']
        print("Using 'contains' method")  # Debugging line
        for ct in _mapper.keys():
            print(f"Processing CampaignType: {ct}")  # Debugging line
            campaign_info = _mapper.get(ct)
            
            if not campaign_info:
                print(f"Skipping CampaignType {ct} as no data found.")  # Skip if no data is found for the campaign type
                continue

            mask = df.CampaignCode.str.contains(campaign_info)
            d[ct] = df.loc[mask, 'CampaignCode'].unique()
            if ct in d:
                print(f"Pattern: {campaign_info}")  # Debugging line
                print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
                if len(d[ct]) == 0:
                    print(f"No campaigns found for {ct}.")  # Debugging line

    print("Final campaign mapping:", d)  # Debugging line
    return d



def _extract_appeals_acquisitions_old(df, mapper):
    print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line

    d = {}
    extraction_field = mapper.get('ExtractionField', 'CampaignCode')  # Default to 'CampaignCode' if not specified
    if mapper['Method'] == 'starts_ends':
        _mapper = mapper['CampaignTypes']
        n = mapper['YearPosition']
        print(f"Using 'starts_ends' method with YearPosition at {n} and ExtractionField {extraction_field}")  # Debugging line
        for ct in _mapper.keys():
            print(f"Processing CampaignType: {ct}")  # Debugging line
            campaign_info = _mapper[ct]
            start_patterns, year_patterns = campaign_info if len(campaign_info) == 2 else (campaign_info, [])
            
            if not year_patterns:  # This means we only got one element which should be year patterns
                year_patterns = start_patterns
                start_patterns = []  # Reset start_patterns if we assumed wrong
            else:
                # Ensure start_patterns is a list of strings or tuples, not a string itself
                start_patterns = [start_patterns] if isinstance(start_patterns, str) else start_patterns

            year_pattern_regex = '|'.join(year_patterns)
            masks = []

            # Adaptation for different extraction fields
            if extraction_field == 'CampaignCode':
                # Original logic for handling CampaignCode
                for start_end_group in start_patterns:
                    if isinstance(start_end_group, tuple):  # Complex patterns
                        for start_pattern, end_patterns in start_end_group:
                            m_start = df['CampaignCode'].str.startswith(start_pattern)
                            m_end = df['CampaignCode'].str.endswith(tuple(end_patterns)) if end_patterns else pd.Series([True] * len(df))
                            masks.append(m_start & m_end)
                    else:  # Simple patterns
                        m_start = df['CampaignCode'].str.startswith(start_end_group)
                        masks.append(m_start)
            elif extraction_field == 'CampaignName':
                # New logic for handling CampaignName
                mask = df['CampaignName'].str.contains('|'.join(start_patterns + year_patterns), case=False)
                masks.append(mask)

            # Apply year filtering based on YearPosition if using CampaignCode
            if extraction_field == 'CampaignCode':
                m_year = df['CampaignCode'].str[n:n+2].str.contains(year_pattern_regex)
                final_mask = pd.Series([False] * len(df))  # Initialize with all False
                for mask in masks:
                    final_mask |= mask & m_year
            else:
                final_mask = masks[0] if masks else pd.Series([False] * len(df))

            d[ct] = df.loc[final_mask, 'CampaignCode'].unique()  # Note: Adjust if you need to return different column based on ExtractionField
            print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
            if len(d[ct]) == 0:
                print(f"No campaigns found for {ct}.")  # Debugging line

    elif mapper['Method'] == 'contains':
        _mapper = mapper['CampaignTypes']
        print("Using 'contains' method and ExtractionField {extraction_field}")  # Debugging line
        for ct in _mapper.keys():
            print(f"Processing CampaignType: {ct}")  # Debugging line
            # Adapting contains method for different extraction fields
            mask = df[extraction_field].str.contains(_mapper[ct], case=False)
            d[ct] = df.loc[mask, 'CampaignCode'].unique()  # Note: Adjust if you need to return different column based on ExtractionField
            print(f"Pattern: {_mapper[ct]}")  # Debugging line
            print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
            if len(d[ct]) == 0:
                print(f"No campaigns found for {ct}.")  # Debugging line

    print("Final campaign mapping:", d)  # Debugging line
    return d









# def is_new_structure_format(sample_campaign_info):
#     # Count the number of instances with more than one pair of brackets
#     multiple_brackets_count = sum(1 for item in sample_campaign_info if isinstance(item, (list, tuple)) and len(item) == 2)
#     # If any campaign type has more than one pair of brackets, consider it the new structure
#     return multiple_brackets_count > 0

# def _extract_appeals_acquisitions(df, mapper):
#     # Assuming 'CampaignTypes' contains lists of patterns for each campaign type
#     # We check the first item in 'CampaignTypes' to determine the structure
#     sample_campaign_info = next(iter(mapper['CampaignTypes'].values()))
#     is_new_structure = is_new_structure_format(sample_campaign_info)

#     # Debugging print statements
#     print(f"Sample Campaign Info: {sample_campaign_info}")
#     print(f"Detected Structure: {'New' if is_new_structure else 'Old'}")

#     # Use the appropriate function based on the detected structure
#     if is_new_structure:
#         return _extract_appeals_acquisitions_new(df, mapper)
#     else:
#         return _extract_appeals_acquisitions_old(df, mapper)

# def _extract_appeals_acquisitions_old(df, mapper): #works normally
#     print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line

#     d = {}
#     if mapper['Method'] == 'starts_ends':
#         _mapper = mapper['CampaignTypes']
#         n = mapper['YearPosition']
#         print(f"Using 'starts_ends' method with YearPosition at {n}")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             campaign_info = _mapper[ct]
#             start_patterns, year_patterns = campaign_info if len(campaign_info) == 2 else (campaign_info, [])
            
#             if not year_patterns:  # This means we only got one element which should be year patterns
#                 year_patterns = start_patterns
#                 start_patterns = []  # Reset start_patterns if we assumed wrong
#             else:
#                 # Ensure start_patterns is a list of strings or tuples, not a string itself
#                 start_patterns = [start_patterns] if isinstance(start_patterns, str) else start_patterns

#             year_pattern_regex = '|'.join(year_patterns)
#             masks = []

#             if start_patterns and isinstance(start_patterns[0], tuple):  # Complex patterns
#                 for start_end_group in start_patterns:
#                     for start_pattern, end_patterns in start_end_group:
#                         m_start = df.CampaignCode.str.startswith(start_pattern)
#                         m_end = df.CampaignCode.str.endswith(tuple(end_patterns)) if end_patterns else pd.Series([True] * len(df))
#                         masks.append(m_start & m_end)
#             else:  # Simple patterns
#                 for start_pattern in start_patterns:
#                     m_start = df.CampaignCode.str.startswith(start_pattern)
#                     masks.append(m_start)

#             m_year = df.CampaignCode.str[n:n+2].str.contains(year_pattern_regex)
#             final_mask = pd.Series([False] * len(df))  # Initialize with all False
#             for mask in masks:
#                 final_mask |= mask & m_year

#             d[ct] = df.loc[final_mask, 'CampaignCode'].unique()
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line

#     elif mapper['Method'] == 'contains':
#         _mapper = mapper['CampaignTypes']
#         print("Using 'contains' method")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             mask = df.CampaignCode.str.contains(_mapper[ct])
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Pattern: {_mapper[ct]}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line

#     print("Final campaign mapping:", d)  # Debugging line
#     return d

# def _extract_appeals_acquisitions_new(df, mapper):
#     print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line

#     d = {}
#     if mapper['Method'] == 'starts_ends':
#         _mapper = mapper['CampaignTypes']
#         n = mapper['YearPosition']
#         print(f"Using 'starts_ends' method with YearPosition at {n}")  # Debugging line
#         for ct in _mapper.keys():
#             campaign_info = _mapper[ct]
#             # Check if the first element of the first item is a list (new structure)
#             if isinstance(campaign_info[0][0], list):  # New case: list of lists
#                 campaign_data = campaign_info  # Here, campaign_data is directly the list of lists
#             else:  # Regular case: list of tuples
#                 campaign_data = [campaign_info]  # Wrap the old structure in a list to standardize processing

#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             masks = []  # To store all masks and then combine them

#             for campaign_patterns in campaign_data:
#                 for start_patterns, end_patterns in campaign_patterns:
#                     for start_pattern in start_patterns:
#                         m1 = df.CampaignCode.str.startswith(start_pattern)
#                         # Handle different end pattern scenarios
#                         if end_patterns:  # Non-empty end_patterns
#                             end_masks = [df.CampaignCode.str.endswith(ep) for ep in end_patterns]
#                             m_end_combined = pd.Series([False] * len(df))  # Initialize with all False
#                             for em in end_masks:
#                                 m_end_combined |= em  # Combine all end pattern masks with OR
#                             masks.append(m1 & m_end_combined)
#                         else:  # Empty end_patterns means any ending is acceptable
#                             masks.append(m1)

#             # Assuming year data is consistent across scenarios
#             # Adjust if your year data handling needs to be different
#             year_data = _mapper[ct][-1]  # Assuming year data is the last element
#             year_pattern_regex = '|'.join(year_data)
#             m_year = df.CampaignCode.str[n:n+2].str.contains(year_pattern_regex)

#             # Combine all masks for this campaign type with AND for year pattern
#             if masks:  # Check if we have any masks to avoid error in combining empty list
#                 final_mask = masks[0] & m_year
#                 for m in masks[1:]:
#                     final_mask |= m & m_year
#                 d[ct] = df.loc[final_mask, 'CampaignCode'].unique()
#                 print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#                 if len(d[ct]) == 0:
#                     print(f"No campaigns found for {ct}.")  # Debugging line
#             else:
#                 print(f"No valid start-end combinations provided for {ct}.")  # Debugging line

#     elif mapper['Method'] == 'contains':
#         # This part remains unchanged, as the new structure does not affect it
#         _mapper = mapper['CampaignTypes']
#         print("Using 'contains' method")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             mask = df.CampaignCode.str.contains(_mapper[ct])
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Pattern: {_mapper[ct]}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line

#     print("Final campaign mapping:", d)  # Debugging line
#     return d





# def _extract_appeals_acquisitions(df, mapper): #this works for choa issue but not normally
#     print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line

#     d = {}
#     if mapper['Method'] == 'starts_ends':
#         _mapper = mapper['CampaignTypes']
#         n = mapper['YearPosition']
#         print(f"Using 'starts_ends' method with YearPosition at {n}")  # Debugging line
#         for ct in _mapper.keys():
#             campaign_info = _mapper[ct]
#             # Check if the first element of the first item is a list (new structure)
#             if isinstance(campaign_info[0][0], list):  # New case: list of lists
#                 campaign_data = campaign_info  # Here, campaign_data is directly the list of lists
#             else:  # Regular case: list of tuples
#                 campaign_data = [campaign_info]  # Wrap the old structure in a list to standardize processing

#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             masks = []  # To store all masks and then combine them

#             for campaign_patterns in campaign_data:
#                 for start_patterns, end_patterns in campaign_patterns:
#                     for start_pattern in start_patterns:
#                         m1 = df.CampaignCode.str.startswith(start_pattern)
#                         # Handle different end pattern scenarios
#                         if end_patterns:  # Non-empty end_patterns
#                             end_masks = [df.CampaignCode.str.endswith(ep) for ep in end_patterns]
#                             m_end_combined = pd.Series([False] * len(df))  # Initialize with all False
#                             for em in end_masks:
#                                 m_end_combined |= em  # Combine all end pattern masks with OR
#                             masks.append(m1 & m_end_combined)
#                         else:  # Empty end_patterns means any ending is acceptable
#                             masks.append(m1)

#             # Assuming year data is consistent across scenarios
#             # Adjust if your year data handling needs to be different
#             year_data = _mapper[ct][-1]  # Assuming year data is the last element
#             year_pattern_regex = '|'.join(year_data)
#             m_year = df.CampaignCode.str[n:n+2].str.contains(year_pattern_regex)

#             # Combine all masks for this campaign type with AND for year pattern
#             if masks:  # Check if we have any masks to avoid error in combining empty list
#                 final_mask = masks[0] & m_year
#                 for m in masks[1:]:
#                     final_mask |= m & m_year
#                 d[ct] = df.loc[final_mask, 'CampaignCode'].unique()
#                 print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#                 if len(d[ct]) == 0:
#                     print(f"No campaigns found for {ct}.")  # Debugging line
#             else:
#                 print(f"No valid start-end combinations provided for {ct}.")  # Debugging line

#     elif mapper['Method'] == 'contains':
#         # This part remains unchanged, as the new structure does not affect it
#         _mapper = mapper['CampaignTypes']
#         print("Using 'contains' method")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             mask = df.CampaignCode.str.contains(_mapper[ct])
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Pattern: {_mapper[ct]}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line

#     print("Final campaign mapping:", d)  # Debugging line
#     return d



# def _extract_appeals_acquisitions(df, mapper):
#     print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line to show keys

#     d = {}
#     if mapper['Method'] == 'starts_ends':
#         _mapper = mapper['CampaignTypes']
#         n = mapper['YearPosition']
#         print(f"Using 'starts_ends' method with YearPosition at {n}")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             m1 = (df.CampaignCode.str.startswith(_mapper[ct][0]))
#             end_patterns = _mapper[ct][-1]
#             if isinstance(end_patterns, tuple):
#                 end_pattern_regex = '|'.join(end_patterns)
#             else:
#                 end_pattern_regex = end_patterns
#             m2 = df.CampaignCode.str[n: n+2].str.contains(end_pattern_regex)
#             mask = m1 & m2
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Start pattern: {_mapper[ct][0]}, End pattern regex: {end_pattern_regex}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line
#     elif mapper['Method'] == 'contains':
#         _mapper = mapper['CampaignTypes']
#         print("Using 'contains' method")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             mask = (df.CampaignCode.str.contains(_mapper[ct]))
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Pattern: {_mapper[ct]}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line

#     print("Final campaign mapping:", d)  # Debugging line
#     return d
# import pandas as pd

# def _extract_appeals_acquisitions(df, mapper):
#     print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line

#     d = {}
#     if mapper['Method'] == 'starts_ends':
#         _mapper = mapper['CampaignTypes']
#         n = mapper['YearPosition']
#         print(f"Using 'starts_ends' method with YearPosition at {n}")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             campaign_data = _mapper[ct]
#             all_masks = []

#             # Determine if dealing with complex or simple structure based on the first element
#             if isinstance(campaign_data[0], tuple):
#                 # Complex structure, we expect a list of tuples (start_patterns, end_patterns)
#                 for start_end_group in campaign_data:
#                     start_patterns, end_patterns = start_end_group if len(start_end_group) == 2 else (start_end_group, [''])
#                     for start_pattern in start_patterns:
#                         m_start = df.CampaignCode.str.startswith(start_pattern)
#                         if end_patterns:  # Non-empty end_patterns
#                             m_end = df.CampaignCode.str.endswith(tuple(end_patterns))
#                         else:  # Empty end_patterns
#                             m_end = pd.Series([True] * len(df))
#                         all_masks.append(m_start & m_end)
#             else:
#                 # Simple structure, directly use campaign_data as year_patterns
#                 year_patterns = campaign_data
#                 # Apply each start pattern with year pattern filtering
#                 for start_pattern in year_patterns:
#                     m_year = df.CampaignCode.str[n:n+2].str.contains(start_pattern)
#                     all_masks.append(m_year)

#             # Combine all masks
#             final_mask = pd.Series([False] * len(df))
#             for mask in all_masks:
#                 final_mask |= mask

#             d[ct] = df.loc[final_mask, 'CampaignCode'].unique()
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line

#     elif mapper['Method'] == 'contains':
#         _mapper = mapper['CampaignTypes']
#         print("Using 'contains' method")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             mask = df.CampaignCode.str.contains(_mapper[ct])
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Pattern: {_mapper[ct]}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line

#     print("Final campaign mapping:", d)  # Debugging line
#     return d


# def _extract_appeals_acquisitions(df, mapper): #works normally
#     print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line

#     d = {}
#     if mapper['Method'] == 'starts_ends':
#         _mapper = mapper['CampaignTypes']
#         n = mapper['YearPosition']
#         print(f"Using 'starts_ends' method with YearPosition at {n}")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             campaign_info = _mapper[ct]
#             start_patterns, year_patterns = campaign_info if len(campaign_info) == 2 else (campaign_info, [])
            
#             if not year_patterns:  # This means we only got one element which should be year patterns
#                 year_patterns = start_patterns
#                 start_patterns = []  # Reset start_patterns if we assumed wrong
#             else:
#                 # Ensure start_patterns is a list of strings or tuples, not a string itself
#                 start_patterns = [start_patterns] if isinstance(start_patterns, str) else start_patterns

#             year_pattern_regex = '|'.join(year_patterns)
#             masks = []

#             if start_patterns and isinstance(start_patterns[0], tuple):  # Complex patterns
#                 for start_end_group in start_patterns:
#                     for start_pattern, end_patterns in start_end_group:
#                         m_start = df.CampaignCode.str.startswith(start_pattern)
#                         m_end = df.CampaignCode.str.endswith(tuple(end_patterns)) if end_patterns else pd.Series([True] * len(df))
#                         masks.append(m_start & m_end)
#             else:  # Simple patterns
#                 for start_pattern in start_patterns:
#                     m_start = df.CampaignCode.str.startswith(start_pattern)
#                     masks.append(m_start)

#             m_year = df.CampaignCode.str[n:n+2].str.contains(year_pattern_regex)
#             final_mask = pd.Series([False] * len(df))  # Initialize with all False
#             for mask in masks:
#                 final_mask |= mask & m_year

#             d[ct] = df.loc[final_mask, 'CampaignCode'].unique()
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line

#     elif mapper['Method'] == 'contains':
#         _mapper = mapper['CampaignTypes']
#         print("Using 'contains' method")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             mask = df.CampaignCode.str.contains(_mapper[ct])
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Pattern: {_mapper[ct]}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line

#     print("Final campaign mapping:", d)  # Debugging line
#     return d

# def _extract_appeals_acquisitions(df, mapper):
#     print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line

#     d = {}
#     if mapper['Method'] == 'starts_ends':
#         _mapper = mapper['CampaignTypes']
#         n = mapper['YearPosition']
#         print(f"Using 'starts_ends' method with YearPosition at {n}")  # Debugging line
#         for ct in _mapper.keys():
#             campaign_data = _mapper[ct][0]  # Extract the first item for campaign patterns
#             year_data = _mapper[ct][1]  # Extract the second item for year patterns
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             masks = []  # To store all masks and then combine them

#             # Process start and end patterns
#             for start_patterns, end_patterns in campaign_data:
#                 for start_pattern in start_patterns:
#                     m1 = df.CampaignCode.str.startswith(start_pattern)
#                     # Handle different end pattern scenarios
#                     if end_patterns:  # Non-empty end_patterns
#                         end_masks = [df.CampaignCode.str.endswith(ep) for ep in end_patterns]
#                         m_end_combined = pd.Series([False] * len(df))  # Initialize with all False
#                         for em in end_masks:
#                             m_end_combined |= em  # Combine all end pattern masks with OR
#                         masks.append(m1 & m_end_combined)
#                     else:  # Empty end_patterns means any ending is acceptable
#                         masks.append(m1)

#             # Process year patterns
#             year_pattern_regex = '|'.join(year_data)
#             m_year = df.CampaignCode.str[n:n+2].str.contains(year_pattern_regex)

#             # Combine all masks for this campaign type with AND for year pattern
#             if masks:  # Check if we have any masks to avoid error in combining empty list
#                 final_mask = masks[0] & m_year
#                 for m in masks[1:]:
#                     final_mask |= m & m_year
#                 d[ct] = df.loc[final_mask, 'CampaignCode'].unique()
#                 print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#                 if len(d[ct]) == 0:
#                     print(f"No campaigns found for {ct}.")  # Debugging line
#             else:
#                 print(f"No valid start-end combinations provided for {ct}.")  # Debugging line

#     elif mapper['Method'] == 'contains':
#         _mapper = mapper['CampaignTypes']
#         print("Using 'contains' method")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             mask = df.CampaignCode.str.contains(_mapper[ct])
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Pattern: {_mapper[ct]}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line

#     print("Final campaign mapping:", d)  # Debugging line
#     return d


def process_cash_flow_data(CashFlow, shape, loc, scale, months, client):

    """
    Processes cash flow data for campaigns based on either budget or gifts, depending on the client.

    This function takes a DataFrame containing campaign cash flow data and performs a series of operations to calculate 
    and organize this data into a readable format. It works with two versions of the data: one based on campaign budgets 
    and another based on campaign gifts, if applicable. The function applies a Cumulative Distribution Function (CDF) 
    based on provided gamma distribution parameters and calculates dollar values for each campaign. It then populates 
    a DataFrame for each version (budget and gifts) with this information.

    Dependencies:
    - Pandas library for DataFrame manipulation.
    - Helper functions: 'create_MailDateDict', 'create_campaign_budgets', 'apply_cdf_to_result_dict', 
      'calculate_dollar_values', 'merge_dates_with_values', 'populate_dataframe'.

    Parameters:
    - CashFlow (DataFrame): DataFrame with campaign cash flow data, including 'CampaignName', 'MailDate', 'Budget', 
      and 'Gifts'.
    - shape, loc, scale (float): Gamma distribution parameters for applying the CDF.
    - months (list[datetime]): List of datetime objects representing the months for DataFrame population.
    - client (str): String representing the client's name.

    Returns:
    - DataFrame(s): If the client is in 'GiftClients', returns two DataFrames: one for budget data and one for gifts 
      data. If the client is not in 'GiftClients', returns only the budget data DataFrame.

    Example:
    # >>> CashFlow = pd.DataFrame({...})
    # >>> shape, loc, scale = 2.0, 0.0, 1.0
    # >>> months = [datetime(2021, 1, 1), datetime(2021, 2, 1)]
    # >>> client = 'Client1'
    # >>> df_budget, df_gifts = process_cash_flow_data(CashFlow, shape, loc, scale, months, client)
    # [DataFrames populated with budget and gifts data]

    Note:
    - 'GiftClients' is a predefined list of clients for whom gift data processing is required.
    - The function uses several helper functions like 'create_MailDateDict', 'create_campaign_budgets', 
      'apply_cdf_to_result_dict', etc., to process the data.
    """

    MailDateDict = create_MailDateDict(CashFlow, 'CampaignName', 'MailDate')

    # Always perform the budget version
    campaign_budgets = create_campaign_budgets(CashFlow, 'CampaignName', 'Budget')
    result_dict_budget = create_result_dict(MailDateDict)
    cdf_values_budget = apply_cdf_to_result_dict(result_dict_budget, shape, loc, scale)
    campaigns_budget = list(cdf_values_budget.keys())
    dollar_values_with_budgets = calculate_dollar_values(campaigns_budget, campaign_budgets, cdf_values_budget)
    mail_date_dict_budget = merge_dates_with_values(MailDateDict, dollar_values_with_budgets)
    df_budget = populate_dataframe(mail_date_dict_budget, months)

    # Check if the client is in GIFT_CLIENTS and perform the gifts version
    if client in GIFT_CLIENTS:
        campaign_gifts = create_campaign_budgets(CashFlow, 'CampaignName', 'Gifts')
        result_dict_gifts = create_result_dict(MailDateDict)
        cdf_values_gifts = apply_cdf_to_result_dict(result_dict_gifts, shape, loc, scale)
        campaigns_gifts = list(cdf_values_gifts.keys())
        dollar_values_with_gifts = calculate_dollar_values(campaigns_gifts, campaign_gifts, cdf_values_gifts)
        mail_date_dict_gifts = merge_dates_with_values(MailDateDict, dollar_values_with_gifts)
        df_gifts = populate_dataframe(mail_date_dict_gifts, months)
        # Combine df_budget and df_gifts as needed, or return them separately

    # Return the appropriate dataframe(s)
    if client in GIFT_CLIENTS:
        return df_budget, df_gifts
    else:
        return df_budget

def generate_month_list_from_dataframes(cashFlowDict, extend_months=12):

    """
    Generates a list of months as datetime objects based on the dates in a dictionary of DataFrames.

    This function extracts dates from the 'MailDate' column of each DataFrame in the provided dictionary. It then 
    standardizes these dates, determines the earliest and latest dates across all DataFrames, and generates a list 
    of month start dates as datetime objects. The function can extend the list by a specified number of months beyond 
    the latest date found.

    Dependencies:
    - Pandas library for DataFrame manipulation.
    - Dateutil's relativedelta for date arithmetic.
    - Custom 'parse_date' function for standardizing date strings.

    Parameters:
    - cashFlowDict (dict): A dictionary where each key corresponds to a category, and each value is a DataFrame with 
      a 'MailDate' column containing date strings.
    - extend_months (int, optional): An optional integer specifying the number of months to extend beyond the latest 
      date found in the DataFrames (default is 12 months).

    Returns:
    - list[datetime]: A list of datetime objects representing the first day of each month from the earliest date found 
      to the extended latest date.

    Example:
    # >>> cashFlowDict = {'Type1': pd.DataFrame({'MailDate': ['2021-01-15', '2021-02-15']}), 
    #                     'Type2': pd.DataFrame({'MailDate': ['2021-03-15', '2021-04-15']})}
    # >>> month_list = generate_month_list_from_dataframes(cashFlowDict, extend_months=12)
    # >>> month_list
    [datetime(2021, 1, 1), datetime(2021, 2, 1), ..., datetime(2022, 4, 1)]

    Operations:
    - Extracts and standardizes dates from the 'MailDate' column in each DataFrame.
    - Determines the earliest and latest dates across all DataFrames.
    - Extends the latest date by a specified number of months (if applicable).
    - Generates a list of month start dates, from the earliest date to the extended latest date.
    """

    all_dates = []

    # Extract and standardize all dates
    for df in cashFlowDict.values():
        standardized_dates = df['MailDate'].apply(parse_date)
        all_dates.extend(standardized_dates)

    # Find the earliest and latest dates
    earliest_date = min(all_dates)
    latest_date = max(all_dates) + relativedelta(months=extend_months)

    # Generate the list of months as datetime objects
    month_list = []
    current_date = earliest_date.replace(day=1)  # Set to the first day of the month
    while current_date <= latest_date:
        month_list.append(current_date)
        current_date += relativedelta(months=1)

    return month_list

def parse_date(date_str):

    """
    Parses a date string into a datetime object.

    This function attempts to convert a date string into a datetime object. It uses a date parser to handle a variety 
    of date string formats. If the string cannot be parsed into a valid date, the function raises a ValueError with a 
    descriptive message.

    Dependencies:
    - Dateutil's parser module for parsing date strings.

    Parameters:
    - date_str (str): A string representing a date.

    Returns:
    - datetime: A datetime object representing the parsed date.

    Exceptions:
    - Raises a ValueError if the date string cannot be parsed into a valid date, with a message indicating the unparseable 
      date string.

    Example:
    # >>> date_str = '2021-01-15'
    # >>> parsed_date = parse_date(date_str)
    # >>> parsed_date
    datetime.datetime(2021, 1, 15, 0, 0)

    Operations:
    - Attempts to parse the provided date string using a date parser.
    - If successful, returns the parsed datetime object.
    - If the string cannot be parsed, raises a ValueError indicating the issue.
    """

    try:
        # Attempt to parse the date string
        return parser.parse(date_str)
    except ValueError:
        # Raise an error if the string cannot be parsed as a date
        raise ValueError(f"Unable to parse date string: {date_str}")

# COMMAND ----------

# def calculate_cdf_dataframe(paramDict, start_day=0, end_day=340, num_points=341):
#     """
#     This function calculates the Cumulative Distribution Function (CDF) for each campaign type based on gamma distribution parameters provided in 'paramDict'. It generates a DataFrame that lists the percentage of completion (as determined by the CDF) for each campaign type across a specified range of days.

#     Dependencies:
#     - Numpy for generating a range of days using linspace.
#     - Pandas for creating and manipulating the DataFrame.
#     - Scipy.stats for computing the gamma CDF.

#     Parameters:
#     - paramDict (dict): A dictionary where keys are campaign types (e.g., 'ACQ', 'APP') and values are tuples containing the parameters (shape, loc, scale) for the gamma distribution associated with each campaign type.
#     - start_day (int, optional): The starting day number for the CDF calculation. Defaults to 0.
#     - end_day (int, optional): The ending day number for the CDF calculation. Defaults to 340.
#     - num_points (int, optional): The number of points to calculate between 'start_day' and 'end_day', inclusive. This determines the resolution of the CDF calculation. Defaults to 341.

#     Returns:
#     - pd.DataFrame: A DataFrame where each column represents a different campaign type, and each row represents the percentage of completion for that campaign type at a given day. The first column lists the day numbers.

#     Example:
#     >>> paramDict = {'ACQ': (1.753, 0, 22), 'APP': (1.843, 0, 24)}
#     >>> df_cdf = calculate_cdf_dataframe(paramDict)
#     >>> print(df_cdf.head())
#         Number of Days  ACQ Percent Complete  APP Percent Complete
#     0              0.0                   0.0                   0.0
#     1              1.0                   x.x                   y.y
#     2              2.0                   x.x                   y.y
#     ...
    
#     Note: 'x.x' and 'y.y' represent calculated percentage values based on the gamma CDF for 'ACQ' and 'APP' respectively.
#     """
#     # Define the range of days for the CDF calculation
#     days_range = np.linspace(start_day, end_day, num_points)

#     # Initialize a dictionary to hold CDF results
#     cdf_results = {'Number of Days': days_range}

#     # Calculate the CDF for each campaign type using their parameters
#     for campaign_type, params in paramDict.items():
#         # Unpack the parameters for the gamma distribution
#         shape, loc, scale = params
#         # Calculate the CDF for the given range of days
#         cdf = gamma.cdf(days_range, a=shape, loc=loc, scale=scale)
#         # Store the CDF results
#         cdf_results[f'{campaign_type} Percent Complete'] = cdf * 100  # Convert to percentage

#     # Convert the results into a DataFrame
#     df_cdf = pd.DataFrame(cdf_results)

#     return df_cdf

# def generate_end_years(df, clientMap):
#     """
#     Updates the clientMap with end years for campaign types for each client.
#     Calculates the end years based on the maximum fiscal year in 'df', 
#     counting back 3 years. This tuple of years is then appended to each 
#     campaign type for every client in the clientMap.

#     Dependencies:
#     - Pandas library for DataFrame manipulation.

#     Parameters:
#     df (DataFrame): A pandas DataFrame containing at least a 'FiscalYear' column.
#     clientMap (dict): A nested dictionary containing client and their campaign type details.

#     Returns:
#     dict: The updated clientMap with end years added to each campaign type for each client.

#     Example:
#     # >>> df = pd.DataFrame({'FiscalYear': [2018, 2019, 2020, 2021]})
#     # >>> clientMap = {"Client1": {"CampaignTypes": {"TypeA": [], "TypeB": []}}}
#     # >>> updated_clientMap = generate_end_years(df, clientMap)
#     # >>> updated_clientMap["Client1"]["CampaignTypes"]["TypeA"]
#     [(18, 19, 20)]

#     Operations:
#     - Finds the maximum fiscal year from the 'FiscalYear' column of the DataFrame.
#     - Calculates the start fiscal year, which is three years prior to the maximum fiscal year.
#     - Creates a tuple of end years, consisting of the last two digits of each year from the 
#     start fiscal year to the year before the maximum fiscal year.
#     - Iterates through each client and campaign type in the clientMap, appending the tuple 
#     of end years to the campaign type details.
#     """
#     # Find the maximum fiscal year and calculate the start year (3 years back)
#     max_fiscal_year = df['FiscalYear'].max()
#     start_fiscal_year = max_fiscal_year - 3

#     # Create a tuple of end years (last two digits), from start_fiscal_year to max_fiscal_year - 1
#     end_years = tuple(str(year)[-2:] for year in range(start_fiscal_year, max_fiscal_year))

#     # Update each campaign type in clientMap with separate tuples for start patterns and end years
#     for campaign_type in clientMap['CampaignTypes']:
#         start_patterns = tuple(clientMap['CampaignTypes'][campaign_type])

#         # Update the campaign type with two tuples: start patterns and end years
#         clientMap['CampaignTypes'][campaign_type] = [start_patterns, end_years]

#     return clientMap

# def vectorized_subtract_dates(df, result_dict):
#     """
#     The function takes a Pandas DataFrame and a result_dict mapping CampaignCode to dates.
#     It returns a new DataFrame with an additional column 'DateDelta' representing the number of days
#     between the gift date and mail date.

#     Dependencies:
#     - Pandas library for DataFrame manipulation and datetime operations.

#     Parameters:
#     - df (DataFrame): A Pandas DataFrame with 'CampaignCode' and 'GiftDate' columns.
#     - result_dict (dict): A dictionary mapping 'CampaignCode' to mail dates.
    
#     Returns:
#     - DataFrame: The input DataFrame with an additional 'DateDelta' column.

#     Example:
#     # >>> df = pd.DataFrame({'CampaignCode': ['C1', 'C2'], 'GiftDate': ['2021-01-01', '2021-01-10']})
#     # >>> result_dict = {'C1': '2020-12-25', 'C2': '2021-01-05'}
#     # >>> updated_df = vectorized_subtract_dates(df, result_dict)
#     # >>> updated_df[['CampaignCode', 'DateDelta']]
#     [DataFrame with 'DateDelta' column showing the difference in days]

#     Operations:
#     - Maps 'CampaignCode' to 'MailDate' using the 'result_dict' and converts it to datetime.
#     - Converts 'GiftDate' to datetime format.
#     - Calculates 'DateDelta' as the difference in days between 'GiftDate' and 'MailDate'.
#     - Handles invalid dates by setting the 'DateDelta' for those records to None.
#     """

#     # Convert CampaignCode to mail_date using result_dict
#     df['MailDate'] = df['CampaignCode'].map(result_dict).astype('datetime64[ns]')

#     # Convert GiftDate to datetime
#     df['GiftDate'] = pd.to_datetime(df['GiftDate'])

#     # Subtract dates and get the difference in days
#     df['DateDelta'] = (df['GiftDate'] - df['MailDate']).dt.days

#     # Handle invalid dates
#     df.loc[~df['GiftDate'].notna() | ~df['MailDate'].notna(), 'DateDelta'] = None

#     return df

# def get_params(df, Campaign, end_year):
#     """
#     Fits a gamma distribution to the 'DateDelta' values of a specified campaign and fiscal year in a DataFrame. 
#     The function filters data based on CampaignCode and FiscalYear, handles edge cases like empty data frames or 
#     non-finite values, and checks the validity of fitted parameters.

#     Dependencies:
#     - numpy for numerical operations and checks.
#     - scipy.stats.gamma for fitting and evaluating the gamma distribution.
#     - matplotlib.pyplot for plotting histograms and distribution curves.

#     Parameters:
#     - df (DataFrame): DataFrame with campaign data, including 'CampaignCode', 'FiscalYear', and 'DateDelta' columns.
#     - Campaign (str): String specifying the campaign code.
#     - end_year (int): Integer specifying the fiscal year.

#     Returns:
#     - params (tuple): Tuple (shape, loc, scale) of the fitted gamma distribution, or None if checks fail or an exception occurs.

#     Example:
#     # >>> data = {'CampaignCode': ['C1', 'C1'], 'FiscalYear': [2020, 2020], 'DateDelta': [10, 15]}
#     # >>> df = pd.DataFrame(data)
#     # >>> params = get_params(df, 'C1', 2020)
#     # >>> params
#     (shape, loc, scale values of fitted gamma distribution)

#     Operations:
#     - Filters the DataFrame for the specified campaign and fiscal year.
#     - Handles cases with empty data frames or non-finite 'DateDelta' values.
#     - Fits a gamma distribution to the 'DateDelta' values.
#     - Checks for valid fitted parameters, including checking for NaN values and bounds on the shape parameter.
#     - Plots a histogram of 'DateDelta' with the fitted gamma distribution curve.
#     - Prints campaign details and fitted parameters.
#     """

#     mask = (df.CampaignCode == Campaign) & (df.FiscalYear == end_year) 
#     _df = df.loc[mask]

#     # Handle empty data frames
#     if _df.empty:
#         print(f"Skipping {Campaign} due to empty data frame.")
#         return None

#     # Handle non-finite values in the 'DateDelta' column
#     if not np.isfinite(_df['DateDelta']).all():
#         print(f"Skipping {Campaign} due to non-finite values in 'DateDelta'.")
#         return None

#     try:
#         # Fit gamma distribution
#         params = stats.gamma.fit(_df['DateDelta'])
#     except Exception as e:
#         print(f"Skipping {Campaign} due to an exception during gamma fit: {e}")
#         return None

#     # Check for NaN values in params
#     if any(np.isnan(param) for param in params):
#         print(f"Skipping {Campaign} due to NaN values in parameters.")
#         return None

#     # Skip if shape parameter is less than 0.3 or greater than 5
#     shape_param = params[0]
#     if shape_param < 0.3 or shape_param > 5:
#         print(f"Skipping {Campaign} due to shape parameter being out of bounds (less than 0.3 or greater than 5).")
#         return None

#     gift_sum = _df.groupby('DateDelta')['GiftAmount'].sum()
#     gift_count = _df.groupby('DateDelta').size()

#     # Plotting sum of GiftAmount for each DateDelta
#     plt.figure(figsize=(14, 6))
#     gift_sum.plot(kind='bar', color='b')
#     plt.title(f"Campaign: {Campaign} - Total Gift Amount by DateDelta")
#     plt.xlabel('DateDelta')
#     plt.ylabel('Total Gift Amount')
#     plt.show()

#     # Plotting count of gifts for each DateDelta
#     plt.figure(figsize=(14, 6))
#     gift_count.plot(kind='bar', color='g')
#     plt.title(f"Campaign: {Campaign} - Gift Count by DateDelta")
#     plt.xlabel('DateDelta')
#     plt.ylabel('Gift Count')
#     plt.show()

#     print(f"Campaign: {Campaign}")
#     print(f"Shape: {shape_param}, Loc: {params[1]}, Scale: {params[2]}")

#     return params

# def calculate_campaign_average_from_campaigns(df, Campaigns, curve_type, client_map):
#     """
#     Calculates the average gamma distribution parameters for a set of campaigns from a DataFrame, 
#     based on a specific client and curve type. The function relies on 'get_params' to fit gamma 
#     distributions for each campaign and uses 'client_map' to extract campaign years. It then averages 
#     the parameters of successful gamma fits.

#     Dependencies:
#     - get_params(df, Campaign, end_year): Fits a gamma distribution to 'DateDelta' values for a campaign.

#     Parameters:
#     - df (DataFrame): DataFrame with campaign data, including 'CampaignCode', 'FiscalYear', and 'DateDelta' columns.
#     - Campaigns (dict): Dictionary of campaign codes.
#     - curve_type (str): String specifying the type of curve (e.g., 'Linear', 'Exponential').
#     - client_map (dict): Dictionary mapping client names to properties (like 'YearPosition').
#     - client_name (str): String specifying the client name.

#     Returns:
#     - CampAVG (dict): Dictionary with the average gamma distribution parameters for the specified campaigns,
#     or None if no valid campaigns are found.

#     Example:
#     # >>> df = pd.DataFrame({...})
#     # >>> Campaigns = {"Campaign1": ..., "Campaign2": ...}
#     # >>> curve_type = "Linear"
#     # >>> client_map = {"Client1": {"YearPosition": ...}}
#     # >>> client_name = "Client1"
#     # >>> CampAVG = calculate_campaign_average_from_campaigns(df, Campaigns, curve_type, client_map, client_name)
#     # >>> CampAVG
#     {'Linear Average Response Curve': (average_shape, average_loc, average_scale)}

#     Operations:
#     - Extracts the fiscal year and campaign year position based on the client.
#     - Loops through the specified campaigns to calculate gamma distribution parameters.
#     - Skips campaigns with invalid parameters.
#     - Calculates and stores the average of the valid gamma distribution parameters.
#     - Returns the dictionary of averaged parameters or None if no valid campaigns are found.
# """

#     CampaignDict = {}

#     # Extract the end year - 1
#     end_year = df.FiscalYear.max() - 2

#     # Retrieve the year position directly from client_map
#     year_position = client_map['YearPosition']

#     # Loop through the specified campaigns in Campaigns dictionary
#     for Campaign in Campaigns:
#         # Extract the year part of the campaign based on year_position
#         campaign_year = Campaign[year_position: year_position + 2]

#         # Check if the campaign year matches the last two characters of end_year - 1
#         if campaign_year == str(end_year)[2:]:
#             params = get_params(df, Campaign, end_year)

#             # Skip the campaign if parameters are not valid
#             if params is None:
#                 print(f"Skipping {Campaign} due to negative parameter, shape less than 0.3, or None parameter.")
#                 continue

#             print(f"Campaign: {Campaign}")

#             # Check if params has at least three elements before accessing properties
#             if len(params) >= 3:
#                 print(f"Shape: {params[0]}, Loc: {params[1]}, Scale: {params[2]}")
#                 CampaignDict[Campaign] = params
#                 print("*" * 50)
#             else:
#                 print(f"Invalid params for {Campaign}: {params}")
#                 print(f"Invalid params for {Campaign}: {params}")

#     # Calculate and return average parameters
#     if CampaignDict:
#         total_sum = (0, 0, 0)

#         # Calculate the sum of values in CampaignDict
#         for key in CampaignDict:
#             values = CampaignDict[key]
#             total_sum = tuple(sum(x) for x in zip(total_sum, values))

#         # Calculate the average
#         average_values = tuple(x / len(CampaignDict) for x in total_sum)

#         # Determine the curve type for the key name
#         curve_type_name = f"{curve_type} " if curve_type else ""
#         # Create the CampAVG dictionary
#         CampAVG = {f"{curve_type_name}Average Response Curve": average_values}
#         return CampAVG
#     else:
#         print("No campaigns found for averaging.")
#         return None

# def process_campaign_data(data_frame):
#     """
#     Processes campaign data from a DataFrame to clean and analyze campaign dates and counts. 
#     The function cleans 'MailDate' values, converts them to datetime format, calculates the 
#     count of each 'CampaignCode', determines the minimum 'MailDate' for each 'CampaignCode', 
#     and creates a sorted DataFrame and a dictionary based on these values.

#     Dependencies:
#     - Pandas library for DataFrame manipulation and datetime operations.

#     Parameters:
#     - data_frame (DataFrame): DataFrame containing campaign data with columns 'MailDate' and 'CampaignCode'.

#     Returns:
#     - Tuple containing two elements:
#       1. A sorted DataFrame with columns 'CampaignCode', 'Count', and 'MinMailDate'.
#       2. A dictionary with 'CampaignCode' as keys and 'MailDate' as values.

#     Example:
#     # >>> data = {'CampaignCode': ['C1', 'C2', 'C1'], 'MailDate': ['2021-01-01', '2021-01-02', '2021-01-03']}
#     # >>> df = pd.DataFrame(data)
#     # >>> sorted_campaigns, result_dict = process_campaign_data(df)
#     # >>> sorted_campaigns
#     [DataFrame showing sorted campaign data]
#     # >>> result_dict
#     {'C1': '2021-01-03', 'C2': '2021-01-02'}

#     Operations:
#     - Cleans 'MailDate' values by stripping whitespace and removing non-printable characters.
#     - Converts 'MailDate' to datetime format, handling errors by coercion.
#     - Calculates the count of each 'CampaignCode'.
#     - Determines the minimum 'MailDate' for each 'CampaignCode'.
#     - Creates a DataFrame with 'CampaignCode', its count, and minimum 'MailDate'.
#     - Sorts this DataFrame by count (descending) and then by 'MinMailDate'.
#     - Generates a dictionary with 'CampaignCode' as keys and 'MailDate' as values.

# """

#     # Check if 'MailDate' is a string type and then apply string methods
#     if data_frame['MailDate'].dtype == object:
#         data_frame['MailDate'] = data_frame['MailDate'].str.strip()
#         data_frame['MailDate'] = data_frame['MailDate'].apply(lambda x: ''.join(filter(str.isprintable, x)) if x is not None else None)

#     # Convert 'MailDate' to datetime
#     data_frame['MailDate'] = pd.to_datetime(data_frame['MailDate'], errors='coerce')

#     # Calculate campaign counts
#     campaign_counts = data_frame['CampaignCode'].value_counts()

#     # Calculate the minimum MailDate for each CampaignCode
#     min_mail_dates = data_frame.groupby('CampaignCode')['MailDate'].min()

#     # Create a DataFrame with counts and minimum MailDates
#     result_df = pd.DataFrame({
#         'CampaignCode': campaign_counts.index, 
#         'Count': campaign_counts.values, 
#         'MinMailDate': min_mail_dates.values
#     })

#     # Sort the DataFrame first by count (in descending order), then by MinMailDate
#     sorted_campaigns = result_df.sort_values(by=['Count', 'MinMailDate'], ascending=[False, True])

#     # Create a dictionary with CampaignCode as keys and MailDate as values
#     result_dict = data_frame.set_index('CampaignCode')['MailDate'].to_dict()

#     return sorted_campaigns, result_dict

# def extract_and_process_campaigns(df, sc, unique_campaign_codes, fiscal_start_month):
#     """
#     Extracts and processes specific campaign data from a DataFrame based on campaign types and fiscal year criteria. 
#     The function verifies required campaign types, calculates fiscal years for gifts, extracts campaign codes within 
#     the current fiscal year, and creates dictionaries for the maximum 'GiftDate' and 'MailDate' for each extracted 
#     campaign code.

#     Dependencies:
#     - Pandas library for DataFrame manipulation and datetime operations.
#     - 'fiscal_from_column' function to calculate the fiscal year from a date column.

#     Parameters:
#     - df (DataFrame): DataFrame with campaign data including columns 'CampaignCode' and 'GiftDate'.
#     - sc (DataFrame): DataFrame containing mail campaign data with columns 'CampaignCode' and 'MailDate'.
#     - unique_campaign_codes (dict): Dictionary with campaign types ('Appeals', 'Acquisitions', etc.) as keys 
#       and lists of campaign codes as values.
#     - fiscal_start_month (int): Integer representing the start month of the fiscal year.

#     Returns:
#     - Tuple of two dictionaries:
#       1. max_gift_dates_campaigns: Dictionary with campaign codes as keys and the maximum 'GiftDate' as values.
#       2. max_mail_dates_campaigns: Dictionary with campaign codes as keys and the maximum 'MailDate' as values.
#       Returns (None, None) if required campaign types are missing or no valid campaigns are found.

#     Example:
#     # >>> df = pd.DataFrame({...})
#     # >>> sc = pd.DataFrame({...})
#     # >>> unique_campaign_codes = {'Appeals': ['A1', 'A2'], 'Acquisitions': ['B1', 'B2']}
#     # >>> fiscal_start_month = 7
#     # >>> max_gift_dates, max_mail_dates = extract_and_process_campaigns(df, sc, unique_campaign_codes, fiscal_start_month)
#     # >>> max_gift_dates
#     {'A1': '2021-07-15', 'B1': '2021-08-20'}
#     # >>> max_mail_dates
#     {'A1': '2021-07-10', 'B1': '2021-08-15'}

#     Operations:
#     - Verifies the presence of 'Appeals' and 'Acquisitions' in unique_campaign_codes.
#     - Calculates the fiscal year for each gift based on 'GiftDate' and fiscal_start_month.
#     - Determines the current fiscal year considering the fiscal start month.
#     - Extracts campaign codes within the current fiscal year that meet a 365-day criterion.
#     - Creates dictionaries for the maximum 'GiftDate' and 'MailDate' for each extracted campaign code.
#     """

#     if 'Appeals' not in unique_campaign_codes or 'Acquisitions' not in unique_campaign_codes:
#         print("Missing 'Appeals' or 'Acquisitions' in unique_campaign_codes.")
#         return None, None

#     # Determine fiscal year for each gift
#     df['FiscalYear'] = fiscal_from_column(df, 'GiftDate', fiscal_start_month) 

#     # Find the current fiscal year
#     current_date = pd.to_datetime('today')
#     current_fiscal_year = current_date.year + (current_date.month >= fiscal_start_month)

#     print("Current Fiscal Year: FY" + str(current_fiscal_year))  # Debugging statement

#     # Extract campaign codes within the current fiscal year and meeting the 365-day criterion
#     keys_to_extract = {}
#     for campaign_type, codes in unique_campaign_codes.items():
#         keys_to_extract[campaign_type] = []
#         for code in codes:
#             campaign_data = df[df['CampaignCode'] == code]
#             if not campaign_data.empty:
#                 campaign_fiscal_years = campaign_data['FiscalYear'].unique()
#                 if current_fiscal_year in campaign_fiscal_years:
#                     min_date = campaign_data['GiftDate'].min()
#                     max_date = campaign_data['GiftDate'].max()
#                     if pd.notna(min_date) and pd.notna(max_date) and (max_date - min_date).days < 340:
#                         keys_to_extract[campaign_type].append(code)

#     print("Keys to Extract:", keys_to_extract)  # Debugging statement

#     if not any(keys_to_extract.values()):
#         print("No keys to extract. Check unique_campaign_codes and fiscal year.")

#     # Create dictionaries for max_gift_dates_campaigns and max_mail_dates_campaigns
#     max_gift_dates_campaigns = {key: df[df['CampaignCode'] == key]['GiftDate'].max() for ct, codes in keys_to_extract.items() for key in codes}
#     max_mail_dates_campaigns = {key: sc[sc['CampaignCode'] == key]['MailDate'].max() for ct, codes in keys_to_extract.items() for key in codes}

#     print("Max Gift Dates Campaigns:", max_gift_dates_campaigns)  # Debugging statement
#     print("Max Mail Dates Campaigns:", max_mail_dates_campaigns)  # Debugging statement

#     return max_gift_dates_campaigns, max_mail_dates_campaigns


# def extract_and_process_campaigns(df, sc, unique_campaign_codes, fiscal_start_month):
#     if 'Appeals' not in unique_campaign_codes or 'Acquisitions' not in unique_campaign_codes:
#         print("Missing 'Appeals' or 'Acquisitions' in unique_campaign_codes.")
#         return None, None

#     # Determine fiscal year for each gift
#     df['FiscalYear'] = fiscal_from_column(df, 'GiftDate', fiscal_start_month)

#     # Initialize the loop and set a flag to track if we need to check the previous year
#     check_previous_year = True
#     attempts = 0

#     while check_previous_year and attempts < 2:
#         current_date = pd.to_datetime('today')
#         current_fiscal_year = current_date.year + (current_date.month >= fiscal_start_month) - attempts

#         print(f"Checking Fiscal Year: FY{current_fiscal_year}")  # Debugging statement

#         # Extract campaign codes within the checking fiscal year and meeting the 365-day criterion
#         keys_to_extract = {campaign_type: [] for campaign_type in unique_campaign_codes}
#         for campaign_type, codes in unique_campaign_codes.items():
#             for code in codes:
#                 campaign_data = df[df['CampaignCode'] == code]
#                 if not campaign_data.empty:
#                     campaign_fiscal_years = campaign_data['FiscalYear'].unique()
#                     if current_fiscal_year in campaign_fiscal_years:
#                         min_date = campaign_data['GiftDate'].min()
#                         max_date = campaign_data['GiftDate'].max()
#                         if pd.notna(min_date) and pd.notna(max_date) and (max_date - min_date).days < 365:
#                             keys_to_extract[campaign_type].append(code)

#         print("Keys to Extract:", keys_to_extract)  # Debugging statement

#         # If no keys to extract and this is the first attempt, decrement fiscal year and try again.
#         # Otherwise, stop checking.
#         if not any(keys_to_extract.values()) and attempts == 0:
#             attempts += 1
#         else:
#             check_previous_year = False

#     # Create dictionaries for max_gift_dates_campaigns and max_mail_dates_campaigns
#     max_gift_dates_campaigns = {key: df[df['CampaignCode'] == key]['GiftDate'].max() for ct, codes in keys_to_extract.items() for key in codes}
#     max_mail_dates_campaigns = {key: sc[sc['CampaignCode'] == key]['MailDate'].max() for ct, codes in keys_to_extract.items() for key in codes}

#     print("Max Gift Dates Campaigns:", max_gift_dates_campaigns)  # Debugging statement
#     print("Max Mail Dates Campaigns:", max_mail_dates_campaigns)  # Debugging statement

#     return max_gift_dates_campaigns, max_mail_dates_campaigns


# def days_in_subsequent_months(input_date):

#     """
#     Calculates the number of days remaining in the input month and in the subsequent eleven months.
#     The function computes the days remaining in the month of the input date and the total days in each 
#     of the next eleven months, considering leap years if applicable. It handles year transitions and 
#     leap years correctly.

#     Dependencies:
#     - calendar.monthrange() from the calendar module to determine the number of days in a month.

#     Parameters:
#     - input_date (datetime.date or datetime.datetime): A datetime object representing the date for 
#       which the calculation is to be done.

#     Returns:
#     - List[int]: A list of integers where the first element is the number of days remaining in the 
#       month of the input date and the subsequent elements are the total days in each of the next 
#       eleven months.

#     Example:
#     # >>> from datetime import date
#     # >>> input_date = date(2021, 1, 15)
#     # >>> remaining_days = days_in_subsequent_months(input_date)
#     # >>> remaining_days
#     [16, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

#     Operations:
#     - Determines the number of days in the month of the input date.
#     - Calculates the remaining days in the input month.
#     - Iterates over the next eleven months, calculating the number of days in each month.
#     - Accounts for year changes and leap years in the calculation.
#     """

#     _, days_in_start_month = monthrange(input_date.year, input_date.month)
#     remaining_days = [days_in_start_month - input_date.day]

#     for i in range(1, 12):
#         next_month = (input_date.month + i) % 12
#         if next_month == 0:
#             next_month = 12
#         _, days_in_month = monthrange(input_date.year + (input_date.month + i - 1) // 12, next_month)
#         remaining_days.append(days_in_month)

#     return remaining_days

# COMMAND ----------

# def apply_cdf_to_result_dict(result_dict, shape, loc, scale):
#     """
#     Applies the Cumulative Distribution Function (CDF) of the gamma distribution to a dictionary of results. 
#     The function calculates the CDF values of the gamma distribution for the given shape, loc, and scale parameters 
#     for each key in the input dictionary. The dictionary values are assumed to be lists of days, representing time 
#     intervals. The function calculates the cumulative probability for each interval and stores the results in a new 
#     dictionary.

#     Dependencies:
#     - scipy.stats.gamma for calculating the CDF of the gamma distribution.

#     Parameters:
#     - result_dict (dict): Dictionary with keys representing identifiers and values as lists of days (intervals).
#     - shape (float): The shape parameter of the gamma distribution.
#     - loc (float): The location parameter of the gamma distribution.
#     - scale (float): The scale parameter of the gamma distribution.

#     Returns:
#     - dict: A dictionary with the same keys as 'result_dict'. Each key is associated with a list of CDF values 
#       corresponding to the intervals specified in the input dictionary.

#     Example:
#     # >>> result_dict = {'Campaign1': [10, 20, 30], 'Campaign2': [15, 25, 35]}
#     # >>> shape, loc, scale = 2.0, 0.0, 1.0
#     # >>> cdf_result = apply_cdf_to_result_dict(result_dict, shape, loc, scale)
#     # >>> cdf_result
#     {'Campaign1': [CDF values], 'Campaign2': [CDF values]}

#     Operations:
#     - Iterates over each key-value pair in the input dictionary.
#     - For each list of days (value), computes the CDF value for the cumulative time up to that day and subtracts 
#       the CDF value of the previous cumulative time.
#     - Stores the computed CDF values in a new dictionary with the same keys.
#     """
#     cdf_values = {}
#     for key, value in result_dict.items():
#         cdf_val = []
#         prev_day = 0
#         for days in value:
#             cdf_val.append(stats.gamma.cdf(prev_day + days, shape, loc, scale) - stats.gamma.cdf(prev_day, shape, loc, scale))
#             prev_day += days
#         cdf_values[key] = cdf_val
#     return cdf_values

# def create_result_dict(MailDateDict):

#     """
#     Creates a dictionary that maps campaign codes to lists of the number of days remaining in the current month 
#     and in the subsequent eleven months. The function parses each date string in the input dictionary and calculates 
#     the days remaining using the 'days_in_subsequent_months' function.

#     Dependencies:
#     - 'dateutil.parser.parse' for parsing date strings.
#     - 'days_in_subsequent_months' function to calculate the number of days in subsequent months.

#     Parameters:
#     - MailDateDict (dict): Dictionary with campaign codes as keys and date strings as values.

#     Returns:
#     - dict: A dictionary with campaign codes as keys. Each key is associated with a list of integers where the 
#       first element is the number of days remaining in the month of the input date and the subsequent elements 
#       are the total days in each of the next eleven months.

#     Example:
#     # >>> MailDateDict = {'Campaign1': '2021-01-15', 'Campaign2': '2021-02-20'}
#     # >>> result_dict = create_result_dict(MailDateDict)
#     # >>> result_dict
#     {'Campaign1': [16, 28, 31, 30, ...], 'Campaign2': [8, 31, 30, 31, ...]}

#     Operations:
#     - Iterates over each key-value pair in the input dictionary.
#     - Parses each date string to a datetime object.
#     - Calculates the days remaining in the current month and in the subsequent eleven months for each parsed date.
#     - Stores the calculated days in a new dictionary with the same keys.
#     """

#     result_dict = {}
#     for campaign, date_str in MailDateDict.items():
#         try:
#             # Parse the date string
#             input_date = parser.parse(date_str)
#         except ValueError:
#             # Handle the case where the date string cannot be parsed
#             print(f"Unable to parse date string for campaign {campaign}: {date_str}")
#             continue  # Skip this campaign

#         days_remaining = days_in_subsequent_months(input_date)
#         result_dict[campaign] = days_remaining

#     return result_dict

# def calculate_dollar_values(campaigns, campaign_budgets, cdf_values):

#     """
#     Calculates the dollar values for campaigns based on their budgets and CDF values from a gamma distribution.

#     This function multiplies the CDF values of each campaign by its corresponding budget to calculate the expected 
#     dollar values. It processes a list of campaign identifiers, their associated budgets, and a dictionary of CDF 
#     values for each campaign. The function handles exceptions and prints relevant information for each campaign 
#     during processing.

#     Dependencies:
#     - Basic Python operations. No external dependencies.

#     Parameters:
#     - campaigns (list): A list of campaign identifiers (strings).
#     - campaign_budgets (dict): A dictionary with campaign identifiers as keys and their respective budgets as values.
#     - cdf_values (dict): A dictionary with campaign identifiers as keys and lists of CDF values as values.

#     Returns:
#     - dict: A dictionary with campaign identifiers as keys and lists of calculated dollar values as values. If an 
#       error occurs, it prints the error message and returns the dictionary with processed data up to the point of the error.

#     Example:
#     # >>> campaigns = ['Campaign1', 'Campaign2']
#     # >>> campaign_budgets = {'Campaign1': 1000, 'Campaign2': 2000}
#     # >>> cdf_values = {'Campaign1': [0.1, 0.2, 0.3], 'Campaign2': [0.2, 0.4, 0.6]}
#     # >>> dollar_values = calculate_dollar_values(campaigns, campaign_budgets, cdf_values)
#     # >>> dollar_values
#     {'Campaign1': [100.0, 200.0, 300.0], 'Campaign2': [400.0, 800.0, 1200.0]}

#     Operations:
#     - Iterates over each campaign in the campaigns list.
#     - Retrieves the CDF values and budget for the current campaign.
#     - Calculates the dollar value for each interval by multiplying the CDF value by the campaign's budget.
#     - Stores the calculated dollar values in a new dictionary.
#     - Handles any exceptions that occur during the process and prints error messages.
#     """

#     dollar_values_with_budgets = {}
#     try:
#         for i, campaign in enumerate(campaigns):
#             print(f"Processing campaign: {campaign}")
#             print(f"CDF Values: {cdf_values[campaign]}")
#             print(f"Campaign Budget: {campaign_budgets[campaign]}")

#             campaign_dollar_values = [float(val) * float(campaign_budgets[campaign]) for val in cdf_values[campaign]]
#             dollar_values_with_budgets[campaign] = campaign_dollar_values

#             print(f"Dollar Values: {campaign_dollar_values}")
#     except Exception as e:
#         print(f"Error: {e}")

#     return dollar_values_with_budgets

# def merge_dates_with_values(dates_dict, values_dict):

#     """
#     Merges two dictionaries: one with dates and another with values, based on common keys.

#     This function creates a new dictionary that combines the dates and values associated with the same keys from two 
#     input dictionaries. For each key in the values dictionary, it checks if the key exists in the dates dictionary. 
#     If so, it creates a new entry in the merged dictionary with the key, the corresponding date from the dates 
#     dictionary, and the list of values from the values dictionary.

#     Dependencies:
#     - Basic Python operations. No external dependencies.

#     Parameters:
#     - dates_dict (dict): Dictionary with keys representing identifiers and values as dates.
#     - values_dict (dict): Dictionary with the same identifiers as keys and lists of values associated with these 
#       identifiers.

#     Returns:
#     - dict: A dictionary where each key corresponds to an identifier found in both input dictionaries. Each key is 
#       associated with a list containing the date from the dates dictionary followed by the list of values from the 
#       values dictionary.

#     Example:
#     # >>> dates_dict = {'Campaign1': '2021-01-01', 'Campaign2': '2021-02-01'}
#     # >>> values_dict = {'Campaign1': [100, 200, 300], 'Campaign2': [400, 500, 600]}
#     # >>> merged_dict = merge_dates_with_values(dates_dict, values_dict)
#     # >>> merged_dict
#     {'Campaign1': ['2021-01-01', [100, 200, 300]], 'Campaign2': ['2021-02-01', [400, 500, 600]]}

#     Operations:
#     - Iterates over each key-value pair in the values dictionary.
#     - Checks if the key is present in the dates dictionary.
#     - If the key is found, creates a new entry in the merged dictionary with the date and the values associated with 
#       the key.
#     - Continues this process for all keys in the values dictionary.

#     """

#     merged_dict = {}
#     for key, values in values_dict.items():
#         if key in dates_dict:
#             merged_dict[key] = [dates_dict[key]] + [values]
#     return merged_dict

# def create_MailDateDict(dataframe, key_column, value_column):
#     """
#     Creates a dictionary from two columns of a DataFrame.

#     This function takes a DataFrame and two column names as inputs and creates a dictionary where the keys are values 
#     from the specified key column and the values are from the specified value column. It's particularly useful for 
#     quickly converting DataFrame columns to a dictionary format for easier data manipulation or lookup.

#     Dependencies:
#     - Pandas library for DataFrame manipulation.
#     - Python's built-in 'zip' function for pairing elements.

#     Parameters:
#     - dataframe (DataFrame): A pandas DataFrame from which the data is extracted.
#     - key_column (str): The name of the column in the DataFrame to be used as keys in the dictionary.
#     - value_column (str): The name of the column in the DataFrame to be used as values in the dictionary.

#     Returns:
#     - dict: A dictionary with keys and values corresponding to the specified columns in the DataFrame.

#     Example:
#     # >>> df = pd.DataFrame({'CampaignCode': ['C1', 'C2'], 'MailDate': ['2021-01-01', '2021-02-01']})
#     # >>> MailDateDict = create_MailDateDict(df, 'CampaignCode', 'MailDate')
#     # >>> MailDateDict
#     {'C1': '2021-01-01', 'C2': '2021-02-01'}

#     Operations:
#     - Extracts the specified key and value columns from the DataFrame.
#     - Uses the 'zip' function to pair elements from the key and value columns.
#     - Converts the zipped pairs into a dictionary.
#     """

#     MailDateDict = dict(zip(dataframe[key_column], dataframe[value_column]))
#     return MailDateDict

# def create_campaign_budgets(dataframe, key_column, value_column):

#     """
#     Creates a dictionary of campaign budgets from two columns of a DataFrame.

#     This function generates a dictionary where the keys are campaign identifiers and the values are their corresponding 
#     budgets. It takes a DataFrame and two column names as inputs, with one column representing the campaign identifiers 
#     and the other representing the budgets. The function is useful for converting DataFrame columns into a dictionary 
#     format for convenient access and manipulation of campaign budget data.

#     Dependencies:
#     - Pandas library for DataFrame manipulation.
#     - Python's built-in 'zip' function for pairing elements.

#     Parameters:
#     - dataframe (DataFrame): A pandas DataFrame from which the data is extracted.
#     - key_column (str): The name of the column in the DataFrame to be used as keys in the dictionary, representing 
#       campaign identifiers.
#     - value_column (str): The name of the column in the DataFrame to be used as values in the dictionary, representing 
#       campaign budgets.

#     Returns:
#     - dict: A dictionary where each key is a campaign identifier and each value is the corresponding budget for that 
#       campaign.

#     Example:
#     # >>> df = pd.DataFrame({'CampaignCode': ['C1', 'C2'], 'Budget': [1000, 2000]})
#     # >>> campaign_budgets = create_campaign_budgets(df, 'CampaignCode', 'Budget')
#     # >>> campaign_budgets
#     {'C1': 1000, 'C2': 2000}

#     Operations:
#     - Extracts the specified key and value columns from the DataFrame, corresponding to campaign identifiers and budgets.
#     - Pairs elements from the key and value columns using the 'zip' function.
#     - Converts these paired elements into a dictionary.
#     """

#     campaign_budgets = dict(zip(dataframe[key_column], dataframe[value_column]))
#     return campaign_budgets

# def populate_dataframe(mail_date_dict, months):

#     """
#      Populates a DataFrame with data based on a mail date dictionary and a list of months.

#     This function creates a DataFrame where each column represents a different campaign, and each row represents a month. 
#     The values in the DataFrame are populated based on the mail date dictionary, which contains campaign start dates and 
#     values associated with each campaign. The function formats the months, initializes the DataFrame, populates it with 
#     campaign data, and calculates monthly totals.

#     Dependencies:
#     - Pandas library for DataFrame manipulation.
#     - Python's datetime module for handling date formatting.

#     Parameters:
#     - mail_date_dict (dict): Dictionary with campaign names as keys and a list containing the start date and associated 
#       values for each campaign as values.
#     - months (list[datetime]): List of datetime objects representing the months for which the DataFrame is to be populated.

#     Returns:
#     - DataFrame: A pandas DataFrame with rows representing months and columns representing campaigns. Each cell contains 
#       the value associated with a campaign for a given month, and there is an additional column for the total values of all 
#       campaigns for each month.

#     Example:
#     # >>> mail_date_dict = {'Campaign1': ['2021-01-01', 100, 200], 'Campaign2': ['2021-02-01', 150, 250]}
#     # >>> months = [datetime(2021, 1, 1), datetime(2021, 2, 1)]
#     # >>> df = populate_dataframe(mail_date_dict, months)
#     # >>> df
#     [DataFrame showing campaigns and their values for each month]

#     Operations:
#     - Formats the 'months' list into strings representing 'Month Year'.
#     - Initializes a DataFrame with columns for each campaign, a 'Month' column, and a 'Monthly Total' column.
#     - Populates the DataFrame with zeros and then with values from the mail date dictionary.
#     - Calculates the total for each month across all campaigns.
#     """

#     # Convert the datetime objects in 'months' to strings in 'Month Year' format
#     formatted_months = [month.strftime('%B %Y') for month in months]

#     # Initialize DataFrame with formatted 'Month' column
#     df = pd.DataFrame(columns=["Month"] + list(mail_date_dict.keys()) + ["Monthly Total"])
#     df['Month'] = formatted_months

#     # Initialize the rest of the DataFrame
#     for key in mail_date_dict:
#         df[key] = 0
#     df["Monthly Total"] = 0

#     for key, value in mail_date_dict.items():
#         # Ensure the date is a datetime object and then format it
#         campaign_start_date = value[0] if isinstance(value[0], datetime) else parse_date(value[0])
#         campaign_start_date_str = campaign_start_date.strftime('%B %Y')

#         if campaign_start_date_str in df['Month'].values:
#             start_index = df.index[df['Month'] == campaign_start_date_str][0]
#             for i, val in enumerate(value[1]):
#                 if start_index + i < len(df):
#                     df.loc[start_index + i, key] = val

#     # Calculate the monthly total
#     df["Monthly Total"] = df.iloc[:, 1:-1].sum(axis=1)
#     return df

# def get_sharepoint_context(site_url, username, password):

#     """
#     Establishes a context for interacting with a SharePoint site using provided credentials.

#     This function attempts to authenticate a user to a SharePoint site using a username and password. If authentication 
#     is successful, it creates and returns a context object that can be used for subsequent interactions with the SharePoint 
#     site. If authentication fails, the function raises an exception.

#     Dependencies:
#     - Office365-REST-Python-Client library for SharePoint site interaction and authentication.

#     Parameters:
#     - site_url (str): A string representing the URL of the SharePoint site to connect to.
#     - username (str): A string representing the username for authentication.
#     - password (str): A string representing the password for authentication.

#     Returns:
#     - ClientContext: A ClientContext object that represents the SharePoint site context, allowing for further interactions 
#       with the site.

#     Exceptions:
#     - Raises an Exception if authentication fails.

#     Example:
#     # >>> site_url = 'https://example.sharepoint.com'
#     # >>> username = 'user@example.com'
#     # >>> password = 'password'
#     # >>> ctx = get_sharepoint_context(site_url, username, password)
#     # [ClientContext object is returned for successful authentication]

#     Operations:
#     - Initializes an AuthenticationContext with the provided SharePoint site URL.
#     - Attempts to acquire a token for the user using the provided username and password.
#     - If authentication is successful, creates a ClientContext for the SharePoint site.
#     - If authentication fails, raises an exception indicating that authentication failed.
#     """

#     ctx_auth = AuthenticationContext(site_url)
#     if ctx_auth.acquire_token_for_user(username, password):
#         ctx = ClientContext(site_url, ctx_auth)
#         return ctx
#     else:
#         raise Exception("Authentication failed")

# def transfer_from_cluster(client_context, relative_url, file_name):

#     """
#     Transfers a file from a cluster to a specified location in a SharePoint library.

#     This function uploads a file from a cluster's temporary storage to a SharePoint library at a specified relative URL. 
#     It uses the ClientContext object to interact with SharePoint, creates a file creation information object, reads the 
#     file content, and then uploads the file to the library. If the operation is successful, it returns a confirmation 
#     message. In case of an exception, the exception is raised for further handling.

#     Dependencies:
#     - Office365-REST-Python-Client library for SharePoint site interaction.
#     - Python's os.path and built-in file handling for reading the file content.

#     Parameters:
#     - client_context (ClientContext): A ClientContext object for interacting with SharePoint.
#     - relative_url (str): A string representing the server-relative URL of the folder in the SharePoint library where 
#       the file is to be uploaded.
#     - file_name (str): A string representing the name of the file to be transferred.

#     Returns:
#     - str: A success message string confirming the transfer of the file to the specified SharePoint library location.

#     Exceptions:
#     - Raises an exception if an error occurs during the file transfer process.

#     Example:
#     # >>> client_context = get_sharepoint_context('https://example.sharepoint.com', 'user@example.com', 'password')
#     # >>> relative_url = '/Shared Documents/folder'
#     # >>> file_name = 'example.txt'
#     # >>> message = transfer_from_cluster(client_context, relative_url, file_name)
#     # >>> message
#     'Transferred example.txt to /Shared Documents/folder'

#     Operations:
#     - Retrieves the folder in the SharePoint library corresponding to the given relative URL.
#     - Initializes a FileCreationInformation object and reads the file content from the cluster's temporary storage.
#     - Sets the URL and overwrite properties for the file creation information.
#     - Uploads the file to the SharePoint library.
#     - Executes the client context query to complete the upload.
#     - Returns a success message upon successful transfer.
#     - Raises an exception if any errors occur during the process.
#     """

#     try:
#         libraryRoot = client_context.web.get_folder_by_server_relative_url(relative_url)
#         info = FileCreationInformation()
#         with open(os.path.join('/dbfs/tmp', file_name), 'rb') as content_file:
#             info.content = content_file.read()
#         info.url = file_name
#         info.overwrite = True
#         upload_file = libraryRoot.files.add(info)
#         client_context.execute_query()
#         return 'Transferred %s to %s' % (file_name, relative_url)
#     except Exception as e:
#         raise e

# def _extract_appeals_acquisitions(df, mapper):
#     print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line to show keys

#     d = {}
#     if mapper['Method'] == 'starts_ends':
#         _mapper = mapper['CampaignTypes']
#         n = mapper['YearPosition']
#         print(f"Using 'starts_ends' method with YearPosition at {n}")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             m1 = (df.CampaignCode.str.startswith(_mapper[ct][0]))
#             end_patterns = _mapper[ct][-1]
#             if isinstance(end_patterns, tuple):
#                 end_pattern_regex = '|'.join(end_patterns)
#             else:
#                 end_pattern_regex = end_patterns
#             m2 = df.CampaignCode.str[n: n+2].str.contains(end_pattern_regex)
#             mask = m1 & m2
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Start pattern: {_mapper[ct][0]}, End pattern regex: {end_pattern_regex}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line
#     elif mapper['Method'] == 'contains':
#         _mapper = mapper['CampaignTypes']
#         print("Using 'contains' method")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             mask = (df.CampaignCode.str.contains(_mapper[ct]))
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Pattern: {_mapper[ct]}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line

#     print("Final campaign mapping:", d)  # Debugging line
#     return d

# # def _extract_appeals_acquisitions(df, mapper):

# #     """
# #     Extracts specific campaign types from a DataFrame based on criteria defined in a mapper dictionary.

# #     This function categorizes campaign codes in a DataFrame into different types (like 'Appeals' or 'Acquisitions') 
# #     based on the rules defined in a mapper dictionary. The mapper can specify different methods for identifying 
# #     campaign types, such as by the start and end patterns of campaign codes ('starts_ends') or by whether the 
# #     campaign code contains certain patterns ('contains'). The function returns a dictionary categorizing campaign 
# #     codes into their respective types.

# #     Dependencies:
# #     - Pandas library for DataFrame manipulation.
# #     - Python's regular expressions (regex) for pattern matching in strings.

# #     Parameters:
# #     - df (DataFrame): A pandas DataFrame with a 'CampaignCode' column containing campaign codes.
# #     - mapper (dict): A dictionary specifying how to categorize campaign codes. It includes the method ('Method'), 
# #       campaign types ('CampaignTypes'), and year position in the campaign code ('YearPosition') if applicable.

# #     Returns:
# #     - dict: A dictionary where keys are campaign types and values are arrays of unique campaign codes corresponding 
# #       to each type.

# #     Example:
# #     # >>> df = pd.DataFrame({'CampaignCode': ['A2021', 'B2021', 'C2022']})
# #     # >>> mapper = {'Method': 'starts_ends', 'CampaignTypes': {'Appeals': ('A', '21')}, 'YearPosition': 1}
# #     # >>> extracted_campaigns = _extract_appeals_acquisitions(df, mapper)
# #     # >>> extracted_campaigns
# #     {'Appeals': ['A2021']}

# #     Note:
# #     - The function includes a debugging print statement to display keys in the mapper dictionary.
# #     - The 'YearPosition' parameter in the mapper is used to dynamically determine end years for the 'starts_ends' method.
# #     Operations:
# #     - Checks the method specified in the mapper dictionary for categorizing campaigns.
# #     - For the 'starts_ends' method, uses start patterns and end patterns (regex) to categorize campaign codes.
# #     - For the 'contains' method, checks if campaign codes contain specified patterns.
# #     - Categorizes campaign codes based on the matching criteria and stores them in a dictionary.
# #     """

# #     # print("Keys in 'mapper' dictionary:", mapper.keys())  # Add this line # TODO make end years in mapper dynmaic 
# #     # d = {}
# #     # if mapper['Method'] == 'starts_ends':
# #     #     _mapper = mapper['CampaignTypes']
# #     #     n = mapper['YearPosition']
# #     #     for ct in _mapper.keys():
# #     #         m1 = (df.CampaignCode.str.startswith(_mapper[ct][0]))
# #     #         end_patterns = _mapper[ct][-1]
# #     #         if isinstance(end_patterns, tuple):
# #     #             # Join the patterns into a regex pattern with OR operator
# #     #             end_pattern_regex = '|'.join(end_patterns)
# #     #         else:
# #     #             end_pattern_regex = end_patterns
# #     #         m2 = df.CampaignCode.str[n: n+2].str.contains(end_pattern_regex)
# #     #         mask = m1 & m2
# #     #         d[ct] = df.loc[mask, 'CampaignCode'].unique()
# #     # elif mapper['Method'] == 'contains':
# #     #     _mapper = mapper['CampaignTypes']
# #     #     for ct in _mapper.keys():
# #     #         mask = (df.CampaignCode.str.contains(_mapper[ct]))
# #     #         d[ct] = df.loc[mask, 'CampaignCode'].unique()
# #     # return d

# def _extract_appeals_acquisitions(df, mapper):
#     print("Keys in 'mapper' dictionary:", mapper.keys())  # Debugging line

#     d = {}
#     if mapper['Method'] == 'starts_ends':
#         _mapper = mapper['CampaignTypes']
#         n = mapper['YearPosition']
#         print(f"Using 'starts_ends' method with YearPosition at {n}")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             m1 = df.CampaignCode.str.startswith(_mapper[ct][0])
#             end_patterns = _mapper[ct][-1]
#             if isinstance(end_patterns, tuple):
#                 end_pattern_regex = '|'.join(end_patterns)
#             else:
#                 end_pattern_regex = end_patterns
            
#             # Adjust here for ending pattern
#             # Check if there is a specific last character pattern defined
#             if len(_mapper[ct]) > 2:  # Assuming the last position is for end character patterns
#                 last_char_pattern = _mapper[ct][2]
#                 if isinstance(last_char_pattern, tuple):
#                     last_char_regex = '|'.join(last_char_pattern)
#                 else:
#                     last_char_regex = last_char_pattern
#                 m3 = df.CampaignCode.str.endswith(last_char_regex)
#             else:
#                 m3 = pd.Series([True] * len(df))  # Default to True if no end character pattern provided
            
#             m2 = df.CampaignCode.str[n: n+2].str.contains(end_pattern_regex)
#             mask = m1 & m2 & m3
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Start pattern: {_mapper[ct][0]}, End pattern regex: {end_pattern_regex}, End char pattern: {last_char_regex if len(_mapper[ct]) > 2 else 'Not provided'}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line
#     elif mapper['Method'] == 'contains':
#         _mapper = mapper['CampaignTypes']
#         print("Using 'contains' method")  # Debugging line
#         for ct in _mapper.keys():
#             print(f"Processing CampaignType: {ct}")  # Debugging line
#             mask = (df.CampaignCode.str.contains(_mapper[ct]))
#             d[ct] = df.loc[mask, 'CampaignCode'].unique()
#             print(f"Pattern: {_mapper[ct]}")  # Debugging line
#             print(f"Found {len(d[ct])} campaigns for type {ct}")  # Debugging line
#             if len(d[ct]) == 0:
#                 print(f"No campaigns found for {ct}.")  # Debugging line

#     print("Final campaign mapping:", d)  # Debugging line
#     return d


# def process_cash_flow_data(CashFlow, shape, loc, scale, months, client):

#     """
#     Processes cash flow data for campaigns based on either budget or gifts, depending on the client.

#     This function takes a DataFrame containing campaign cash flow data and performs a series of operations to calculate 
#     and organize this data into a readable format. It works with two versions of the data: one based on campaign budgets 
#     and another based on campaign gifts, if applicable. The function applies a Cumulative Distribution Function (CDF) 
#     based on provided gamma distribution parameters and calculates dollar values for each campaign. It then populates 
#     a DataFrame for each version (budget and gifts) with this information.

#     Dependencies:
#     - Pandas library for DataFrame manipulation.
#     - Helper functions: 'create_MailDateDict', 'create_campaign_budgets', 'apply_cdf_to_result_dict', 
#       'calculate_dollar_values', 'merge_dates_with_values', 'populate_dataframe'.

#     Parameters:
#     - CashFlow (DataFrame): DataFrame with campaign cash flow data, including 'CampaignName', 'MailDate', 'Budget', 
#       and 'Gifts'.
#     - shape, loc, scale (float): Gamma distribution parameters for applying the CDF.
#     - months (list[datetime]): List of datetime objects representing the months for DataFrame population.
#     - client (str): String representing the client's name.

#     Returns:
#     - DataFrame(s): If the client is in 'GiftClients', returns two DataFrames: one for budget data and one for gifts 
#       data. If the client is not in 'GiftClients', returns only the budget data DataFrame.

#     Example:
#     # >>> CashFlow = pd.DataFrame({...})
#     # >>> shape, loc, scale = 2.0, 0.0, 1.0
#     # >>> months = [datetime(2021, 1, 1), datetime(2021, 2, 1)]
#     # >>> client = 'Client1'
#     # >>> df_budget, df_gifts = process_cash_flow_data(CashFlow, shape, loc, scale, months, client)
#     # [DataFrames populated with budget and gifts data]

#     Note:
#     - 'GiftClients' is a predefined list of clients for whom gift data processing is required.
#     - The function uses several helper functions like 'create_MailDateDict', 'create_campaign_budgets', 
#       'apply_cdf_to_result_dict', etc., to process the data.
#     """

#     MailDateDict = create_MailDateDict(CashFlow, 'CampaignName', 'MailDate')

#     # Always perform the budget version
#     campaign_budgets = create_campaign_budgets(CashFlow, 'CampaignName', 'Budget')
#     result_dict_budget = create_result_dict(MailDateDict)
#     cdf_values_budget = apply_cdf_to_result_dict(result_dict_budget, shape, loc, scale)
#     campaigns_budget = list(cdf_values_budget.keys())
#     dollar_values_with_budgets = calculate_dollar_values(campaigns_budget, campaign_budgets, cdf_values_budget)
#     mail_date_dict_budget = merge_dates_with_values(MailDateDict, dollar_values_with_budgets)
#     df_budget = populate_dataframe(mail_date_dict_budget, months)

#     # Check if the client is in GIFT_CLIENTS and perform the gifts version
#     if client in GIFT_CLIENTS:
#         campaign_gifts = create_campaign_budgets(CashFlow, 'CampaignName', 'Gifts')
#         result_dict_gifts = create_result_dict(MailDateDict)
#         cdf_values_gifts = apply_cdf_to_result_dict(result_dict_gifts, shape, loc, scale)
#         campaigns_gifts = list(cdf_values_gifts.keys())
#         dollar_values_with_gifts = calculate_dollar_values(campaigns_gifts, campaign_gifts, cdf_values_gifts)
#         mail_date_dict_gifts = merge_dates_with_values(MailDateDict, dollar_values_with_gifts)
#         df_gifts = populate_dataframe(mail_date_dict_gifts, months)
#         # Combine df_budget and df_gifts as needed, or return them separately

#     # Return the appropriate dataframe(s)
#     if client in GIFT_CLIENTS:
#         return df_budget, df_gifts
#     else:
#         return df_budget

# COMMAND ----------

# def generate_month_list_from_dataframes(cashFlowDict, extend_months=12):

#     """
#     Generates a list of months as datetime objects based on the dates in a dictionary of DataFrames.

#     This function extracts dates from the 'MailDate' column of each DataFrame in the provided dictionary. It then 
#     standardizes these dates, determines the earliest and latest dates across all DataFrames, and generates a list 
#     of month start dates as datetime objects. The function can extend the list by a specified number of months beyond 
#     the latest date found.

#     Dependencies:
#     - Pandas library for DataFrame manipulation.
#     - Dateutil's relativedelta for date arithmetic.
#     - Custom 'parse_date' function for standardizing date strings.

#     Parameters:
#     - cashFlowDict (dict): A dictionary where each key corresponds to a category, and each value is a DataFrame with 
#       a 'MailDate' column containing date strings.
#     - extend_months (int, optional): An optional integer specifying the number of months to extend beyond the latest 
#       date found in the DataFrames (default is 12 months).

#     Returns:
#     - list[datetime]: A list of datetime objects representing the first day of each month from the earliest date found 
#       to the extended latest date.

#     Example:
#     # >>> cashFlowDict = {'Type1': pd.DataFrame({'MailDate': ['2021-01-15', '2021-02-15']}), 
#     #                     'Type2': pd.DataFrame({'MailDate': ['2021-03-15', '2021-04-15']})}
#     # >>> month_list = generate_month_list_from_dataframes(cashFlowDict, extend_months=12)
#     # >>> month_list
#     [datetime(2021, 1, 1), datetime(2021, 2, 1), ..., datetime(2022, 4, 1)]

#     Operations:
#     - Extracts and standardizes dates from the 'MailDate' column in each DataFrame.
#     - Determines the earliest and latest dates across all DataFrames.
#     - Extends the latest date by a specified number of months (if applicable).
#     - Generates a list of month start dates, from the earliest date to the extended latest date.
#     """

#     all_dates = []

#     # Extract and standardize all dates
#     for df in cashFlowDict.values():
#         standardized_dates = df['MailDate'].apply(parse_date)
#         all_dates.extend(standardized_dates)

#     # Find the earliest and latest dates
#     earliest_date = min(all_dates)
#     latest_date = max(all_dates) + relativedelta(months=extend_months)

#     # Generate the list of months as datetime objects
#     month_list = []
#     current_date = earliest_date.replace(day=1)  # Set to the first day of the month
#     while current_date <= latest_date:
#         month_list.append(current_date)
#         current_date += relativedelta(months=1)

#     return month_list

# def parse_date(date_str):

#     """
#     Parses a date string into a datetime object.

#     This function attempts to convert a date string into a datetime object. It uses a date parser to handle a variety 
#     of date string formats. If the string cannot be parsed into a valid date, the function raises a ValueError with a 
#     descriptive message.

#     Dependencies:
#     - Dateutil's parser module for parsing date strings.

#     Parameters:
#     - date_str (str): A string representing a date.

#     Returns:
#     - datetime: A datetime object representing the parsed date.

#     Exceptions:
#     - Raises a ValueError if the date string cannot be parsed into a valid date, with a message indicating the unparseable 
#       date string.

#     Example:
#     # >>> date_str = '2021-01-15'
#     # >>> parsed_date = parse_date(date_str)
#     # >>> parsed_date
#     datetime.datetime(2021, 1, 15, 0, 0)

#     Operations:
#     - Attempts to parse the provided date string using a date parser.
#     - If successful, returns the parsed datetime object.
#     - If the string cannot be parsed, raises a ValueError indicating the issue.
#     """

#     try:
#         # Attempt to parse the date string
#         return parser.parse(date_str)
#     except ValueError:
#         # Raise an error if the string cannot be parsed as a date
#         raise ValueError(f"Unable to parse date string: {date_str}")