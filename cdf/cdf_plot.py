# Databricks notebook source
def create_trace(campaign_name, x_range, params):
    """
        Creates a trace for a gamma cumulative distribution function (CDF) plot using Plotly, 
        for a given campaign. The trace is initially set to be visible only in the legend and 
        includes a custom hover template.

        Dependencies:
        - stats.gamma.cdf(x, *params): Computes the gamma CDF at values 'x' with given parameters.

        Inputs:
        - campaign_name: String specifying the name of the campaign.
        - x_range: Array or list of x-values where the gamma CDF should be evaluated.
        - params: Tuple or list of parameters for the gamma distribution. Expected to be in the 
        format (shape, loc, scale).

        Returns:
        - A Plotly Scatter object representing the gamma CDF plot for the given campaign. The trace 
        is set to 'lines' mode, is initially visible only in the legend, and includes a custom 
        hovertemplate that shows the campaign name and the percentage completed.

        Example:
        # >>> x_values = np.linspace(0, 10, 100)
        # >>> gamma_params = (2, 0, 1)
        # >>> trace = create_trace("Campaign A", x_values, gamma_params)
        # >>> trace.name
        'Campaign A'
        """
    y_values = stats.gamma.cdf(x_range, *params)
    return go.Scatter(
        x=x_range,
        y=y_values,
        mode='lines',
        name=campaign_name,
        visible='legendonly',
        hovertemplate=f"{campaign_name}<br>%{{y:.2%}} completed<extra></extra>"
    )

def create_html_table_and_link(df, name):
    """
        Converts a DataFrame into an HTML table and creates a downloadable CSV link for it. 
        The function rounds numeric values in the DataFrame to two decimal places before converting 
        to HTML and CSV formats. It generates a link that allows users to download the data as a CSV file.

        Dependencies:
        - pandas.DataFrame.round(): Rounds the numeric values in the DataFrame.
        - pandas.DataFrame.to_csv(): Converts the DataFrame to a CSV string.
        - pandas.DataFrame.to_html(): Converts the DataFrame to an HTML table string.
        - urllib.parse.quote(): URL-encodes the CSV content to embed in the download link.

        Inputs:
        - df: DataFrame containing the data to be converted into HTML and CSV formats.
        - name: String specifying the name for the downloadable CSV file.

        Returns:
        - A tuple containing two elements:
            1. An HTML string representing the DataFrame as a table.
            2. A string containing an HTML anchor tag (<a>) with a link to download the CSV file. 
            The file name is set to the 'name' parameter with a .csv extension.

        Example:
        #>>> data = {'Column1': [1, 2], 'Column2': [3.567, 4.891]}
        #>>> df = pd.DataFrame(data)
        #>>> html_table, csv_link = create_html_table_and_link(df, "SampleData")
        #>>> print(html_table)
        [HTML table string]
        #>>> print(csv_link)
        <a href='data:text/csv;charset=utf-8,...' download='SampleData.csv'>Download CSV (SampleData)</a>
        """
        # Round the DataFrame to two decimal places
        # Select only the numeric columns
    numeric_cols = df.select_dtypes(include='number')

    # Round and convert to integer
    #rounded_df = numeric_cols.round(0).astype(int) #set to this for gifts

    rounded_df = df.round(2)

    # Convert the rounded DataFrame to CSV
    csv_content = rounded_df.to_csv(index=False)
    
    # Convert the rounded DataFrame to HTML
    html_table = rounded_df.to_html(classes=['table'], index=False) if df is not None else ""
    
    # URL-encode the CSV content
    csv_encoded = urllib.parse.quote(csv_content)

    csv_link = f"<a href='data:text/csv;charset=utf-8,{csv_encoded}' download='{name}.csv'>Download CSV ({name})</a>"
    return html_table, csv_link

def extract_campaign_names(campaign_dict):

    """
        Extracts the names of campaigns from a dictionary where campaign names are used as keys. 
        This function simplifies the process of retrieving a list of campaign names from a dictionary 
        that contains campaign-related data.

        Inputs:
        - campaign_dict: Dictionary with campaign names as keys. The values can be any data type, 
        as they are not used in this function.

        Returns:
        - A list of strings, where each string is a campaign name extracted from the keys of the 
        input dictionary.

        Example:
        # >>> campaign_data = {"Campaign A": {...}, "Campaign B": {...}}
        # >>> campaign_names = extract_campaign_names(campaign_data)
        # >>> campaign_names
        ['Campaign A', 'Campaign B']
        """
    return list(campaign_dict.keys())

def transfer_file_to_sharepoint(base_context, relative_url, file_name, html_content):
    """
        Writes HTML content to a file on a temporary path and then transfers it to SharePoint. 
        The function first creates a file with the specified HTML content and then utilizes a 
        separate function to transfer this file to a specified location on SharePoint.

        Dependencies:
        - os.path.join(): Used to construct the file path in a platform-independent way.
        - open(): Used to create and write to the file.
        - transfer_from_cluster(base_context, relative_url, file_name): A function that handles 
        the transfer of the file from the cluster to SharePoint.

        Inputs:
        - base_context: The context or configuration needed for the transfer function.
        - relative_url: String specifying the relative URL in SharePoint where the file will be transferred.
        - file_name: String specifying the name of the file to be created and transferred.
        - html_content: String containing the HTML content to be written to the file.

        Returns:
        - The return value from the 'transfer_from_cluster' function, which typically indicates 
        the success or failure of the file transfer.

        Example:
        # >>> base_context = {...}
        # >>> relative_url = "/sites/documents/reports"
        # >>> file_name = "report.html"
        # >>> html_content = "<html><body><h1>Report</h1></body></html>"
        # >>> result = transfer_file_to_sharepoint(base_context, relative_url, file_name, html_content)
        # >>> result
        [Result of the file transfer]
        """
    file_path = os.path.join('/dbfs/tmp', file_name)
    with open(file_path, 'w') as f:
        f.write(html_content)
    return transfer_from_cluster(base_context, relative_url, file_name)

def create_dropdown_avg_response(campaign_types):
    """
        Creates a dropdown menu for a Plotly figure that allows users to select and display 
        average response curves for different campaign types. The function generates buttons 
        for each campaign type and configures their behavior to update the visibility of 
        corresponding traces in a Plotly figure.

        Dependencies:
        - go.layout.Updatemenu: A class from Plotly's graph_objects module used to create 
        a dropdown menu in a figure layout.

        Inputs:
        - campaign_types: List of strings, where each string is a campaign type. These types 
        are used to label the buttons in the dropdown menu.

        Returns:
        - A Plotly graph_objects Updatemenu object configured with buttons for each campaign 
        type. Each button, when clicked, updates the visibility of the corresponding average 
        response curves in the figure.

        Example:
        >>> campaign_types = ["Type A", "Type B", "Type C"]
        >>> dropdown_menu = create_dropdown_avg_response(campaign_types)
        >>> fig.update_layout(updatemenus=[dropdown_menu])
        [Here 'fig' is a Plotly figure to which the dropdown menu is added]
        """
    buttons = [
        {'label': campaign_type, 
         'method': 'update', 
         'args': [{'visible': ['Average' in trace.name and campaign_type in trace.name for trace in fig.data]}]}
        for campaign_type in campaign_types
    ]
    return go.layout.Updatemenu(
        type='dropdown',
        direction='down',
        x=0.15,
        y=1.12,
        showactive=True,
        buttons=buttons
    )



# def create_dropdown_individual_campaigns(day_diffs, paramDict, clientMap, fig, sc):
#     # Check if this client uses a non-standard ExtractionField, indicating an edge case
#     is_edge_case = clientMap.get('ExtractionField', 'CampaignCode') != 'CampaignCode'

#     # Prepare lists for categorized campaigns
#     acq_campaigns = []
#     app_campaigns = []

#     if is_edge_case:
#         # Edge case logic: Categorize based on information from 'sc'
#         for campaign_code in day_diffs:
#             # Extract campaign type from 'sc' using 'CampaignCode' and categorize
#             campaign_row = sc[sc['CampaignCode'] == campaign_code]
#             if not campaign_row.empty and 'Appeal' in campaign_row['CampaignName'].values[0]:
#                 app_campaigns.append(campaign_code)
#             elif not campaign_row.empty and 'Acquisition' in campaign_row['CampaignName'].values[0]:
#                 acq_campaigns.append(campaign_code)
#     else:
#         # Standard logic: Filter based on campaign prefixes
#         acq_campaigns = [c for c in day_diffs if c.startswith(tuple(clientMap['CampaignTypes']['Acquisitions'][0]))]
#         app_campaigns = [c for c in day_diffs if c.startswith(tuple(clientMap['CampaignTypes']['Appeals'][0]))]

#     # Function to create buttons for a set of campaigns
#     def create_buttons(campaigns, day_diffs):
#         return [
#             {
#                 'label': f"{campaign} ({day_diffs.get(campaign, 'N/A')} days)",
#                 'method': 'update',
#                 'args': [
#                     {'visible': [campaign == trace.name for trace in fig.data]},
#                     {'shapes': [{'type': 'line', 'x0': day_diffs.get(campaign, 0), 'y0': 0, 'x1': day_diffs.get(campaign, 0), 'y1': 1, 'xref': 'x', 'yref': 'paper'}]}
#                 ]
#             } for campaign in campaigns if campaign in day_diffs  # Ensure campaign has a valid day_diff
#         ]

#     # Create buttons for each type using the above function
#     acq_buttons = create_buttons(acq_campaigns, day_diffs)
#     app_buttons = create_buttons(app_campaigns, day_diffs)

#     # Create two separate dropdown menus using buttons
#     acq_dropdown = go.layout.Updatemenu(
#         buttons=acq_buttons,
#         direction='down',
#         x=0.55,
#         y=1.15,  # Adjust the position as needed
#         showactive=True,
#         name='Acquisition'
#     )

#     app_dropdown = go.layout.Updatemenu(
#         buttons=app_buttons,
#         direction='down',
#         x=0.55,
#         y=1.10,  # Adjust the position as needed
#         showactive=True,
#         name='Appeal'
#     )

#     # Return the constructed dropdown menus
#     return [acq_dropdown, app_dropdown]


# import plotly.graph_objects as go  # Ensure Plotly is imported if not already

# def create_dropdown_individual_campaigns(day_diffs, paramDict, clientMap, fig, sc):
#     # Check if this client uses a non-standard ExtractionField, indicating an edge case
#     is_edge_case = clientMap.get('ExtractionField', 'CampaignCode') != 'CampaignCode'

#     # Prepare lists for categorized campaigns
#     acq_campaigns = []
#     app_campaigns = []

#     if is_edge_case:
#         # Edge case logic: Categorize based on information from 'sc'
#         for campaign_code in day_diffs:
#             # Extract campaign type from 'sc' using 'CampaignCode' and categorize
#             campaign_row = sc[sc['CampaignCode'] == campaign_code]
#             if not campaign_row.empty and 'Appeal' in campaign_row['CampaignName'].values[0]:
#                 app_campaigns.append(campaign_code)
#             elif not campaign_row.empty and 'Acquisition' in campaign_row['CampaignName'].values[0]:
#                 acq_campaigns.append(campaign_code)
#     else:
#         # Standard logic: Filter based on campaign prefixes
#         acq_campaigns = [c for c in day_diffs if c.startswith(tuple(clientMap['CampaignTypes']['Acquisitions'][0]))]
#         app_campaigns = [c for c in day_diffs if c.startswith(tuple(clientMap['CampaignTypes']['Appeals'][0]))]

#     # Function to create buttons for a set of campaigns
#     def create_buttons(campaigns, day_diffs):
#         return [
#             {
#                 'label': f"{campaign} ({day_diffs.get(campaign, 'N/A')} days)", 
#                 'method': 'update', 
#                 'args': [
#                     {'visible': [i < len(paramDict) or campaign == trace.name for i, trace in enumerate(fig.data)]},  # Adjust visibility based on campaign match
#                     {'shapes': [{'type': 'line', 'x0': day_diffs.get(campaign, 0), 'y0': 0, 'x1': day_diffs.get(campaign, 0), 'y1': 1, 'xref': 'x', 'yref': 'paper'}]}
#                 ]
#             } for campaign in campaigns if campaign in day_diffs  # Ensure campaign has a valid day_diff
#         ]

#     # Create buttons for each type using the above function
#     acq_buttons = create_buttons(acq_campaigns, day_diffs)
#     app_buttons = create_buttons(app_campaigns, day_diffs)

#     # Create two separate dropdown menus using buttons
#     acq_dropdown = go.layout.Updatemenu(
#         buttons=acq_buttons,
#         direction='down',
#         x=0.55,
#         y=1.15,  # Adjust the position as needed
#         showactive=True,
#         bgcolor='lightblue',  # Optional: add button color
#         name='Acquisition'
#     )

#     app_dropdown = go.layout.Updatemenu(
#         buttons=app_buttons,
#         direction='down',
#         x=0.55,
#         y=1.10,  # Adjust the position as needed
#         showactive=True,
#         bgcolor='lightgreen',  # Optional: add button color
#         name='Appeal'
#     )

#     # Return the constructed dropdown menus
#     return [acq_dropdown, app_dropdown]




















def create_dropdown_individual_campaigns(day_diffs, paramDict, clientMap, fig):
    """
        Creates dropdown menus for a Plotly figure that allow users to select and display 
        individual campaigns, categorized as acquisitions and appeals, based on a specific client. 
        The function generates two dropdown menus, one for acquisitions and one for appeals, 
        each containing buttons for the respective campaign types. Button clicks update the 
        visibility of corresponding traces in the figure and display a vertical line indicating 
        the day difference for the selected campaign.

        Dependencies:
        - go.layout.Updatemenu: A class from Plotly's graph_objects module used to create 
        dropdown menus in a figure layout.

        Inputs:
        - day_diffs: Dictionary with campaign names as keys and the corresponding day differences 
        as values.
        - paramDict: Dictionary containing parameters related to the plot traces.
        - clientMap: Dictionary mapping client names to their data, including campaign types.
        - client_name: String specifying the client name to access the relevant campaign data.

        Returns:
        - A list containing two Plotly graph_objects Updatemenu objects, one for acquisitions 
        and one for appeals. Each dropdown menu is configured with buttons for individual 
        campaigns of the respective type.

        Example:
        # >>> day_diffs = {"Campaign A": 5, "Campaign B": 10}
        # >>> paramDict = {...}
        # >>> clientMap = {"Client1": {"CampaignTypes": {"Acquisitions": ["A"], "Appeals": ["B"]}}}
        # >>> client_name = "Client1"
        # >>> dropdown_menus = create_dropdown_individual_campaigns(day_diffs, paramDict, clientMap, client_name)
        # >>> fig.update_layout(updatemenus=dropdown_menus)
        [Here 'fig' is a Plotly figure to which the dropdown menus are added]
    """

    # Separate campaigns into acquisitions and appeals using the new structure of clientMap
    acq_campaigns = [c for c in day_diffs if c.startswith(tuple(clientMap['CampaignTypes']['Acquisitions'][0]))]
    app_campaigns = [c for c in day_diffs if c.startswith(tuple(clientMap['CampaignTypes']['Appeals'][0]))]

    # Function to create buttons for a set of campaigns
    def create_buttons(campaigns):
        return [
            {
                'label': f"{campaign} ({day_diffs[campaign]} days)", 
                'method': 'update', 
                'args': [
                    {'visible': [i < len(paramDict) or campaign == trace.name for i, trace in enumerate(fig.data)]},
                    {'shapes': [{'type': 'line', 'x0': day_diffs[campaign], 'y0': 0, 'x1': day_diffs[campaign], 'y1': 1, 'xref': 'x', 'yref': 'paper'}]}
                ]
            } for campaign in campaigns
        ]

    # Create buttons for each type
    acq_buttons = create_buttons(acq_campaigns)
    app_buttons = create_buttons(app_campaigns)

    # Create two separate dropdown menus
    acq_dropdown = go.layout.Updatemenu(
        type='dropdown',
        direction='down',
        x=0.55,
        y=1.15,  # Adjust the position as needed
        showactive=True,
        buttons=acq_buttons,
        name='Acquisition'
    )

    app_dropdown = go.layout.Updatemenu(
        type='dropdown',
        direction='down',
        x=0.55,
        y=1.10,  # Adjust the position as needed
        showactive=True,
        buttons=app_buttons,
        name='Appeal'
    )

    return [acq_dropdown, app_dropdown]

def finalize_and_show_plot(fig):
    """
        Applies final layout settings to a Plotly figure and displays it. This function is 
        designed to set the axis titles, axis ranges, tick values, and overall dimensions of 
        the figure. It also sets the hover mode for the plot. After updating the layout, the 
        function displays the plot in the output.

        Inputs:
        - fig: A Plotly figure object that needs final layout adjustments before being displayed.

        Operations:
        - Updates the layout of the figure with predefined settings for the x-axis and y-axis 
        titles, y-axis range and tick values, hover mode, and figure dimensions.
        - Displays the figure using Plotly's 'show' method.

        Example:
        # >>> fig = go.Figure(data=[go.Scatter(x=[1, 2, 3], y=[0.1, 0.3, 0.5])])
        # >>> finalize_and_show_plot(fig)
        [This will display the figure with the specified layout settings]
        """
    fig.update_layout(
        # Add your layout settings here
        xaxis=dict(title='Days Since Mail Date'),
        yaxis=dict(title='Percent Complete', range=[0.0, 1.0], tickvals=[0.0, 0.2, 0.4, 0.6, 0.8, 1.0], ticktext=["0%", "20%", "40%", "60%", "80%", "100%"]),
        hovermode='x unified',
        width=1100,
        height=900
    )
    fig.show()


# def calculate_day_differences(max_gift_dates, max_mail_dates):
#     day_diffs = {}
#     for campaign, gift_date in max_gift_dates.items():
#         if campaign in max_mail_dates and pd.notnull(gift_date) and pd.notnull(max_mail_dates[campaign]):
#             # Both dates are present and not NaT
#             gift_date = pd.to_datetime(gift_date)
#             mail_date = pd.to_datetime(max_mail_dates[campaign])
#             if not pd.isna(gift_date) and not pd.isna(mail_date):  # Additional check for safety
#                 day_diff = (gift_date - mail_date).days
#                 day_diffs[campaign] = day_diff  # Only add to day_diffs if both dates are valid
#     return day_diffs


def calculate_day_differences(max_gift_dates, max_mail_dates):
    """
            Calculates the differences in days between corresponding gift dates and mail dates 
            for each campaign. The function iterates over campaigns in the 'max_gift_dates' dictionary 
            and, if a matching campaign is found in 'max_mail_dates', computes the difference in days 
            between the gift date and the mail date.

            Dependencies:
            - datetime.timedelta.days: Used to extract the difference in days from a timedelta object.

            Inputs:
            - max_gift_dates: Dictionary with campaign names as keys and their respective latest gift 
            dates (datetime objects) as values.
            - max_mail_dates: Dictionary with campaign names as keys and their respective latest mail 
            dates (datetime objects) as values.

            Returns:
            - A dictionary where each key is a campaign name and the corresponding value is the 
            difference in days between the gift date and the mail date. Only campaigns present in both 
            'max_gift_dates' and 'max_mail_dates' are included in the output.

            Example:
            >>> max_gift_dates = {"Campaign A": datetime(2022, 1, 15), "Campaign B": datetime(2022, 2, 20)}
            >>> max_mail_dates = {"Campaign A": datetime(2022, 1, 10), "Campaign B": datetime(2022, 2, 15)}
            >>> day_diffs = calculate_day_differences(max_gift_dates, max_mail_dates)
            >>> day_diffs
            {'Campaign A': 5, 'Campaign B': 5}
            """
    day_diffs = {}
    for campaign in max_gift_dates:
        if campaign in max_mail_dates:
            gift_date = pd.to_datetime(max_gift_dates[campaign])
            mail_date = pd.to_datetime(max_mail_dates[campaign])
            day_diff = (gift_date - mail_date).days
            day_diffs[campaign] = day_diff
    return day_diffs

def create_acq_fig():
    """
        Creates and configures a Plotly figure for acquisition (ACQ) campaigns. This function 
        initializes a new Plotly figure, adds a trace representing the average ACQ campaign, 
        and applies layout settings specific to ACQ campaigns. The trace is generated using 
        the 'create_trace' function, and a dropdown menu is added for interaction.

        Dependencies:
        - go.Figure(): Used to create a new Plotly figure.
        - create_trace(): Used to generate the trace for the average ACQ campaign.
        - go.layout.Updatemenu: A class from Plotly's graph_objects module used to create 
        dropdown menus in a figure layout.

        Global Variables:
        - x_range: A predefined array or list of x-values for the trace.
        - paramDict: A dictionary containing parameters for the ACQ trace.
        - acq_dropdown: A predefined dropdown menu specifically for ACQ campaigns.

        Returns:
        - A Plotly figure object (fig_acq) configured with the ACQ campaign trace, layout 
        settings, and an ACQ-specific dropdown menu.

        Example:
        # >>> fig_acq = create_acq_fig()
        # >>> fig_acq.show()
        [This will display the ACQ campaign figure with the configured layout and trace]
        """

    fig_acq = go.Figure()
    fig_acq.add_trace(create_trace('Average ACQ', x_range, paramDict['ACQ']))
    fig_acq.update_layout(
      updatemenus=[acq_dropdown],
      width=1100,  # Width in pixels
      height=870,  # Height in pixels
      xaxis_title="Days Since Mail Date",
      yaxis_title="Percent Complete",
      yaxis_tickformat=".0%" ,
      margin=dict(l=5, r=5, b=5, t=5)  # Adjust margins to ensure axis labels are visible
    )
    return fig_acq

def create_app_fig():
    """
        Creates and configures a Plotly figure for appeal (APP) campaigns. This function 
        initializes a new Plotly figure, adds a trace representing the average APP campaign, 
        and applies layout settings specific to APP campaigns. The trace is generated using 
        the 'create_trace' function, and a dropdown menu is added for interaction.

        Dependencies:
        - go.Figure(): Used to create a new Plotly figure.
        - create_trace(): Used to generate the trace for the average APP campaign.
        - go.layout.Updatemenu: A class from Plotly's graph_objects module used to create 
        dropdown menus in a figure layout.

        Global Variables:
        - x_range: A predefined array or list of x-values for the trace.
        - paramDict: A dictionary containing parameters for the APP trace.
        - app_dropdown: A predefined dropdown menu specifically for APP campaigns.

        Returns:
        - A Plotly figure object (fig_app) configured with the APP campaign trace, layout 
        settings, and an APP-specific dropdown menu.

        Example:
        # >>> fig_app = create_app_fig()
        # >>> fig_app.show()
        [This will display the APP campaign figure with the configured layout and trace]
        """

    fig_app = go.Figure()
    fig_app.add_trace(create_trace('Average APP', x_range, paramDict['APP']))
    fig_app.update_layout(
      updatemenus=[app_dropdown],
      width=1100,  # Width in pixels
      height=870,  # Height in pixels
      xaxis_title="Days Since Mail Date",
      yaxis_title="Percent Complete",
      yaxis_tickformat=".0%" ,
      margin=dict(l=5, r=5, b=5, t=5)  # Adjust margins to ensure axis labels are visible
    )
    return fig_app
