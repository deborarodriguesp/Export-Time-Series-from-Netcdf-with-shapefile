##################################################################
#
#     Developed by: Débora Rodrigues
#     MARETEC (IST)
#     Date: 09/10/2023
#
##################################################################

import os
import glob
import netCDF4 as nc
import geopandas as gpd
import pandas as pd
import numpy as np
from datetime import datetime
import calendar
from tqdm import tqdm


nc_dir = 'your/netcdf/path/'
shapefile_path = 'your/shapefile/path/'

gdf = gpd.read_file(shapefile_path)

# Initialize lists to store mean values
daily_means = []
monthly_means = []
annual_means = []
normal_climate_means = []

netcdf_files = glob.glob(os.path.join(nc_dir, '*.nc'))[731:]

progress_bar = tqdm(total=len(netcdf_files), desc='Processing NetCDF Files')

for nc_file in netcdf_files:
    # Extract the date from the filename
    filename = os.path.basename(nc_file)
    date_str = filename.split('.')[0]  # Remove the ".nc" extension
    date = datetime.strptime(date_str, '%Y-%m-%d') # this depends on your data

    # Open the netCDF file
    with nc.Dataset(nc_file) as ds:
        # Extract data variable from netCDF
        variable = ds['tp']  # Replace 'your_variable_name' with the actual variable name in your netCDF file

        # Mask the data to the geometry of the shapefile
        data = variable[:].filled(np.nan)
        masked_data = np.where(gdf.geometry, data, np.nan)
        
        # Calculate the mean value
        accumulated_by_cell = np.nansum(masked_data, axis=0)
               
        mean_value = np.nanmean(accumulated_by_cell)       
        
    # Calculate daily mean
    daily_means.append({'Date': date, 'MeanValue': mean_value*1000})
    
    # Update the progress bar
    progress_bar.update(1)

# Close the progress bar
progress_bar.close()
    
# Create a DataFrame from the list of daily mean values
daily_df = pd.DataFrame(daily_means)
daily_df.to_csv('daily_means.csv', index=False)

# Calculate monthly mean values
daily_df['Month'] = daily_df['Date'].dt.month
daily_df['Year'] = daily_df['Date'].dt.year
monthly_means = daily_df.groupby(['Year', 'Month'])['MeanValue'].sum().reset_index()
monthly_means.to_csv('monthly_means.csv', index=False)

# Calculate annual mean values
daily_df['Year'] = daily_df['Date'].dt.year
annual_means = daily_df.groupby(['Year'])['MeanValue'].sum().reset_index()
annual_means.to_csv('annual_means.csv')

# Calculate normal climate mean values (e.g., 30-year climatology)
start_year = daily_df['Year'].min()
end_year = daily_df['Year'].max()
normal_climate_means = daily_df[(daily_df['Year'] >= start_year) & (daily_df['Year'] <= end_year)].groupby(['Month'])['MeanValue'].sum().reset_index()
normal_climate_means.to_csv('normal_climate_means.csv', index=False)
