import cdsapi
import os
import gzip
import shutil
from datetime import datetime

def download_era5_precipitation(latitude, longitude, start_date, end_date, output_dir='era5_data'):
    """
    Download ERA5 precipitation data for a specific location and time range.
    
    Parameters:
    -----------
    latitude : float
        Latitude of the location
    longitude : float
        Longitude of the location
    start_date : str
        Start date in format 'YYYY-MM-DD'
    end_date : str
        End date in format 'YYYY-MM-DD'
    output_dir : str
        Directory to save the output files (default: 'era5_data')
    
    Returns:
    --------
    str : Path to the final unzipped CSV file
    """
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Format the filename with lat, lon, and date range
    lat_str = f"{abs(latitude):.2f}{'N' if latitude >= 0 else 'S'}"
    lon_str = f"{abs(longitude):.2f}{'E' if longitude >= 0 else 'W'}"
    date_range_str = f"{start_date.replace('-', '')}_{end_date.replace('-', '')}"
    
    base_filename = f"ERA5_precip_lat{lat_str}_lon{lon_str}_{date_range_str}"
    
    # Temporary download file
    temp_download = os.path.join(output_dir, f"{base_filename}_temp.csv")
    
    # Final output file
    final_output = os.path.join(output_dir, f"{base_filename}.csv")
    
    print(f"Downloading ERA5 precipitation data...")
    print(f"Location: Lat={latitude}, Lon={longitude}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Output file: {final_output}")
    
    # Set up the API request
    dataset = "reanalysis-era5-single-levels-timeseries"
    request = {
        "variable": ["total_precipitation"],
        "location": {"longitude": longitude, "latitude": latitude},
        "date": [f"{start_date}/{end_date}"],
        "data_format": "csv"
    }
    
    # Download the data
    try:
        client = cdsapi.Client()
        client.retrieve(dataset, request).download(temp_download)
        print(f"Download completed: {temp_download}")
        
        # Check if the file is gzipped
        if temp_download.endswith('.gz') or is_gzipped(temp_download):
            print("File is gzipped. Unzipping...")
            unzipped_file = temp_download.replace('.gz', '') if temp_download.endswith('.gz') else temp_download + '_unzipped'
            
            with gzip.open(temp_download, 'rb') as f_in:
                with open(unzipped_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Remove the gzipped file and rename unzipped file
            os.remove(temp_download)
            os.rename(unzipped_file, final_output)
            print(f"File unzipped and saved as: {final_output}")
        else:
            # If not gzipped, just rename
            os.rename(temp_download, final_output)
            print(f"File saved as: {final_output}")
        
        # Add metadata header to the file
        add_metadata_header(final_output, latitude, longitude, start_date, end_date)
        
        print("Process completed successfully!")
        return final_output
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        # Clean up temporary files if they exist
        if os.path.exists(temp_download):
            os.remove(temp_download)
        raise

def is_gzipped(filepath):
    """
    Check if a file is gzipped by reading its magic number.
    """
    try:
        with open(filepath, 'rb') as f:
            return f.read(2) == b'\x1f\x8b'
    except:
        return False

def add_metadata_header(filepath, latitude, longitude, start_date, end_date):
    """
    Add metadata as comments at the beginning of the CSV file.
    """
    # Read the existing content
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Create metadata header
    metadata = f"""# ERA5 Reanalysis - Total Precipitation Data
# Downloaded: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Location: Latitude={latitude}, Longitude={longitude}
# Date Range: {start_date} to {end_date}
# Data Source: ERA5 Single Levels Timeseries
# Variable: Total Precipitation
# ---
"""
    
    # Write metadata followed by original content
    with open(filepath, 'w') as f:
        f.write(metadata + content)

# Example usage
if __name__ == "__main__":
    # Define parameters
    latitude = 44.5
    longitude = -117
    start_date = "2026-01-01"
    end_date = "2026-02-27"
    
    # Download and process the data
    output_file = download_era5_precipitation(
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
        output_dir='era5_data'
    )
    
    print(f"\nFinal output file: {output_file}")
    
    # Optional: Load and display first few lines
    print("\nFirst few lines of the data:")
    with open(output_file, 'r') as f:
        for i, line in enumerate(f):
            if i < 15:  # Show first 15 lines including metadata
                print(line.rstrip())
            else:
                break