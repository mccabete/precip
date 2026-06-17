import xarray as xr
import earthaccess
import boto3
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import warnings
from IPython.display import display, Markdown
import pandas as pd
import geopandas as gpd
import rasterio
import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import os
import numpy as np



def read_in_fires_and_precip():
    '''
    Helper function that reads in: 
    Zebs's MBTS matched database
    The 12-hour Precipiration IMERG dataset
    The subset of fires from Zeb that has final fire size
    The GACCs 
    This does *not* read in the fire name file. 

    The helper function then does a bunch of temporal alignment, merging, and creation of consistent varibles (ie precip diff) that depend on specific structures. 
    
    '''

    ### Zeb's fire data
    fires = pd.read_parquet("s3://maap-ops-workspace/shared/zbecker/TESS_fire_spread/sigdeltas_Tess.parq")
    subset_fires = gpd.read_parquet("s3://maap-ops-workspace/shared/zbecker/YANG/large_feds_faf_double_matched.parq")
    subset_fires = subset_fires.to_crs(4326)

    subset_fires["centroid"] = subset_fires.to_crs(4326).centroid
    fires["UfireID"] = fires.mergeid.astype("int").astype("str") + "_" + fires.year.astype("str")
    subset_fires["UfireID"] = subset_fires.mergeid.astype("str") + "_" + subset_fires.year.astype("str")
    subset_fires["polygon"] = subset_fires.geometry
    fires = fires[fires.UfireID.isin(subset_fires[subset_fires.intersectsMTBS == True].UfireID)]
    #fname = pd.read_csv("s3://maap-ops-workspace/shared/zbecker/Eli_MTBS_vs_FEDS/v6_output.csv")

    fires = fires.merge(subset_fires[['UfireID', 'centroid', 'polygon']], on = 'UfireID' )
    fires = gpd.GeoDataFrame(fires, geometry = 'polygon')
    fires_sm = fires.groupby("UfireID").apply(get_st_sp_fire).reset_index(drop = True)
    fires_sm["stable_index"] = fires_sm.index

    ### precipitation
    precip = pd.read_parquet(os.path.abspath("IMERG/half_hourly_IMERG_precip"))
    precip = precip.merge(fires_sm[["UfireID",	"centroid"]], on = "UfireID")
    precip.loc[:, "lon"] = precip.centroid.apply(lambda p: p.x)

    ## fixing the weird UTC binning of precip
    precip.loc[:, "offset_hour"] = (precip.lon/15)

    precip.loc[:, "time_lst"] = precip.time_utc.astype("datetime64[ns]") + pd.to_timedelta(precip["offset_hour"], unit='h') #lst_to_utc_offset_hours =  lon/ 15.0
    pm_mask = (precip.time_lst.dt.hour > 6) & (precip.time_lst.dt.hour <= 18) ## correcting to PM 13:30 overpass
    am_mask = (precip.time_lst.dt.hour <= 6) | ((precip.time_lst.dt.hour >= 18))## correcting to AM 1:30 overpass. This is actually an exact  number not a range bc we calcuated it for the extraction. 
    precip.loc[pm_mask, "t"] = precip.loc[pm_mask, "time_lst"].dt.normalize() + pd.Timedelta(hours=12)
    precip.loc[am_mask, "t"] = precip.loc[am_mask, "time_lst"].dt.normalize() + pd.Timedelta(hours=0)

    ## Finding days_since_t for easier indexing (ie 3 days since start etc)
    fires.t = fires.t.astype('datetime64[ns]')
    fires = fires.merge(precip[[ 'precipitation', 'UfireID', 't']], how = 'outer', on = ['UfireID', 't'])
    fires = fires.merge(fires_sm[['UfireID', 'start_time', 'end_time','end_time_plus']], on = 'UfireID')
    fires = fires.merge(subset_fires[['n_pixels', 'n_newpixels',
       'farea', 'duration', 'pixden', 'meanFRP',  'UfireID']], on = 'UfireID')

    ## adding the GACCS
    gsp = gpd.read_file("/home/jovyan/Preparedness_level/GACC_borders/National_GACC_Boundaries.shp")
    fires = fires.groupby("UfireID").apply(per_id_constants).reset_index(drop = True) ## need the centroids and things on a per id basis to avoid being droped by merge / make more interpretable 
    gsp = gsp.to_crs("EPSG:4326")
    gsp = gsp.dissolve(by='GACCAbbrev').reset_index()
    fires = fires.sjoin(gsp[['GACCName', 'GACCAbbrev','geometry']], how = "left")
    fires = fires.groupby("UfireID").apply(per_id_constants, cols = ["year", "l1_ecoregion", 	"centroid", "farea", 'duration', 'GACCName', 'GACCAbbrev']).reset_index(drop = True) # help with interpretation/ plotting by varible
    
    ## Calculating time offset from ending
    fires['end_time_offset'] =  fires.t.astype("datetime64[ns]") - fires.end_time.astype("datetime64[ns]") # Positive means days past the end date
    fires['start_time_offset'] = fires.t.astype("datetime64[ns]") - fires.start_time.astype("datetime64[ns]") # Positive means days past the start date, negative means before
    fires['start_off_12hrs'] = fires['start_time_offset'] / pd.Timedelta(hours=12) 
    fires['end_off_12hrs'] = fires['end_time_offset'] / pd.Timedelta(hours=12)

    ## add some status helper columns
    af_mask =  (fires['start_off_12hrs'] >= 0) & (fires['end_off_12hrs'] <= 0)

    pre_fire_mask = fires.start_off_12hrs == -1 ## conservative, next 12 hours, could expand to 24 because of differences in detection effecientcy
    post_fire_mask = fires.end_off_12hrs == 1
    mulitday = fires.duration > 0 
    fires.loc[pre_fire_mask, "fr_active_status"] = "pre_fire"
    fires.loc[post_fire_mask, "fr_active_status"] = "post_fire"
    fires.loc[af_mask, "fr_active_status"] = "during_fire"
    fires.loc[af_mask & (fires.area_growth_at_t_km2.isna()), "area_growth_at_t_km2"] = 0
    fires.loc[fires.end_time.astype("datetime64[ns]").dt.hour == 12, "AMPM_end"] = True
    fires.loc[fires.end_time.astype("datetime64[ns]").dt.hour == 0 , "AMPM_end"] = False

    ## some log convinence stuff
    fires.loc[:, "precipitation_no_zero"] = fires["precipitation"] + 1
    fires.loc[:, "area_growth_at_t_km2_no_zero"] = fires["area_growth_at_t_km2"] + 1

    ### calculate precipitation differences
    fires.t = fires.t.astype("datetime64[ns]")
    fires = fires.sort_values(by = ["UfireID", "t"])
    fires["precip_diff"] = fires.groupby("UfireID").precipitation.diff() ## Positive is when the amount of rain goes up, negative down
    return(fires)



def get_st_sp_fire(df, days_after = 7):
    df.loc[:, "start_time"] = df.t.min()
    df.loc[:, "end_time"] = df.t.max()
    df.loc[:, "end_time_plus"] = df.t.astype("datetime64[ns]").max()  + datetime.timedelta(days = days_after)
    df = df.loc[df.t == df.t.max(), :]
    return(df)

def per_id_constants(df, cols = ["year", "l1_ecoregion", 	"centroid"]):
    for col in cols:
        col_val = df.loc[~df[col].isna(), col].iloc[0]
        df.loc[:, col] = col_val
    return(df)