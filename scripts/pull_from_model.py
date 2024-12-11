######
# This script pulls a number of variables from the reigonal model in Visum
#
######
# What does model need before running this?
#
#  Ensure shuttles throughout the region are included
#
#  Ensure the necessary skims have been run and exist
#    Number of Transfers (480 NTR)
#    Journey Time (490 JRT)
#    Journey Distance (100033 JRD) - need to run
#    Highway Time (290 TTC)  -tsys specific time interval in loaded network
#    Transit Distance (270 DIS)
#    Highway Volume (2000 HWY)
#    Transit Volume (2100 ToTotal)
#    Transfer Wait Time (100032 TWT) - need to run
#    Total Volume (100034 TotalVol_AllModes) - need to run
#
######

import pandas as pd
from sqlalchemy_utils import database_exists, create_database
import env_vars as ev
from env_vars import ENGINE, conn
import numpy
import VisumPy.helpers as h
import psycopg2 as psql
import os


#create database and enable postgis
if not database_exists(ENGINE.url):
    create_database(ENGINE.url)
ENGINE.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

#look for version files in run folder
runDir = r"D:\dvrpc_shared\NetworkGap_Directness\ModelRun\TIM251_2019_Full_Run"
TODs = ["AM", "MD", "PM", "NT"]

#append TOD to the file path
paths = []
for root, dirs, files in os.walk(runDir):
    for f in files: 
        if True in [TOD in f for TOD in TODs] and f.endswith(".ver"):
            paths.append(os.path.join(root, f))
            
print("pull data from model")

#create dictionaries to hold data
TOD_VolSums = {}
HWY_TOD_VolSums = {}

Visum = h.CreateVisum(23)
#open version files to gather data
for versionFilePath in paths:
    Visum.LoadVersion(versionFilePath)
    TOD = Visum.Net.AttValue("TOD")
    
    #get values from OD Pairs listing
    FromZone = h.GetMulti(Visum.Net.ODPairs,"FromZoneNo")
    ToZone = h.GetMulti(Visum.Net.ODPairs,"ToZoneNo")
    NumTransfers = h.GetMulti(Visum.Net.ODPairs,"MatValue(480 NTR)")
    JourneyTime = h.GetMulti(Visum.Net.ODPairs,"MatValue(490 JRT)")
    JourneyDist = h.GetMulti(Visum.Net.ODPairs,"MatValue(100033 JRD)")
    HwyTime = h.GetMulti(Visum.Net.ODPairs,"MatValue(290 TTC)") #tsys specific time interval in loaded network
    PrTDist = h.GetMulti(Visum.Net.ODPairs,"MatValue(270 DIS)")
    HwyVol = h.GetMulti(Visum.Net.ODPairs,"MatValue(2000 Highway)")
    TransitVol = h.GetMulti(Visum.Net.ODPairs,"MatValue(2100 ToTotal)")
    TransferWait = h.GetMulti(Visum.Net.ODPairs,"MatValue(100032 TWT)")
    TotalVol = h.GetMulti(Visum.Net.ODPairs,"MatValue(100034 TotalVol_AllModes)")
    
    #add to data frame
    df = pd.DataFrame({
        'FromZone':FromZone,
        'ToZone':ToZone,
        'NumTransfers':NumTransfers,
        'JourneyTime': JourneyTime,
        'JourneyDist': JourneyDist,
        'HwyTime':HwyTime,
        'PrTDist':PrTDist,
        'HwyVol':HwyVol,
        'TransitVol':TransitVol,
        'TransferWait':TransferWait,
        'TotalVol':TotalVol
    }
    )   
    

    
    # creating table
    # sql = '''CREATE TABLE (%s)(
    # FromZone numeric,
    # ToZone numeric,
    # NumTransfers numeric,
    # JourneyTime numeric,
    # JourneyDist numeric,
    # HwyTime numeric,
    # PrTDist numeric,
    # HwyVol numeric,
    # TransitVol numeric,
    # TransferWait numeric,
    # TotalVol numeric
    # )''', TOD
    
    #create Connection Score table in postgres
    df.to_sql(('(%s)',TOD), ENGINE, chunksize = 10000)
    
    #calculate total transit volume for time period
    TOD_Volume = h.GetMatrix(Visum, 2200)
    TOD_VolSum = TOD_Volume.sum()
    TOD_VolSums[TOD] = TOD_VolSum
    
    #calculate total highway volume for time period
    HWY_TOD_Volume = h.GetMatrix(Visum, 2000)
    HWY_TOD_VolSum = HWY_TOD_Volume.sum()
    HWY_TOD_VolSums[TOD] = HWY_TOD_VolSum
    
del NumTransfers
del JourneyTime 
del JourneyDist
del HwyTime 
del PrTDist 
del HwyVol
del TransitVol
del TransferWait


# creating a cursor object
cur = conn.cursor()
# Create a table if it doesn't exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS transit_vol_sum (
        tod TEXT,
        volume numeric
    )
""")

# Insert the dictionary into the table
for t in TODs:
    cur.execute("INSERT INTO transit_vol_sum (tod, volume) VALUES (%s, %s)", (t, TOD_VolSums[t]))

# Commit the transaction
conn.commit()

#repeat for highway volumes
cur.execute("""
    CREATE TABLE IF NOT EXISTS hwy_vol_sum (
        tod TEXT,
        volume numeric
    )
""")

for t in TODs:
    cur.execute("INSERT INTO hwy_vol_sum (tod, volume) VALUES (%s, %s)", (t, HWY_TOD_VolSums[t]))

conn.commit()

# Close the connection
cur.close()
conn.close()