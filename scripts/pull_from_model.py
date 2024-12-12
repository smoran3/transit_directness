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
from env_vars import ENGINE
import numpy
import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the current script's directory
visum_dir = os.path.join(script_dir, 'VisumPy')  # Adjust as needed
sys.path.append(visum_dir)

from VisumPy import helpers as h
import psycopg2 as psql
import sqlalchemy
from sqlalchemy import text



#create database and enable postgis
if not database_exists(ENGINE.url):
    create_database(ENGINE.url)
conn = ENGINE.connect()
Q = "CREATE EXTENSION IF NOT EXISTS postgis;"
conn.execute(text(Q))


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

def insert_to_pg(list,name,tod):
    df = pd.DataFrame(list)
    n = name+'_'+tod
    df.to_sql(n, ENGINE, chunksize = 10000)


Visum = h.CreateVisum(23)
#open version files to gather data
for versionFilePath in paths:
    Visum.LoadVersion(versionFilePath)
    TOD = Visum.Net.AttValue("TOD")
    print(TOD)
    
    #get values from OD Pairs listing
    print("FromZone")
    FromZone = h.GetMulti(Visum.Net.ODPairs,"FromZoneNo")
    insert_to_pg(FromZone, 'FromZone', TOD)
    del FromZone

    print("ToZone")
    ToZone = h.GetMulti(Visum.Net.ODPairs,"ToZoneNo")
    insert_to_pg(ToZone, 'ToZone', TOD)
    del ToZone

    print("NumTransfers")
    NumTransfers = h.GetMulti(Visum.Net.ODPairs,"MatValue(480 NTR)")
    insert_to_pg(NumTransfers, 'NumTransfers', TOD)
    del NumTransfers

    print("JourneyTime")
    JourneyTime = h.GetMulti(Visum.Net.ODPairs,"MatValue(490 JRT)")
    insert_to_pg(JourneyTime, 'JourneyTime', TOD)
    del JourneyTime

    print("JourneyDist")
    JourneyDist = h.GetMulti(Visum.Net.ODPairs,"MatValue(100033 JRD)")
    insert_to_pg(JourneyDist, 'JourneyDist', TOD)
    del JourneyDist

    print("HwyTime")
    HwyTime = h.GetMulti(Visum.Net.ODPairs,"MatValue(290 TTC)") #tsys specific time interval in loaded network
    insert_to_pg(HwyTime, 'HwyTime', TOD)
    del HwyTime

    print("PrTDist")
    PrTDist = h.GetMulti(Visum.Net.ODPairs,"MatValue(270 DIS)")
    insert_to_pg(PrTDist, 'PrTDist', TOD)
    del PrTDist

    print("HwyVol")
    HwyVol = h.GetMulti(Visum.Net.ODPairs,"MatValue(2000 Highway)")
    insert_to_pg(HwyVol, 'HwyVol', TOD)
    del HwyVol

    print("TransitVol")
    TransitVol = h.GetMulti(Visum.Net.ODPairs,"MatValue(2100 ToTotal)")
    insert_to_pg(TransitVol, 'TransitVol', TOD)
    del TransitVol

    print("TransferWait")
    TransferWait = h.GetMulti(Visum.Net.ODPairs,"MatValue(100032 TWT)")
    insert_to_pg(TransferWait, 'TransferWait', TOD)
    del TransferWait

    print("TotalVol")
    TotalVol = h.GetMulti(Visum.Net.ODPairs,"MatValue(100034 TotalVol_AllModes)")
    insert_to_pg(TotalVol, 'TotalVol', TOD)
    del TotalVol


    
    #calculate total transit volume for time period
    TOD_Volume = h.GetMatrix(Visum, 2200)
    TOD_VolSum = TOD_Volume.sum()
    TOD_VolSums[TOD] = TOD_VolSum
    
    #calculate total highway volume for time period
    HWY_TOD_Volume = h.GetMatrix(Visum, 2000)
    HWY_TOD_VolSum = HWY_TOD_Volume.sum()
    HWY_TOD_VolSums[TOD] = HWY_TOD_VolSum
    



# creating a cursor object
cur = ENGINE.cursor()
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
ENGINE.commit()

#repeat for highway volumes
cur.execute("""
    CREATE TABLE IF NOT EXISTS hwy_vol_sum (
        tod TEXT,
        volume numeric
    )
""")

for t in TODs:
    cur.execute("INSERT INTO hwy_vol_sum (tod, volume) VALUES (%s, %s)", (t, HWY_TOD_VolSums[t]))

ENGINE.commit()

# Close the connection
cur.close()
ENGINE.close()
