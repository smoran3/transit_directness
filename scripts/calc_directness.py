import pandas as pd
from sqlalchemy_utils import database_exists, create_database
import env_vars as ev
from env_vars import ENGINE, conn
import numpy
import sys
import os
import psycopg2 as psql
import sqlalchemy
from sqlalchemy import text

### get data from Postgres DB

#create dictionaries to hold data
Transfers = {}
Journeys = {}
JourTime = {}
CarDist = {}
CarTime = {}
PrTvol = {}
PuTvol = {}
TrWait = {}

TOD_VolSums = {}
HWY_TOD_VolSums = {}

TODs = ["AM", "MD", "PM", "NT"]

# connect to DB (sqlalchemy)
con = ENGINE.connect()


#pull fromzone and tozone into lists to reference
print('pull from zone numbers')
FromZone = list(con.execute(text('Select "0" From "FromZone_AM";')))
ToZone = list(con.execute(text('Select "0" From "ToZone_AM";')))

cur = conn.cursor()

#pull matrix values into dictionaries
def db_to_dictionary(TOD, tablename, dictname):
    table = tablename+'_'+TOD
    print(table)
    q = f'Select "0" from "{table}";'
    cur.execute(q)
    rows = cur.fetchall()
    dictname[TOD] = rows


for tod in TODs:
    db_to_dictionary(tod, "NumTransfers", Transfers)
    db_to_dictionary(tod, "JourneyDist", Journeys)
    db_to_dictionary(tod, "JourneyTime", JourTime)
    db_to_dictionary(tod, "PrTDist", CarDist)
    db_to_dictionary(tod, "HwyTime", CarTime)
    db_to_dictionary(tod, "HwyVol", PrTvol)
    db_to_dictionary(tod, "TransitVol", PuTvol)
    db_to_dictionary(tod, "TransferWait", TrWait)

def volsum_to_dict(table, vols):
    q = f'Select "0", "1" from {table};'
    cur.execute(q)
    result = cur.fetchall()
    vols.update(dict(result))

print('Sending volume sums to dictionary')
volsum_to_dict("transit_vol_sum", TOD_VolSums)
print(TOD_VolSums)
volsum_to_dict("hwy_vol_sum", HWY_TOD_VolSums)
print(HWY_TOD_VolSums)




#calculate sum of transit volume
TotTransitVol = 0

print("calculating total transit volume")

for TOD in TOD_VolSums:
    print(TOD, TOD_VolSums[TOD])
    TotTransitVol += TOD_VolSums[TOD]
#print to test
print(TotTransitVol)

#calculate sum of highway volume
TotHwyVol = 0

print("calculating total highway volume")

for TOD in HWY_TOD_VolSums:
    print(TOD, HWY_TOD_VolSums[TOD])
    TotHwyVol += HWY_TOD_VolSums[TOD]
#print to test
print(TotHwyVol)

print("calculate TOD weighted averages")

#calculate average and volume weighted averages for Number of Transfers
#create lists to hold counts, sums, and the average
NTR_counts = []
NTR_w_sums = []
NTR_w_avg = []
NTR_volsum = []

#iterate through all the items in each list of TOD NTR
print("step one")
for i in range(0, len(Transfers["AM"])):
    count = 0.0
    w_sum = 0.0
    volsum = 0.0
    #iterate through the TODs
    for key in Transfers:
        #see if there is a path between O and D (JRD <> 999999)
        #threshold is set lower because of effects of volume weighting in JRD skimming calculations
        if Transfers[key][i] < 166665:
            count += 1
            w_sum += (Transfers[key][i] * TOD_VolSums[key])
            volsum += (TOD_VolSums[key])
    NTR_counts.append(count)
    NTR_w_sums.append(w_sum)
    NTR_volsum.append(volsum)

        
print ("step two")
for i in range(0, len(NTR_counts)):
    if NTR_counts[i] == 0:
        NTR_w_avg.append(-1)
    else:
        NTR_w_avg.append(NTR_w_sums[i]/NTR_volsum[i])
        
del NTR_counts
del NTR_w_sums

#repeat for Journey Distance
#create lists to hold counts, sums, and the average
JRD_counts = []
JRD_w_sums = []
JRD_w_avg = []
JRD_volsum = []

#iterate through all the items in each list of TOD JRD
for i in range(0, len(Journeys["AM"])):
    count = 0.0
    w_sum = 0.0  
    volsum = 0.0
    #iterate through the TODs
    for key in Journeys:
        #see if there is a path between O and D (JRD <> 999999)
        #threshold is set lower because of effects of volume weighting in JRD skimming calculations
        if Journeys[key][i] < 166665:
            count += 1
            w_sum += (Journeys[key][i] * TOD_VolSums[key])
            volsum += (TOD_VolSums[key])
    JRD_counts.append(count)
    JRD_w_sums.append(w_sum)
    JRD_volsum.append(volsum)
    
for i in range(0, len(JRD_counts)):
    if JRD_counts[i] == 0:
        JRD_w_avg.append(0)
    else:
        JRD_w_avg.append(JRD_w_sums[i]/JRD_volsum[i])
        
del JRD_counts
del JRD_w_sums

#repeat for Journey Time
#create lists to hold counts, sums, and the average
JRT_counts = []
JRT_w_sums = []
JRT_w_avg = []
JRT_volsum = []

#iterate through all the items in each list of TOD JRT
for i in range(0, len(JourTime["AM"])):
    count = 0.0
    w_sum = 0.0  
    volsum = 0.0
    #iterate through the TODs
    for key in JourTime:
        #see if there is a path between O and D (JRT <> 999999)
        #threshold is set lower because of effects of volume weighting in JRT skimming calculations
        if JourTime[key][i] < 166665:
            count += 1
            w_sum += (JourTime[key][i] * TOD_VolSums[key])
            volsum += TOD_VolSums[key]
    JRT_counts.append(count)
    JRT_w_sums.append(w_sum)
    JRT_volsum.append(volsum)
    
for i in range(0, len(JRT_counts)):
    if JRT_counts[i] == 0:
        JRT_w_avg.append(0)
    else:
        JRT_w_avg.append(JRT_w_sums[i]/JRT_volsum[i])
        
del JRT_counts
del JRT_w_sums

#repeat for Highway Distance
#create lists to hold counts, sums, and the average
HWY_counts = []
HWY_w_sums = []
HWY_w_avg = []
HWY_volsum = []

#iterate through all the items in each list of TOD JRD
for i in range(0, len(CarDist["AM"])):
    count = 0.0
    w_sum = 0.0
    volsum = 0.0
    #iterate through the TODs
    for key in CarDist:
        count += 1
        w_sum += (CarDist[key][i] * HWY_TOD_VolSums[key])
        volsum += HWY_TOD_VolSums[key]
    HWY_counts.append(count)
    HWY_w_sums.append(w_sum)
    HWY_volsum.append(volsum)
    
for i in range(0, len(HWY_counts)):
    HWY_w_avg.append(HWY_w_sums[i]/HWY_volsum[i])
    
del HWY_counts
del HWY_w_sums

#repeat for Highway Time
#create lists to hold counts, sums, and the average
HwyT_counts = []
HwyT_w_sums = []
HwyT_w_avg = []
HwyT_volsum = []

#iterate through all the items in each list of TOD JRD
for i in range(0, len(CarTime["AM"])):
    count = 0.0
    w_sum = 0.0
    volsum = 0.0
    #iterate through the TODs
    for key in CarTime:
        count += 1
        w_sum += (CarTime[key][i] * HWY_TOD_VolSums[key])
        volsum += HWY_TOD_VolSums[key]
    HwyT_counts.append(count)
    HwyT_w_sums.append(w_sum)
    HwyT_volsum.append(volsum)
    
for i in range(0, len(HwyT_counts)):
    HwyT_w_avg.append(HwyT_w_sums[i]/HwyT_volsum[i])
    
del HwyT_counts
del HwyT_w_sums

#repeat for Transfer Wait Times
#create lists to hold counts, sums, and the average
TWT_counts = []
TWT_w_sums = []
TWT_w_avg = []
TWT_volsum = []

#iterate through all the items in each list of TOD JRD
for i in range(0, len(TrWait["AM"])):
    count = 0.0
    w_sum = 0.0
    volsum = 0.0
    #iterate through the TODs
    for key in TrWait:
        if TrWait[key][i] < 166665:
            count += 1
            w_sum += (TrWait[key][i] * TOD_VolSums[key])
            volsum += TOD_VolSums[key]
    TWT_counts.append(count)
    TWT_w_sums.append(w_sum)
    TWT_volsum.append(volsum)
    
for i in range(0, len(TWT_counts)):
    if TWT_counts[i] == 0:
        # leave open possiblity of 0 wait time; filter out later
        TWT_w_avg.append(-1)
    else:
        TWT_w_avg.append(TWT_w_sums[i]/TWT_volsum[i])
        
del TWT_counts
del TWT_w_sums

#update NTR based on valid JRD
#if JRD is valid, there is a connection. however, NTR might be invalid (999999) because you can't make the whole trip witihin the 2 hour assignment time interval
#changing the NTR to 4 reflects the fact that the number of transfers is prohibitively high
#make a copy of the list
NTR_copy = list(NTR_w_avg)
           
origneg = 0
changed = 0
for i in range(0, len(NTR_copy)):
    if NTR_copy[i] == -1:
        origneg += 1
        if JRD_w_avg[i] > 0:
            changed += 1
            NTR_copy[i] = 4
            
print (origneg)
print (changed)

#then, in change 'valid connection' criteria later in dataframe


print ("transfer and directness criteria")

#flag OD pairs with 1 or more transfer required
print ("Transfer Flag")
TransferFlag = []
for i in range(0, len(NTR_copy)):
    #is a transfer required?
    if NTR_copy[i] >= 1:
        #yes = 1
        TransferFlag.append(1)
    else:
        #no = 0
        TransferFlag.append(0)
        
#flag OD pairs where transit distance is greater than highway distance
print ("Distance Flag")
DistanceFlag = []
for i in range(0, len(JRD_w_avg)):
    if JRD_w_avg[i] > HWY_w_avg[i]:
        DistanceFlag.append(1) #yes
    else:
        DistanceFlag.append(0) #no
        
#flag OD pairs where transit time is greater than highway time
print ("Time Flag")
TimeFlag = []
for i in range(0, len(JRT_w_avg)):
    if JRT_w_avg[i] > HwyT_w_avg[i]:
        TimeFlag.append(1) #yes
    else:
        TimeFlag.append(0) #no
        
#criteria for points for number of transfers
print ("Transfer Point")
TransferPoint = []
for i in range(0, len(NTR_copy)):
    #how many transfers?
    if NTR_copy[i] <= 1:
        #1 or fewer = 0
        TransferPoint.append(0)
    elif NTR_copy[i] >1:
        #2 or more = 1
        TransferPoint.append(1)
        
#check
print (len(DistanceFlag))
print (len(TimeFlag))
print (TransferPoint[200])
print (TransferPoint[300])

#criteria for TWT point
#updated 5/7/19 based on SEPTA feedback re: transfer penaltys
print ("TWT Point")
TWTPoint = []
#what is schedule transfer wait time?
for i in range(0, len(TWT_w_avg)):
    #if one transfer and wait time is equal to or over 10 minutes, 1 point
    if (NTR_copy[i] >= 1 and NTR_copy[i] < 2 and TWT_w_avg[i] >= 10):
        TWTPoint.append(1)
    #if one transfer and wait time is less than 10 minutes, 0 points
    elif (NTR_copy[i] >= 1 and NTR_copy[i] < 2 and TWT_w_avg[i] < 10):
        TWTPoint.append(0)
    #if 2 or more transfers and wait time is equal to or over 20 minutes, 2 points
    elif (NTR_copy[i] >= 2 and TWT_w_avg[i] >= 20):
        TWTPoint.append(2)
    #if 2 or more transfers and wait time is less than 20 minutes, 0 points
    elif (NTR_copy[i] >= 2 and TWT_w_avg[i] < 20):
        TWTPoint.append(0)
    #Transfer < 1
    else: 
        TWTPoint.append(0)   
        
#sum points for connection score
ConnectionScore = []
for i in range(0, len(FromZone)):
    x = DistanceFlag[i] + TimeFlag[i] + TransferPoint[i] + TWTPoint[i]
    ConnectionScore.append(x)
    
print ("creating data frame") 
    
#create dataframe from these lists
df = pd.DataFrame(
    {'FromZone': FromZone,
     'ToZone':   ToZone,
     'NumTransfers': NTR_copy,
     'TrWait': TWT_w_avg,
     'AvgDist_Hwy': HWY_w_avg,
     'AvgDist_JRD': JRD_w_avg,
     'AvgTime_JRT': JRT_w_avg,
     'AvgTime_CAR': HwyT_w_avg,
     'DistanceFlag': DistanceFlag,
     'TimeFlag': TimeFlag,
     'TransferPoint': TransferPoint,
     'TWTPoint': TWTPoint,
     'ConnectionScore': ConnectionScore
    })
    
    
#filter for zones only within the region
FromRegion = df['FromZone'] < 50000
ToRegion = df['ToZone'] < 50000

#create DF with just Region Zones
RegionDF = pd.DataFrame(df[FromRegion & ToRegion])

#filter for OD pairs with a valid connection or no connection
ValidConnectionJ = RegionDF['AvgDist_JRD'] > 0
NoConnection = RegionDF['AvgDist_JRD'] == 0

ValidRegionDF = pd.DataFrame(RegionDF[ValidConnectionJ])

NoConRegionDF = pd.DataFrame(RegionDF[NoConnection])

print ("assigning no connection score")

#overwrite connection score in no connection table
NoConRegionDF['ConnectionScore'] = 6

#update the parent DF with the child df (Nope) and test to make sure it updated
RegionDF.update(NoConRegionDF)

del NoConRegionDF

#update full DF to then select columns to add back into Visum as UDAs
df.update(RegionDF)

#delete dictionaries
del Transfers
del Journeys
del JourTime
del CarDist
del CarTime
del PrTvol
del PuTvol
del TrWait

SubRegionDF = RegionDF.loc[:,['FromZone', 'ToZone', 'NumTransfers', 'TrWait', 'DistanceFlag', 'TimeFlag', 'TransferPoint', 'TWTPoint','ConnectionScore']]

print ("importing connection score table")

#create Connection Score table in postgres
SubRegionDF.to_sql('ConnectionScore', ENGINE, chunksize = 10000)
