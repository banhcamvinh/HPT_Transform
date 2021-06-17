import json
import pandas as pd
import os
import glob
import re
import datetime
import pyodbc
from fast_to_sql import fast_to_sql as fts

# connect to database
server = 'LAPTOP-N94DSRJ8\VINHSEVER' 
database = 'HPT_BT2' 
conn = pyodbc.connect('Driver={SQL Server};Server='+server+';Database='+database+';Trusted_Connection=yes;')
print('connect to sql success')

# read DIM_region.json
region_f = open('Dim_Regions.json',)
region_data= json.load(region_f)

# insert data to dim_region table
for region in region_data:
    pass
    # cursor= conn.cursor()
    # try:
    #     cursor.execute("INSERT INTO Dim_Region values('"+str(region_data[region][0]['region-code'])+"','"+region+"')") 
    #     conn.commit()
    # except Exception as e:
    #     print("unsuccess "+str(e))
        
# insert data to dim_Sub_region table
for region in region_data:
    for sub_region in region_data[region]:
        pass
        # cursor= conn.cursor()
        # try:
        #     cursor.execute("INSERT INTO Sub_Region values('"+str(sub_region['sub-region-code'])+"','"+str(sub_region['region-code'])+"','"+sub_region['sub-region']+"')") 
        #     conn.commit()
        # except Exception as e:
        #     print("unsuccess "+str(e))
        
# insert data to dim_country
country_f= open('Dim_Countries.json')
country_lst= json.load(country_f)
for country in country_lst:
    pass
    # Có một số record mang gái trị null    
    if country['country-code']== None:
        country['country-code']=0
    if country['sub-region-code']== None:
        country['sub-region-code']=0
    # cursor= conn.cursor()
    # try:
        # cursor.execute("INSERT INTO Dim_Country VALUES (?,?,?)",country['country'],country['country-code'], country['sub-region-code'])
    #     conn.commit()
    # except Exception as e:
    #     print("unsuccess "+str(e))
  
WordPop_DF= pd.read_csv('WorldPop.csv')
WordPop_DF= WordPop_DF[WordPop_DF["Year"] == 2018]
# cursor= conn.cursor()
# for index in range(0,WordPop_DF.shape[0]):
#     countrycode=str(WordPop_DF.iloc[index]['Country Code'])
#     WP_Year=str(WordPop_DF.iloc[index]['Year'])
#     Value=str(WordPop_DF.iloc[index]['Value'])
#     try:
#         cursor.execute("INSERT INTO World_Pop values('"+countrycode+"','"+WP_Year+"','"+Value+"')") 
#         conn.commit()
#     except Exception as e:
#         print(countrycode)
#         print("unsuccess "+str(e))



# World_Covid_Report
path = 'csse_covid_19_daily_reports/'
files = [f for f in glob.glob(path + "*.csv", recursive=True)]
World_Covid_Report_DF=None

for f in files:
    Temp_Report_DF= pd.read_csv(f)
    # Xử lí các cột bị sai tên
    for colname in Temp_Report_DF.columns.values.tolist():
        if re.search("Country/Region",colname) != None:
            Temp_Report_DF=Temp_Report_DF.rename(columns={colname:'Country_Region'})
        if re.search("Last Update",colname) != None:
            Temp_Report_DF=Temp_Report_DF.rename(columns={colname:'Last_Update'})
    # Xử lí các file thiếu cột active
    if 'Active' not in Temp_Report_DF:
        Temp_Report_DF['Active']= None
    #Chọn các cột cần thiết
    Temp_Report_DF= Temp_Report_DF[['Last_Update','Country_Region','Confirmed','Deaths','Recovered','Active']]
    # Xử lí dữ liệu
    Temp_Report_DF=Temp_Report_DF.fillna(0) #loại bỏ null
    Temp_Report_DF = Temp_Report_DF.astype({"Confirmed": int,"Deaths": int,"Recovered": int,"Active": int,"Country_Region": str})
    Temp_Report_DF['Last_Update'] = pd.to_datetime(Temp_Report_DF['Last_Update'])
    Temp_Report_DF['Last_Update'] = Temp_Report_DF['Last_Update'].dt.normalize() #chỉnh lấy ngày tháng năm
    #Lọc bỏ các dữ liệu sai (<0)
    Temp_Report_DF=Temp_Report_DF[Temp_Report_DF['Confirmed'] >= 0]
    Temp_Report_DF=Temp_Report_DF[Temp_Report_DF['Deaths'] >= 0]
    Temp_Report_DF=Temp_Report_DF[Temp_Report_DF['Recovered'] >= 0]
    Temp_Report_DF=Temp_Report_DF[Temp_Report_DF['Active'] >= 0]
    Temp_Report_DF=Temp_Report_DF.groupby(['Last_Update','Country_Region'],as_index=False).sum()
    # concat từng file vào df tổng
    World_Covid_Report_DF= pd.concat([World_Covid_Report_DF,Temp_Report_DF],ignore_index=True)

# World_Covid_Report_DF.to_excel("Test.xlsx",sheet_name='Sheet_name_1')

# print(World_Covid_Report_DF)

# cursor= conn.cursor()
# for index in range(0,World_Covid_Report_DF.shape[0]):
#     try:
#         cursor.execute("INSERT INTO Word_Covid_Report VALUES (?,?,?,?,?,?)",World_Covid_Report_DF.iloc[index]['Last_Update'],World_Covid_Report_DF.iloc[index]['Country_Region'],int(World_Covid_Report_DF.iloc[index]['Confirmed']),int(World_Covid_Report_DF.iloc[index]['Deaths']),int(World_Covid_Report_DF.iloc[index]['Recovered']),int(World_Covid_Report_DF.iloc[index]['Active']))
#         conn.commit()
#     except Exception as e:
#         pass
