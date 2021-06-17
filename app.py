import pandas as pd
import pyodbc 
import re
from fast_to_sql import fast_to_sql as fts
import chardet

# init
server = 'LAPTOP-N94DSRJ8\VINHSEVER' 
database = 'HPT_BT' 
conn = pyodbc.connect('Driver={SQL Server};Server='+server+';Database='+database+';Trusted_Connection=yes;')
print('connect to sql success')

cleanlist2=['thcs','thpt','-','hệ thống']
cleanlist3=['th','thpt','-']
namelist=['trường chinh','trường chinh a']

def transform_pattern(mystr,pat,repl):
    mystr=re.sub(pat , repl, mystr)
    return mystr

def transform_name(mystr):
    mystr=(mystr.lower()).replace("giáo dục và đào tạo","gd&đt")
    mystr= (mystr.title()).replace("Gd&Đt","GD&ĐT")
    return mystr

def transform_domain(mystr):
    protocol= 'http://'
    if not protocol in mystr:
        mystr= protocol+mystr
    return mystr

def transform_schoolname_v2(mystr):
    # b1 -> lower case , space
    mystr= mystr.lower()
    # b2 -> clean
    for word in cleanlist2:
        if(word in mystr):
            mystr=mystr.replace(word,"")
    mystr=mystr.strip()
    mystr= transform_pattern(mystr,"\s+"," ")
    #b3 -> tiểu học -> th
    mystr= transform_pattern(mystr,"tiểu học","th")
    #b4 -> s_word + th -> th
    mystr= transform_pattern(mystr,"^(.*)th\s","th ")
    #b5 -> Trường đứng đầu -> th , th đặc biệt
    truong_count= mystr.count('trường')
    if truong_count >1:
        mystr=transform_pattern(mystr,"^(trường)","th")
    else:
        if not mystr in namelist:
            mystr=transform_pattern(mystr,"^(trường)","th")
    #b6 -> thêm th cho tên trường không
    if not "th" in mystr:
        mystr= "th "+mystr
    #b7 -> Viết hoa
    mystr=mystr.title()
    mystr=transform_pattern(mystr,"^(Th)\s","TH ")
    return mystr

def transform_schoolname_v3(mystr):
    mystr= mystr.lower()
    mystr=mystr.strip()
    mystr= transform_pattern(mystr,"\s+"," ")
    mystr= transform_pattern(mystr,"(trung học cơ sở)|(trung học)","thcs")
    mystr= transform_pattern(mystr,"^(.*)thcs\s","thcs ")
    truong_count= mystr.count('trường')
    if truong_count >1:
        mystr=transform_pattern(mystr,"^(trường)","thcs")
    else:
        if not mystr in namelist:
            mystr=transform_pattern(mystr,"^(trường)","thcs")
    #b6 -> thêm th cho tên trường không
    if not "thcs" in mystr:
        mystr= "thcs "+mystr
    #b7 -> Viết hoa
    mystr=mystr.title()
    mystr=transform_pattern(mystr,"^(Thcs)\s","THCS ")
    return mystr

# print(transform_schoolname_v2("hệ thống trường abc xyz"))
# print(transform_schoolname_v3("TH - THCS Tân Trung"))

df1 = pd.read_csv('1.edu_ds_donvi_phong_gd_dt_0.csv')
df1['DiaChi'] = df1['DiaChi'].fillna('None')
df1['TenDonVi'] = df1['TenDonVi'].apply(lambda x: transform_name(x))
df1['TenMien'] = df1['TenMien'].apply(lambda x: transform_domain(x))
# fts.fast_to_sql(df1, "tbl_GDVDT", conn, if_exists="append", custom=None, temp=False)
# conn.commit()

df2 = pd.read_csv('2.edu_ds_donvi_khoi_tieu_hoc_0.csv')
df2['DiaChi'] = df2['DiaChi'].fillna('None')
df2['TenDonVi'] = df2['TenDonVi'].apply(lambda x: transform_schoolname_v2(x))
df2['TenMien'] = df2['TenMien'].apply(lambda x: transform_domain(x))
# fts.fast_to_sql(df2, "tbl_tieuhoc", conn, if_exists="append", custom=None, temp=False)
# conn.commit()

df3= pd.read_csv('3.edu_ds_donvi_khoi_thcs_1_edited.csv')
df3['DiaChi'] = df3['DiaChi'].fillna('None')
df3['TenDonVi'] = df3['TenDonVi'].apply(lambda x: transform_schoolname_v3(x))
df3['TenMien'] = df3['TenMien'].apply(lambda x: transform_domain(x))
# fts.fast_to_sql(df3, "tbl_thcs", conn, if_exists="append", custom=None, temp=False)
# conn.commit()


# cursor= conn.cursor()
# for index,row in df3.iterrows():
#     parent_id=str(df3.iloc[index]['ParentID'])
#     name=str(df3.iloc[index]['TenDonVi'])
#     id=str(df3.iloc[index]['MaDonVi'])
#     addr=str(df3.iloc[index]['DiaChi'])
#     domain=str(df3.iloc[index]['TenMien'])
#     try:
#         cursor.execute("INSERT INTO tbl_thcs values("+parent_id+",N'"+name+"',N'"+id+"',N'"+addr+"',N'"+domain+"')") 
#         conn.commit()
#     except Exception as e:
#         print("unsuccess "+str(e))



# mylist=[]
# f = open("3.edu_ds_donvi_khoi_thcs_1_.csv", "r",encoding='latin-1')
# for line in f:
#     line_item_list= f.readline().split("\t")
#     mylist.append(line_item_list)

# df3= pd.DataFrame(mylist,columns=['ParentID','TenDonVi','MaDonVi','DiaChi','TenMien'])
# df3['DiaChi'] = df3['DiaChi'].fillna('None')
# df3['TenMien'] = df3['TenMien'].apply(lambda x: transform_domain(x))
# print(df3)
# # df3['TenDonVi']= df3['TenDonVi'.apply(lambda x:transform_schoolname_v2(x))]
# print(df3.iloc[0]['TenDonVi'])
# test= df3.iloc[0]['TenDonVi']
# bytes(test,'utf-8').decode('utf-8')
