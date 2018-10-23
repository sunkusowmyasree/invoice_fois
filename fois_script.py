import pymysql.cursors
import sys
import re
import sqlalchemy as sq
import csv
import pymysql
import pandas as pd
import numpy as np


##### GLOBAL VARIABLES###########
################################
def crt(table_name):
    connection = pymysql.connect("localhost","root","","test",charset="utf8")
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS "+table_name)
    sql_command = """
    CREATE TABLE
    IF NOT EXISTS """+table_name+"""(
    Request_ID      varchar(20) UNIQUE ,
    Incident_Number varchar(20)     NULL,
    Task_ID varchar(20)     NULL,
    GUTS_Ticket_ID  varchar(50)     NULL,
    SR_Status       varchar(100)    NULL,
    SR_Status_Reason        varchar(100)    NULL,
    Incident_Status varchar(100)    NULL,
    Incident_Status_Reason  varchar(100)    NULL,
    Resolution_Notes        text    NULL,
    Resolution_Categorization_Tier_1        varchar(100)    NULL,
    Resolution_Categorization_Tier_2        varchar(100)    NULL,
    SR_Create_Date_Time     datetime        NULL,
    SR_Completion_Date      datetime        NULL,
    SR_Closed_Cancelled_Date        datetime        NULL,
    Site_Access_Approved_Date       datetime        NULL,
    Requested_Due_Date_From datetime        NULL,
    Requested_Due_Date_To   datetime        NULL,
    Requested_By_Full_Name  varchar(100)    NULL,
    Incident_Assignee       varchar(100)    NULL,
    Best_Effort 	    varchar(100)    NULL,
    Assigned_WFM_FT text    NULL,
    Service_Request_Priority        varchar(100)    NULL,
    Incident_Priority       varchar(100)    NULL,
    Customer_Region varchar(50)     NULL,
    Ericsson_Region varchar(50)     NULL,
    Site_Group      varchar(50)     NULL,
    Site    varchar(50)     NULL,
    Address         Text            NULL,
    City            varchar(100)     NULL,
    State          varchar(100)     NULL,
    Zip             varchar(20)     NULL,
    Country varchar(50)     NULL,
    Time_Zone       varchar(100)    NULL,
    Work_Type       varchar(50)     NULL,
    WFM_Scheduled_Start_Date        datetime        NULL,
    Travel_Start_Date       datetime        NULL,
    On_Site_Arrival_Work_Start_Date datetime        NULL,
    Work_End_Date   datetime        NULL,
    Travel_Duration_mins    varchar(50)     NULL,
    Install_Duration_mins   varchar(50)     NULL,
    PA_SLA_Start_Time       varchar(50)     NULL,
    PA_SLA_End_Time varchar(50)     NULL,
    PA_SLA_Due_Time varchar(50)     NULL,
    PA_SLA_Duration varchar(50)     NULL,
    PA_SLA_Met_Breached     varchar(50)     NULL,
    RV_SLA_Start_Time       varchar(50)     NULL,
    RV_SLA_End_Time varchar(50)     NULL,
    RV_SLA_Due_Time varchar(50)     NULL,
    RV_SLA_Duration varchar(50)     NULL,
    RV_SLA_Met_Breached     varchar(50)     NULL,
    SAT_SLA_Start_Time      varchar(50)     NULL,
    SAT_SLA_End_Time        varchar(50)     NULL,
    SAT_SLA_Due_Time        varchar(50)     NULL,
    SAT_SLA_Duration        varchar(50)     NULL,
    SAT_SLA_Met_Breached    varchar(50)     NULL,
    Edit_Flag       INT      ,
    Acceptance_Flag INT      NULL,
    Valid_Flag      INT      NULL,
    Need_Review     VARCHAR(20)      NULL,
    Invoice_Flag    INT      NULL,
    Audit_Flag      INT      NULL,
    Drive_Fee       varchar(11)      NULL,
    Work_Fee        varchar(11)      NULL,
    Total_Fee       varchar(11)      NULL,
    Invoice_Number  INT      NULL,
    Drive_Rate      varchar(11)      NULL,
    Work_Rate       varchar(11)      NULL,
    Record_Update_Time  datetime    NULL,
    Dispute_Flag  INT     NULL,
    ID        INT  NOT NULL  AUTO_INCREMENT ,
    Invoice_Date       datetime     NULL,
    Dispute_Reason        text     NULL,
    Scope        varchar(50)     NULL,
    Customer_Review_Flag        INT     NULL,
    PRIMARY KEY(ID));"""
    cursor.execute(sql_command)
    sql_command=""" ALTER TABLE """+table_name+""" CONVERT TO CHARACTER SET utf8;"""
    cursor.execute(sql_command)
    connection.commit()
    s=""" UPDATE """+table_name+""" SET Scope='Limited';"""
    cursor.execute(s)
    connection.commit()
    connection.close()
    
def csv_to_panda_df(src):
    df1 = pd.read_csv(src,parse_dates=True,low_memory=False,encoding='latin-1')
    return df1

def filter_df(df_raw):
    df_raw.columns = [x.replace('/','_') for x in df_raw.columns] #replacing the '/' charcter with '_' in the column names as it wont be vaild for database
    df_raw.columns = [x.replace(')','_') for x in df_raw.columns] #replacing the ')' charcter with '_' in the column names as it wont be vaild for database
    df_raw.columns = [x.replace('(','_') for x in df_raw.columns] #replacing the '(' charcter with '_' in the column names as it wont be vaild for database
    df_raw.columns = [x.strip().replace(' ','_') for x in df_raw.columns] #replacing the spaces in  column names as it wont be vaild for database
    df_raw.columns = [x.replace('Travel_Duration__mins_','Travel_Duration_mins') for x in df_raw.columns]
    df_raw.columns = [x.replace('Install_Duration__mins_','Install_Duration_mins') for x in df_raw.columns]
    df_raw.columns = [x.replace('Install_Duration__mins_','Install_Duration_mins') for x in df_raw.columns]
    df_raw.columns = [x.replace('Install_Duration__mins_','Install_Duration_mins') for x in df_raw.columns]
    df_raw.columns = [x.replace('On_Site_Arrival___Work_Start_Date','On_Site_Arrival_Work_Start_Date') for x in df_raw.columns]
    df_raw.columns = [x.replace('City_Township','City') for x in df_raw.columns]
    df_raw.columns = [x.replace('State_Province','State') for x in df_raw.columns]
    df_raw.columns = [x.replace('Zip_Postal_Code','Zip') for x in df_raw.columns]
    df_raw.drop(['WFM_WO_ID','Title','Task_Status','Task_Status_Reason','REQ_Last_Modified_Date','REQ_Last_Modified_By','INC_Last_Modified_Date','INC_Last_Modified_By','TAS_Last_Modified_Date','TAS_Last_Modified_By','WFM_Status','WFM_Status_Reason_Code','Incident_Create_Date_Time','Task_Create_Date_Time','Requested_For_Full_Name','Equipment_Type','Technical_Contact_First_Name','Technical_Contact_Last_Name','Technical_Contact_Email','Technical_Contact_Phone_#','General_Description','Restricted_Access','Restricted_Access_Notes','Tools_Required?','Tools_Required_Notes','Is_Part_Required?','Is_Part_Required_Notes','Consumables_Required?','Consumables_Required_Notes','SLA_Reason_Code','SLA_Reason_Text'], axis=1, inplace=True)
    #dff=df_raw[df_raw.columns[[0,1,2,4,6,7,8,9,20,21,22,23,26,27,28,29,30,31,33,34,35,37,38,39,40,41,42,43,44,45,46,48,62,63,64,65,66,67,70,71,72,73,74,75]]] #selection of columns needed
    df_raw=df_raw[df_raw.Incident_Status_Reason != 'Invalid Request']#removing the rows that are marked 'Invalid Request; in the column Incident_Status_Reason
    df_raw=df_raw[df_raw.Incident_Status_Reason != 'Request Invalid']
    df_raw=df_raw[df_raw.SR_Status != 'Pending']#removing the rows that are marked 'Invalid Request; in the column Incident_Status_Reason
    df_raw=df_raw[df_raw.SR_Status != 'In Progress']#removing the rows that are marked 'Invalid Request; in the column Incident_Status_Reason
    df_raw=df_raw[df_raw.SR_Status != 'Planning']             

    try:
        df_raw['Travel_Duration_mins']=pd.to_numeric(df_raw['Travel_Duration_mins'].str.replace(',',''), errors='coerce') #converting the tring numbers with comma to number format 
        df_raw['Install_Duration_mins']=pd.to_numeric(df_raw['Install_Duration_mins'].str.replace(',',''), errors='coerce')#converting the tring numbers with comma to number format 
        return df_raw
    except:
        return df_raw
def mysql_conn(hostname,username,pwd,database):
    db = pymysql.connect(hostname,username,pwd,database,charset="utf8") 
    return db

def temp_to_master(engine,table_name,db):
    cursor=db.cursor()
    try:
        query="SELECT Request_ID,Edit_Flag,Acceptance_Flag,Invoice_Flag,Dispute_Flag,Customer_Review_Flag FROM fois_master_table"
        mas_df=pd.read_sql_query(sql=query,con=engine,index_col=None)
        
    except:
        sql = "CREATE TABLE fois_master_table LIKE "+table_name
        sql1 = "INSERT INTO fois_master_table SELECT * FROM "+table_name
        cursor.execute(sql)
        cursor.execute(sql1)
        db.commit()
        mas_df=None
    return mas_df

import sqlalchemy as sq
import csv
import pymysql
import pandas as pd
import math
import numpy as np
import datetime
from datetime import timedelta
from time import gmtime, strftime
import myfunctions
import shutil
import os

def GMT_imply(dff,col_nam):
    for coll in col_nam:
        for index,row in dff.iterrows():
            time_str=row['Time_Zone']
            actual_time=row[coll]
            #print(actual_time)
            sign=time_str[4]
            strip_time=time_str[5:(time_str.find(')'))]
            colon_ind=strip_time.find(':')
            if colon_ind == -1:
                continue
            hour=int(strip_time[:colon_ind])
            minu=int(strip_time[(colon_ind+1):(time_str.find(')'))])
            if sign == '-':
                delta=datetime.timedelta(hours=hour,minutes=minu)
                converted_time=actual_time-delta
                #print(converted_time)
                dff.set_value(index,coll,converted_time)
            else:
                delta=datetime.timedelta(hours=hour,minutes=minu)
                converted_time=actual_time+delta
                dff.set_value(index,coll,converted_time)
    return dff

#########Find the csv file in the directory reads it and moves it to log folder#############
###################CRON job runs for every 1 hour###########################################
curr=strftime("%H:%M:%S")
for file in os.listdir("C:\\Users\\ezsresu\\Desktop\\Reports"):
    if file.endswith(".csv"):
        var=os.path.join(file)
ind=var.find('SLA')
suff=var[ind:]
ind1=suff.find('/')
ind2=suff.find('.')
filename=suff[(ind1+1):ind2]
dst='/var/www/html/google_fois/log_sftp/'+filename+'_'+curr+'.csv'
scr=var

#intializing the variables
off_hours=set([8,9,10,11,12,13,14,15,16,17])
weekdays=set([0,1,2,3,4])
table_name='temp_f'
crt(table_name)

engine = sq.create_engine('mysql+pymysql://root:@localhost/test?charset=utf8')
#path of the excel file to be used
#raw_data=r''+var
raw_data=r'C:\\Users\\ezsresu\\Desktop\\Service Level & Invoice Report - SFTP Report - 24 Apr 2018.csv'

#make connection with Mysql database
db=myfunctions.mysql_conn("localhost","root","","test")

#build the dataframe table from csv
df_raw=csv_to_panda_df(raw_data)

#filter the dataframe
dff=filter_df(df_raw)
#shutil.move(scr,dst)

flag_df = pd.DataFrame(columns=['Edit_Flag', 'Acceptance_Flag', 'Valid_Flag','Invoice_Flag', 'Audit_Flag', 'Drive_Fee', 'Work_Fee', 'Total_Fee','Invoice_Number','Drive_Rate','Work_Rate','Need_Review','Record_Update_Time','Dispute_Flag','Invoice_Date','Dispute_Reason','Scope','Customer_Review_Flag'])
#fetch the threshold values of Travel duration and Install duration 
thres_df=pd.read_sql_table(table_name='threshold_table',con=engine,index_col='Validation_Threshold',coerce_float=True) 
travel_dur_threshold_min=thres_df.ix[0][1]
travel_dur_threshold_max=thres_df.ix[0][2]
install_dur_threshold_min=thres_df.ix[1][1]
install_dur_threshold_max=thres_df.ix[1][2]

#fetch the holiday details
holz_df=pd.read_sql_table(table_name='holiday_table',con=engine,index_col='id',coerce_float=True)
holz=[]
for index,row in holz_df.iterrows():
    x=str(row['holiday_date'])
    n=x.find(' ')
    x=x[:n]
    holz.append(x)
'''holz_df=pd.read_sql_table(table_name='holiday_table',con=engine,index_col='id',coerce_float=True)
holz=[]
for index,row in holz_df.iterrows():
    x=str(row['holiday_date'])
    n=x.find(' ')
    x=x[:n] #removing timestamp from datetime
    x1=row['country']
    x2=[x,x1] #appending country and date
    holz.append(x2) #list of all holiday dates with respective countries '''
    

df_re=dff['Request_ID'] 
df_re.drop_duplicates( keep='first', inplace=True)#DEBUG PURPOSE: dataframe of REQUEST_ID alone

sla_index=dff.columns.get_loc("SLA_Milestone") #index number of column name 'SLA_Milestone'
df_pre=dff.iloc[:,0:sla_index] #This dataframe contains all the columns before 'SLA_Milestone'
df_pre.drop_duplicates(subset=None, keep='first', inplace=True)#droppping the duplicate rows in df_pre
df_sff=dff.iloc[:,(sla_index+6):]#This dataframe contains columns after 'SLA_Met_Breached'
# renaming the phrases of SLA_Milestone column for easy evalution
dff1=dff.replace(['Plan & Act SLA for P2','Plan & Act SLA for P1','Plan & Act SLA for P0','Request Validation SLA for P1','Request Validation SLA for P0','Request Validation SLA for P2','Site Arrival Time SLA for P1','Site Arrival Time SLA for P0','Site Arrival Time SLA for P2'],['Plan','Plan','Plan','Request','Request','Request','Site','Site','Site'])

sla_plan= dff1[dff1['SLA_Milestone'] =='Plan'] #this df contains all the rows that belongs to 'Plan' (SLA_Milestone)
sla_req= dff1[dff1['SLA_Milestone'] =='Request'] #this df contains all the rows that belongs to 'Request' (SLA_Milestone)
sla_site= dff1[dff1['SLA_Milestone'] =='Site'] #this df contains all the rows that belongs to 'Site' (SLA_Milestone)

fin=df_pre.join(df_sff) # joining the df_pre and df_sff into one df called fin

# renaming the column names in sla_plan,sla_req,sla_site for easy evalution
sla_plan=sla_plan.rename(columns={'SLA_Start_Time':'PA_SLA_Start_Time','SLA_End_Time':'PA_SLA_End_Time','SLA_Due_Time':'PA_SLA_Due_Time','SLA_Duration':'PA_SLA_Duration',
       'SLA_Met_Breached':'PA_SLA_Met_Breached'})
sla_req=sla_req.rename(columns={'SLA_Start_Time':'RV_SLA_Start_Time','SLA_End_Time':'RV_SLA_End_Time','SLA_Due_Time':'RV_SLA_Due_Time','SLA_Duration':'RV_SLA_Duration',
       'SLA_Met_Breached':'RV_SLA_Met_Breached'})
sla_site=sla_site.rename(columns={'SLA_Start_Time':'SAT_SLA_Start_Time','SLA_End_Time':'SAT_SLA_End_Time','SLA_Due_Time':'SAT_SLA_Due_Time','SLA_Duration':'SAT_SLA_Duration',
       'SLA_Met_Breached':'SAT_SLA_Met_Breached'})

# extract only the columns 'Request_Id' and SLA criterias into respective DFs for mergeing process
t_sla_plan=sla_plan[['Request_ID','PA_SLA_Start_Time','PA_SLA_End_Time','PA_SLA_Due_Time','PA_SLA_Duration','PA_SLA_Met_Breached']]
t_sla_req=sla_req[['Request_ID','RV_SLA_Start_Time','RV_SLA_End_Time','RV_SLA_Due_Time','RV_SLA_Duration','RV_SLA_Met_Breached']]
t_sla_site=sla_site[['Request_ID','SAT_SLA_Start_Time','SAT_SLA_End_Time','SAT_SLA_Due_Time','SAT_SLA_Duration','SAT_SLA_Met_Breached']]
t_sla_plan['Request_ID'].drop_duplicates(keep='last', inplace=True)
t_sla_req['Request_ID'].drop_duplicates(keep='last', inplace=True)
t_sla_site['Request_ID'].drop_duplicates(keep='last', inplace=True)

merged_df = fin.merge(t_sla_plan, how = 'outer', on = ['Request_ID'])#merging the fin df w.r.t Respect_ID with sla_plan df
merged_df = merged_df.merge(t_sla_req, how = 'outer', on = ['Request_ID'])#merging the fin df w.r.t Respect_ID with sla_req df
merged_df = merged_df.merge(t_sla_site, how = 'outer', on = ['Request_ID'])#merging the fin df w.r.t Respect_ID with sla_site df
merged_df.drop_duplicates(keep='first', inplace=True) #droppping the duplicate rows in merged df

date_for=['SR_Create_Date_Time','SR_Completion_Date','SR_Closed_Cancelled_Date','Site_Access_Approved_Date', 'Requested_Due_Date_From','Requested_Due_Date_To','WFM_Scheduled_Start_Date','Travel_Start_Date','On_Site_Arrival_Work_Start_Date', 'Work_End_Date','PA_SLA_Start_Time', 'PA_SLA_End_Time', 'PA_SLA_Due_Time','RV_SLA_Start_Time','RV_SLA_End_Time', 'RV_SLA_Due_Time','SAT_SLA_Start_Time','SAT_SLA_End_Time','SAT_SLA_Due_Time']

for col in date_for:
    merged_df[col]=pd.to_datetime(merged_df[col]) # converting the datetime format from dd/mm/yyyy to yyyy/mm/dd

merged_df['Travel_Start_Date']=pd.to_datetime(merged_df['Travel_Start_Date'])
#########
eval_dff= merged_df[['Country','On_Site_Arrival_Work_Start_Date','SR_Status','Travel_Duration_mins','Install_Duration_mins','Resolution_Notes']]
eval_flag=flag_df
join_df=eval_dff.join(eval_flag)
join_df['Need_Review']=join_df['Need_Review'].astype(str)

nr=[]
#Loop through each row applying the Validation condition then update valid_flag
for index, temp in join_df.iterrows():
    x=temp['Resolution_Notes']
    
    try:
        y=x.find('Cancel')
        y1=y.find('cancel')
    except:
        y=-1
        y1=-1
    nr=[]
    temp0=temp.isnull()
    if(y==-1 and y1==-1 and temp0['On_Site_Arrival_Work_Start_Date'] == False and (temp['SR_Status'] != "Cancelled" and (math.isfinite(float(temp['Install_Duration_mins'])) and math.isfinite(float(temp['Travel_Duration_mins']))))):
        a=str(temp['On_Site_Arrival_Work_Start_Date'])
        n=a.find(' ')
        date=a[:n]
        time=a[(n+1):]
        hour=int(time[:2])
        year=int(date[:4])
        month=int(date[5:7])
        day=int(date[8:])
        weekno = datetime.date(year,month,day).weekday()
        in_off= hour in off_hours
        work_day= weekno in weekdays
        if (date in holz) or(in_off == False or work_day == False):
            join_df.set_value(index,'Valid_Flag',int('7')) #valid_flag=7 for outside business hours
            nr.append('7')
            nr=str(nr)
            join_df.set_value(index,'Need_Review',nr)
            continue
    if(temp['SR_Status']=='Pending' or temp['SR_Status']=='In Progress'):
         join_df.set_value(index,'Valid_Flag',int('1')) #valid_flag=1 for within threshold
         nr.append('1')
    if(temp['SR_Status']=='Closed' or temp['SR_Status']=='Completed')and((float(temp['Travel_Duration_mins']) >= travel_dur_threshold_min and float(temp['Travel_Duration_mins'])<= travel_dur_threshold_max) and (float(temp['Install_Duration_mins']) >= install_dur_threshold_min and float(temp['Install_Duration_mins'])<= install_dur_threshold_max)):
         join_df.set_value(index,'Valid_Flag',int('1')) #valid_flag=1 for within threshold
         nr.append('1')
    if(temp['SR_Status']=='Closed' or temp['SR_Status']=='Completed')and(float(temp['Travel_Duration_mins']) < travel_dur_threshold_min ) or (float(temp['Install_Duration_mins']) < install_dur_threshold_min ):
        join_df.set_value(index,'Valid_Flag',int('2'))  #valid_flag=2 for under threshold
        nr.append('2')
    if (temp['SR_Status']=='Closed' or temp['SR_Status']=='Completed')and(float(temp['Travel_Duration_mins']) > travel_dur_threshold_max ) or (float(temp['Install_Duration_mins']) > install_dur_threshold_max ):
        join_df.set_value(index,'Valid_Flag',int('3'))  #valid_flag=3 for over threshold
        nr.append('3')
    if (temp['SR_Status']=='Closed' or temp['SR_Status']=='Completed')and(math.isnan(float(temp['Travel_Duration_mins']) ) or math.isnan(float(temp['Install_Duration_mins']))):
        join_df.set_value(index,'Valid_Flag',int('4'))  #valid_flag=4 for missing metric
        nr.append('4')
    if ((temp['SR_Status']=='Completed' or temp['SR_Status']=='Closed') and ((y>-1) or(y1>-1)))or(temp['SR_Status']=='Cancelled' and (math.isfinite(float(temp['Install_Duration_mins'])) or math.isfinite(float(temp['Travel_Duration_mins'])))):
        join_df.set_value(index,'Valid_Flag',int('5'))  #valid_flag=5 for cancelled metric
        nr.append('5')
    if len(nr)>1:
        join_df.set_value(index,'Valid_Flag',int('6'))  #valid_flag=6 for incomplete metric
        nr.append('6') 
    nr=str(nr)
    join_df.set_value(index,'Need_Review',nr)

join_df=join_df[['Edit_Flag', 'Acceptance_Flag', 'Valid_Flag', 'Need_Review','Invoice_Flag', 'Audit_Flag', 'Drive_Fee', 'Work_Fee', 'Total_Fee','Invoice_Number','Drive_Rate','Work_Rate','Record_Update_Time','Dispute_Flag','Invoice_Date','Dispute_Reason','Scope','Customer_Review_Flag']]
df=merged_df.join(join_df)
df = df[pd.notnull(df['Valid_Flag'])]
current_time=strftime("%Y-%m-%d %H:%M:%S")
df['Record_Update_Time']=current_time  #reading the update time as current time
df[['Edit_Flag','Dispute_Flag','Acceptance_Flag','Invoice_Flag','Audit_Flag','Customer_Review_Flag']]=0
df['Scope']='Limited'

col_names=['SR_Create_Date_Time','SR_Completion_Date','SR_Closed_Cancelled_Date','Site_Access_Approved_Date','Requested_Due_Date_From','Requested_Due_Date_To','WFM_Scheduled_Start_Date','Travel_Start_Date','On_Site_Arrival_Work_Start_Date','Work_End_Date']
df=GMT_imply(df,col_names)

df.to_sql(name=table_name,con=engine,if_exists='append',index=False)


####### checking the flags of master table ######
mas=temp_to_master(engine,table_name,db)
df_f=df
if not mas is None:
    df_dff=df_f[['Request_ID','Edit_Flag','Acceptance_Flag','Invoice_Flag','Dispute_Flag','Customer_Review_Flag']]
    for index,row in df_f.iterrows():
        if mas.Request_ID[mas.Request_ID == row['Request_ID']].count()>0:
            
            e=mas.Edit_Flag[mas.Request_ID == row['Request_ID']]
            e=e.iloc[0]
            a=mas.Acceptance_Flag[mas.Request_ID == row['Request_ID']]
            a=a.iloc[0]
            i=mas.Invoice_Flag[mas.Request_ID == row['Request_ID']]
            i=i.iloc[0]
            d=mas.Dispute_Flag[mas.Request_ID == row['Request_ID']]
            d=d.iloc[0]
            co=mas.Customer_Review_Flag[mas.Request_ID == row['Request_ID']]
            co=co.iloc[0]
            if( (e or a or i or d or co )== 1):
                #print(row['Request_ID'])
                df_f.drop(index, inplace=True)
            else:
                df_f.drop(index, inplace=True)
    df_f.to_sql(name='fois_master_table',con=engine,if_exists='append',index=False)#update the master table 
