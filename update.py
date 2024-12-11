#!/usr/bin/env python
# coding: utf-8

import pyodbc
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa
from pandasql import sqldf
from datetime import date
import datetime
import time
import os
import tkinter as tk
from tkinter import messagebox
import sys


def popup_error(message):
    root = tk.Tk()
    root.withdraw()
    # Message box that exits the program when clicking ok
    messagebox.showerror("Error", message)
    root.destroy()
    sys.exit()


def check_vpn_connection():
    try:
        file = open("N:/DoIT/US/Metrics/Core Monthly Report/ServiceAvail/service_config.csv")
        file.close()
        return True
    except:
        return False


def get_total_minutes(year, month):
    if month in [1, 3, 5, 7, 8, 10, 12]:  # Months with 31 days
        return 44640  # 31 days
    elif month == 2:  # February
        # Check for leap year
        if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
            return 43200  # 29 days in leap year
        else:
            return 40320  # 28 days in non-leap year
    else:  # Months with 30 days
        return 43200  # 30 days (April, June, September, November)
    
def get_db_config():
    with open('db.config', 'r') as f:
        lines = f.readlines()
        lines = [line.strip().split("=") for line in lines]
        config = {line[0]: line[1] for line in lines}
        return config

def get_data():    
    
    pysqldf = lambda q: sqldf(q)

    config = get_db_config()

    conn_string = f"DRIVER={{SQL Server}};SERVER={config['SERVER_URL']};DATABASE={config['DB_NAME']};UID={config['UNAME']};PWD={config['PWD']}"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_string})
    engine = create_engine(connection_url)
    
    print("Connecting to database...")
    time.sleep(1)
    
    
    with engine.begin() as conn:
        print("Connected!")
        cust_table = pd.read_sql_query(
            """
            SELECT o.*,
                    p.TechnicalService,
                    ci.FriendlyName,
                    COALESCE(p.TechnicalService, ci.FriendlyName, NULL) as TechService,
                    ci.ConfigurationItemTypeName,
                    ci.OwnerDepartment
             FROM Outage o
                      JOIN TrebuchetLink tl
                           ON o.RecID = tl.JoinChildBusObId
                      LEFT JOIN Problem p on p.RecID = tl.JoinParentBusObId
                      LEFT JOIN Change c on c.RecID = tl.JoinParentBusObId
                      LEFT JOIN ChangeRequestLinksCI cr on cr.ParentChangeID = c.RecID
                      LEFT JOIN ConfigurationItem ci
                                on (ci.RecID = cr.ChildID or ci.RecID = p.TechnicalServiceID) and
                                   ci.ConfigurationItemTypeName =
                                   'Config - Technical Service'
             WHERE (p.Status is null or p.Status != 'Cancelled')
               AND COALESCE(p.TechnicalService, ci.FriendlyName) IS NOT NULL
               AND OutageType = 'Unplanned'
            """
        , conn)
    
    
    
    services_df = pd.read_csv("N:/DoIT/US/Metrics/Core Monthly Report/ServiceAvail/service_config.csv")
    
    
    
    all_svcs = sqldf(f"""SELECT *, 
        (JULIANDAY(OutageEnd) - JULIANDAY(OutageStart)) * 1440 as OutageLength
        FROM cust_table 
        where TechService in {tuple(services_df["Service"])}""")
    
    
    svcs_grouped = sqldf("""
        SELECT 
            TechService,
            strftime('%Y', OutageStart) as year,
            strftime('%m', OutageStart) as month,
            SUM(OutageLength) as total_mins
        FROM all_svcs
        GROUP BY TechService, year, month
        """)

    
    svcs_grouped["year"] = svcs_grouped["year"].astype(int)
    svcs_grouped["month"] = svcs_grouped["month"].astype(int)

    
    first_day_previous_month = (pd.Timestamp.now() - pd.DateOffset(months=1)).replace(day=1)
    dates = pd.date_range('2017-07-01', first_day_previous_month, freq='MS')
    
    months_df = pd.DataFrame({
        'Year': dates.year,
        'Month': dates.month
    })
    
    services_df = services_df.merge(months_df, how='cross')
    
    print("Records collected")
    
    
    outage_records = services_df.merge(svcs_grouped, how='left', left_on=["Service", "Year", "Month"], right_on=["TechService", "year", "month"])[["Department", "Service", "Target", "Year", "Month", "total_mins"]]
    outage_records["total_mins"] = outage_records["total_mins"].fillna(0)
    outage_records.rename(columns={"total_mins": "outage_mins"}, inplace=True)
    
    
    outage_records['month_mins'] = outage_records.apply(lambda row: get_total_minutes(row['Year'], row['Month']), axis=1)
    outage_records["pct_up"] = 1 - (outage_records["outage_mins"]/outage_records["month_mins"])

    return outage_records


if __name__ == "__main__":

    print("--------------------- Service Availability Update Tool ---------------------")
    print("\nThis tool will update the service availability data for the previous month.")
    print("This tool will also create a backup of the current data.")

    print("\nPlease make sure you are connected to VPN before updating.\n")

    if not check_vpn_connection():

        popup_error("Please connect to Global Protect VPN and try again.")
        

    data = get_data()


    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    try:
        data.to_csv(f"N:/DoIT/US/Metrics/Core Monthly Report/ServiceAvail/backups/outages_{timestamp}.csv", index=False)
        print("Backup written")

        data.to_csv('N:/DoIT/US/Metrics/Core Monthly Report/ServiceAvail/outages.csv', index=False)
        print("Records written")
        print("Data available at N:/DoIT/US/Metrics/Core Monthly Report/ServiceAvail/outages.csv")

    except Exception as e:
        print("Error writing data: please make sure you are connected to VPN and try again.")
        exit()

    print("Opening data folder...")
    time.sleep(1)


    os.startfile(r'N:/DoIT/US/Metrics/Core Monthly Report/ServiceAvail')

