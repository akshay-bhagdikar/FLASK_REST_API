## Created by: Akshay Bhagdikar
## Date modified: 11/02/2018
## Application to check data and insert into database


from __future__ import print_function
import mysql.connector
from mysql.connector import errorcode
import re
from dateutil.parser import parse



## Function to check if the user data is valid. The function checks if user data is integers. Returns user if valid
def check_user(user,raise_exception_flag):
    regex_user = r'[\d]+'
    if re.match(regex_user,user):
        return user
    else:
        if(user!=''):
            raise Exception('user should contain integers')
        else:
            raise Exception('user has null value')
            
## Function to check if the dates are of valid format (month_name/date/year) or (date-month-year). Returns date in valid date format or None
def check_date(date,raise_exception_flag):
    regex_date_1 = r'((January|February|March|April|May|June|July|August|September|October|November|December)/(0?[1-9]|[1-2][0-9]|3?[0-1])/([1-2]{1}[0-9]{3}))'       
    regex_date_2 = r'((0?[1-9]|[1-2][0-9]|3?[0-1])-(0?[1-9]|[1-2][0-9]|3?[0-1])-([1-2]{1}[0-9]{3}))'
    if(re.match(regex_date_1,date) or re.match(regex_date_2,date)):
        return_date = parse(date,dayfirst=True).strftime("%Y-%m-%d")
        return (return_date)
    else:
        if(date!=''):
            if(raise_exception_flag==True):
                raise Exception('Date format not valid: ',date)
            else:
                print("returning null value for "+ amount)
                return None
        else:
            return None

## Function to check if sales amount is valid decimal number. If the amount is negative then it is assumed that transaction was cancelled and the amoun was returned. So negative values are not discarded. Returns valid sales amount or None     
def check_sales_amount(amount,raise_exception_flag):
    regex_amount = r'-{0,1}[\d]+\.\d{1,2}'
    if(re.match(regex_amount,amount)):
        return float(amount)
    else:
        if(amount!=''):
            if(raise_exception_flag==True):
                raise Exception('Sales amount format not valid: ' + amount)
            else:
                print("returning null value for "+ amount)
                return None
        else:
            return None

        
##Function to check if region data is valid. Checks if the region field is capital alphabet (A to Z). Returns valid region or None
def check_region(region,raise_exception_flag):
    regex_region = r'[A-Z]{1}'
    if(re.match(regex_region,region)):
        return region
    else:
        if(region!=''):
            if(raise_exception_flag==True):
                raise Exception('Region format not valid')
            else:
                print("returning null value for "+ region)
                return None
        else:
            return None


## Function to connect to the database
def connect_db(host="data-challenge.cqc9xz3gmhnl.us-west-2.rds.amazonaws.com",\
               port=3306,user="bhagdikara",password='maxocoil12',DB_NAME='transactions'):   
    cnx = mysql.connector.connect(host=host,user=user,password=password,database=DB_NAME)
    cursor = cnx.cursor()
    return cnx,cursor


## Function to insert data from csv files to database. raise_exception_flag parameter if set to True then the data is checked before insertion to database and if the data is not valid then an exception is raised. If it is set to False then the data is checked before insertion to database and if th data is not valid then null data is inserted.
def populate_db(csv_file, transactions_table_name, raise_exception_flag=False,\
               host="data-challenge.cqc9xz3gmhnl.us-west-2.rds.amazonaws.com",\
                user="bhagdikara",password='maxocoil12',DB_NAME='transactions'):
    
    try:
        cnx,cursor = connect_db(host=host,user=user,password=password,DB_name=DB_name) 
        print("Connected to db")
        try:
            sql_transactions = "INSERT INTO " + transactions_table_name + \
                               " (user, transaction_date, sales_amount, joining_date, region) VALUES (%s,%s,%s,%s,%s)"
            print("Now populating")
            with open(csv_file,'rb') as i_file:
                line = next(i_file)
                line_no = 1
                for line in i_file:
                    print(line_no)
                    tokens = re.split(r'[,\t]', line)
                    user = check_user(tokens[0].strip(),raise_exception_flag)
                    transaction_date = check_date(tokens[1].strip(),raise_exception_flag)
                    sales_amount = check_sales_amount(tokens[2].strip(),raise_exception_flag)
                    join_date = check_date(tokens[3].strip(),raise_exception_flag)
                    region = check_region(tokens[4].strip(),raise_exception_flag)
                    val_transactions = (user, transaction_date, sales_amount, join_date, region)
                    cursor.execute(sql_transactions, val_transactions)
                    cnx.commit()
                    line_no += 1
            print("DB populated successful")
            print("Inserted {} lines".format(line_no-1))
        finally:
            cursor.close()
            cnx.close()
    except mysql.connector.Error as err:
        print("Failed inserting data: {}".format(err))


## Executing the above code
populate_db('Data/transactions_2013.csv','transactions_table',True)
populate_db('Data/transactions_2014.csv','transactions_table',True)
populate_db('Data/transactions_2015.csv','transactions_table',True)
populate_db('Data/transactions_2016.csv','transactions_table',True)
        
     





