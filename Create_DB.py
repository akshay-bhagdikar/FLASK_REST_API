## Created by: Akshay Bhagdikar
## Date modified: 11/02/2018
## Application to create a database and tables if they do not exist


import mysql.connector
from mysql.connector import errorcode

#Function to create a connection to the remote database service. Returns cursor and connection object
def create_connection_cursor(host='data-challenge.cqc9xz3gmhnl.us-west 2.rds.amazonaws.com'\
                              ,port=3306,user='bhagdikara',password='maxocoil12'):
    
    cnx = mysql.connector.connect(host=host\
                              ,port=port,user=user,password=password)
    cursor = cnx.cursor()
    return cnx,cursor
    
    

#Function to create a database. If fails then throws the corresponding error. Returns void
def create_database(cursor,DB_NAME):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))   

        
#Function to execute the creation of database. Checks if the database creation is successful. Returns void
def check_and_execute_creation(cursor,cnx,DB_NAME):
    DB_NAME = DB_NAME
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor,DB_NAME)
            print("Database {} created successfully.".format(DB_NAME))
        else:
            print("Database creation unsuccessful: {}".format(err))
    finally:
        cursor.close()
        cnx.close()


#Function to create table in the specified database.'tables' should be a dictionary with the valid insert\query. Returns void
def create_table(tables,cursor,cnx,DB_NAME):
    try:
        cursor.execute("USE {}".format(DB_NAME))
        for table_name in tables:
            table_description = tables[table_name]
            try:
                print("Creating table {}: ".format(table_name), end='')
                cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("Table {} successfully created".format(table_name))
    except mysql.connector.Error as err:
        print("Failed to connect to the database: {}".format(err))
    finally:
        cnx.close()
        cursor.close()
        
        
tables = {}
tables['transactions_table'] = (
    "CREATE TABLE `transactions_table` ("
    "  `row_id` int(6) NOT NULL AUTO_INCREMENT,"
    "  `user` VARCHAR(5) NOT NULL,"
    "  `transaction_date` date,"
    "  `sales_amount` DECIMAL(6,2),"
    "  `joining_date` date,"
    "  `region` CHAR(1),"
    "  PRIMARY KEY (`row_id`)"
    ") ENGINE=InnoDB")


DB_NAME = 'transactions'
cnx,cursor = create_connection_cursor()
check_and_execute_creation(cursor,cnx,DB_NAME)
cnx,cursor = create_connection_cursor()
create_table(tables, cursor,cnx, DB_NAME)



