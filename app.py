## Created by: Akshay Bhagdikar
## Date modified: 11/02/2018
## Python Flask application to fetch metric from user, query the database for the corresponding metric and serve the results in a json format

from flask import Flask,jsonify
import mysql.connector
from mysql.connector import errorcode
from flask import make_response

app = Flask(__name__)


#Function to connect to the Database. Returns cursor,connection object
def connect_db(DB_NAME, host = 'data-challenge.cqc9xz3gmhnl.us-west-2.rds.amazonaws.com',user = 'bhagdikara'\
                       ,password = 'maxocoil12'):
    try:
        cnx = mysql.connector.connect(host=host,\
                                      user=user,password=password,database=DB_NAME)
        cursor = cnx.cursor()
        print("DB connected")
        return cnx,cursor
    except mysql.connector.Error as err:
        print("Failed connecting to database: {}".format(err))
    

# Function to query the database for revenue per year. Returns jsonified revenue per year
def get_revenue(cursor):
    cursor.execute("SELECT DATE_FORMAT(transaction_date,'%Y') AS transaction_year, SUM(sales_amount) AS total_sales \
                    FROM transactions_table \
                    GROUP BY DATE_FORMAT(transaction_date,'%Y')")
    result = cursor.fetchall()
    return_dict = {}
    for r in result:
        return_dict[r[0]] = str(r[1])
    return_result = jsonify({'revenue': return_dict}) 
    return return_result
    
# Function to query the database for active user count per year. Active user is defined as one who has atleast one transaction in a given year. Returns jsonified active user count
def get_active_user_count(cursor):
    cursor.execute("SELECT DATE_FORMAT(transaction_date,'%Y') AS transaction_year,COUNT(DISTINCT user) AS user_count FROM \
                    transactions_table \
                    GROUP BY DATE_FORMAT(transaction_date,'%Y') ")
    result = cursor.fetchall()
    return_dict = {}
    for r in result:
        return_dict[r[0]] = str(r[1])
    return_result = jsonify({'activeusers': return_dict}) 
    return return_result   
  
#Function to query the database for count of new users per year. Returns jsonified new user count
def get_new_user_count(cursor):
    cursor.execute("SELECT  DATE_FORMAT(joining_date,'%Y') AS join_year, COUNT(DISTINCT user) AS user_count \
                    FROM transactions_table \
                    WHERE DATE_FORMAT(joining_date,'%Y') IS NOT NULL\
                    GROUP BY DATE_FORMAT(joining_date,'%Y')")
    result = cursor.fetchall()
    return_dict = {}
    for r in result:
        return_dict[r[0]] = str(r[1])
    return_result = jsonify({'newusercount': return_dict}) 
    return return_result 


#Function to query the database for revenue per active user count. Returns jsonified active user count. 
def get_average_revenue_per_active_user(cursor):
    cursor.execute("SELECT transaction_year, total_sales/active_user_count AS arpau FROM \
                    (SELECT DATE_FORMAT(transaction_date,'%Y') AS transaction_year , \
                    COUNT(DISTINCT user) AS active_user_count, SUM(sales_amount) AS total_sales FROM \
                    transactions_table \
                    GROUP BY DATE_FORMAT(transaction_date,'%Y') ) AS T1 ")
    result = cursor.fetchall()
    return_dict = {}
    for r in result:
        return_dict[r[0]] = str(r[1])
    return_result = jsonify({'average_revenue_per_active_user': return_dict}) 
    return return_result
    

#Function to get the requested metric from the user, query the database for the corresponding metric and display the result.  
@app.route('/<resource_id>/', methods=['GET'])
def get_resource(resource_id):
    cnx,cursor = connect_db('transactions')
    resources = ['revenue','activeusers','newusercount','arpau']
    if resource_id in resources:
        switcher = {
        'revenue': get_revenue(cursor),
        'activeusers': get_active_user_count(cursor),
        'newusercount': get_new_user_count(cursor),
        'arpau': get_average_revenue_per_active_user(cursor)
        }
        result = switcher.get(resource_id)
        cursor.close()
        cnx.close()
        return result
        
    else:
	help_result = {"To get revenue": 'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/revenue/',
                   "To get active users": 'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/activeusers/',
                   "To get new user count":'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/newusercount/',
                   "To get average revenue per active user": 'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/arpau/'
                  }
        return jsonify({'Inavlid url. Following are the valid urls': help_result})


#Function to handle URLs with no metrics specified. Returns jsonified valid results
@app.route('/')
def show_url():
    help_result = {"To get revenue": 'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/revenue/',
                   "To get active users": 'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/activeusers/',
                   "To get new user count":'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/newusercount/',
                   "To get average revenue per active user": 'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/arpau/'
                  }
    result = jsonify({'Specify the metric. Following are the valid urls': help_result})
    return result    


#Function to handle non existent resource request. Returns jsonified valid results
@app.errorhandler(404)
def not_found(error):
    help_result = {"To get revenue": 'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/revenue/',
                   "To get active users": 'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/activeusers/',
                   "To get new user count":'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/newusercount/',
                   "To get average revenue per active user": 'http://ec2-54-191-103-14.us-west-2.compute.amazonaws.com:8500/arpau/'
                  }
    return make_response(jsonify({'Error not found. Following are the valid urls':help_result}), 404)        

# Run the appication  
if __name__ == '__main__':
    app.run(host='ec2-54-191-103-14.us-west-2.compute.amazonaws.com',port='8500')
