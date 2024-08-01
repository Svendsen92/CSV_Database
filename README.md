# CSV Database


### *Objective*
The objective of the CSV_Database library is to simple way to read and write to a CSV file in a database like maner. It can be used as a database, however, its performance is not comparable to SQL and noSQL databases. It is more a tool to read and write data directly to and from a CSV file in a database like maner over a network connection. 

### *How to get it running*
The Library requires a few packages to be installed.
1. pip install csv
2. pip install socket
3. pip install json
4. pip install threading

Now the library should be ready to be used.

### *How to use to application*
The CSV_Database server can be give an input argument for which port to be used for the TCP/IP connection with clients. An example of a server side setup is shown in [CSV_Server_Database.py](CSV_Server_Database.py)<br>

The CSV_Database client can be set up however one would like and a simple example can be seen in [CSV_Client_Database_Script.py](CSV_Client_Database_Script.py)


### *License* 
This project is licensed under the MIT License - see the [LICENSE.md](https://github.com/Svendsen92/CSV_Database/blob/main/LICENSE) file for details.
