import pathlib
import socket
import time
import sys
from CSV_DatabaseLib import Database
from CSV_DatabaseLib import ServerInterface


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('192.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP



def main():

    try:
        # Get program argument for PORT number
        port = int(sys.argv[1])
    except:
        port = 12345
        print("Default PORT parameter has been used : " + str(port))
    finally:
        # Get local lan IP
        ip = get_local_ip()
        print("IP = " + ip)
        print("PORT = " + str(port))
    
    # Create interface object
    interface = ServerInterface(ip=ip, port=port)

    # Get path to current file's directory
    filePath = str(pathlib.Path(__file__).parent.resolve()) + "\\"

    # Create database object
    database = Database(filePath=filePath) 

    while (True):
    
        ''' Request message examples
        reqCreateMsgJson = '{ "Operation":"Create", "TableName":"test1", "ColumnNames":["col1","col2","col3","col4"] }'
        reqInsertMsgJson = '{ "Operation":"Insert", "TableName":"test1", "rowData":["col1_Data1", "col2_Data2", "col3_Data3","col4_Data4"]}'
        reqUpdateByIndexMsgJson = '{ "Operation":"UpdateByIndex", "TableName":"test1", "Index":"index", "rowData":["col1_Data1", "col2_Data2", "col3_Data3","col4_Data4"]}'
        reqUpdateByKeyValueMsgJson = '{ "Operation":"UpdateByKeyValue", "TableName":"test1", "Key":"key", "Equals":"equals", "rowData":["col1_Data1", "col2_Data2", "col3_Data3","col4_Data4"]}'
        reqGetRowByIndexMsgJson = '{ "Operation":"GetRowByIndex", "TableName":"test1", "Index":"index"}'
        reqGetRowsByKeyValueMsgJson = '{ "Operation":"GetRowByKeyValue", "TableName":"test1", "Key":"key", "Equals":"equals"}'
        reqDeleteRowByIndexMsgJson = '{ "Operation":"DeleteRowByIndex", "TableName":"test1", "Index":"index"}'
        reqDeleteRowsByKeyValueMsgJson = '{ "Operation":"DeleteRowByKeyValue", "TableName":"test1", "Key":"key", "Equals":"equals"}'
        '''
           
        # Create socket and wait for client connection
        isConnected = interface.createSocket()

        if (isConnected):
            # Receive data when client connects to socket
            jsonDict = interface.receiveRequest()

            if (len(jsonDict) > 0):
                # Decode Json message and execute Database commands
                msgDict = interface.executeRequest(dataBase=database, jsonDict=jsonDict)
            else:
                msgDict = {"Operation":"Socket Failed","Status":0}
            # Send Json message return with Status and Data : retMsg = '{ "Operation":string,"Status":int, "data":List }'
            interface.sendRequestData(msgDict)
        else:
            time.sleep(5)
        


main()