import atexit
import pathlib
import time
import sys
from CSV_DatabaseLib import Database
from CSV_DatabaseLib import ServerInterface

def main():
    # Get program arguments
    try:
        ip = str(sys.argv[1])
    except:
        print("Please provide IP parameter ex. 127.0.0.1")
        #exit
        ip = "127.0.0.1"
    try:
        port = int(sys.argv[2])
    except:
        print("Please provide PORT parameter ex. 12345")
        #exit
        port = 12345
    
    print("IP = " + ip)
    print("PORT = " + str(port))
    
    # Create interface object
    interface = ServerInterface("127.0.0.2", 12345)

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