import pathlib
from CSV_DatabaseLib import Database
from CSV_DatabaseLib import ServerInterface

def main():

    # Get path to current file's directory
    filePath = str(pathlib.Path(__file__).parent.resolve()) + "\\"

    # Create database object
    database = Database(filePath=filePath) 

    # Create interface object
    interface = ServerInterface("127,0,0,1", 12345)
    
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
        interface.createSocket()

        # Receive data when client connects to socket
        jsonMsg = interface.receiveRequest()

        # Decode Json message and execute Database commands
        msgDict = interface.executeRequest(dataBase=database, json=jsonMsg)

        # Send Json message return with Status and Data : retMsg = '{ "Operation":string,"Status":int, "data":List }'
        interface.sendRequestData(msgDict)
        
        


main()