from CSV_DatabaseLib import ClientInterface
import atexit
import time

'''
reqCreateMsgJson = '{ "Operation":"Create", "TableName":"test13", "ColumnNames":["col1","col2","col3","col4"] }'
reqInsertMsgJson = '{ "Operation":"Insert", "TableName":"test13", "rowData":["col1_Data1", "col2_Data2", "col3_Data3","col4_Data4"]}'
reqUpdateByIndexMsgJson = '{ "Operation":"UpdateByIndex", "TableName":"test1", "Index":3, "rowData":["UpdateIdx1", "UpdateIdx2", "UpdateIdx3","UpdateIdx4"]}'
reqUpdateByKeyValueMsgJson = '{ "Operation":"UpdateByKeyValue", "TableName":"test1", "Key":"key", "Equals":"equals", "rowData":["UpdateKey1", "UpdateKey2", "UpdateKey3","UpdateKey4"]}'
reqGetRowByIndexMsgJson = '{ "Operation":"GetRowByIndex", "TableName":"test1", "Index":"index"}'
reqGetRowsByKeyValueMsgJson = '{ "Operation":"GetRowByKeyValue", "TableName":"test1", "Key":"key", "Equals":"equals"}'
reqDeleteRowByIndexMsgJson = '{ "Operation":"DeleteRowByIndex", "TableName":"test1", "Index":"index"}'
reqDeleteRowsByKeyValueMsgJson = '{ "Operation":"DeleteRowByKeyValue", "TableName":"test1", "Key":"key", "Equals":"equals"}'
'''


def main():
    interface = ClientInterface(ip='127.0.0.2', port=12345)
    atexit.register(interface.Connection.close())

    tableName = "test20"
    colunmNames = ["Col1", "col2", "col3", "col4"]
    msgJson = interface.createTable(tableName=tableName, columnNames=colunmNames)

    startTime = round(time.time() * 1000)

    if (msgJson['Status'] == '001'):
        print("Database table could not be created")
        return 0

    data = []
    iterationMax = 100
    for i in range(1,iterationMax,1):
    
        data = ['data' + str(i),'data' + str(i+1),'data' + str(i+2),'data' + str(i+3)]
        msgJson = interface.InsertRow(tableName=tableName,rowData=data)

        #msgJson = interface.GetRowByIndex(tableName=tableName, index=i)
        #data.append(msgJson['Data'])


    endTime = round(time.time() * 1000)
    print("Total time[s] = " + str((endTime - startTime) / 1000))
    print("Time/operation[ms] = " + str(((endTime - startTime) / iterationMax)))

    #print("data[99]: " + str(data[999]))

main()