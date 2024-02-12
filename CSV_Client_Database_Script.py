from CSV_DatabaseLib import ClientInterface
import ipaddress
import socket
import time
import sys



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

def getIpFromDeviceName(deviceName: str, networkAddr: str, searchRange: range) -> str:
    remoteDeviceIP = ""
    for i in range(searchRange.start, searchRange.stop):
        try:
            ipStr = ipAddr + str(i)
            print(ipStr)
            net4 = ipaddress.ip_network(ipStr)
            hostname = socket.gethostbyaddr(str(net4.hosts()[0]))[0]
            print(hostname)
            if (hostname.find(deviceName) >= 0):
                remoteDeviceIP = str(net4.hosts()[0])
                print(remoteDeviceIP)
                break
        except:
            pass
    return remoteDeviceIP

def main():

    # Get program arguments
    try:
        port = int(sys.argv[1])
    except:
        port = 12345
        print("Invalid PORT number provided in program arguments")
        print("Default PORT = " + str(port))
    try:
        ip = str(sys.argv[2])
    except:
        ip = getIpFromDeviceName(deviceName="raspberrypi" ,networkAddr="192.168.0.", searchRange=range(0,255))
        #ip = "192.168.0.56"
        #ip = "127.0.0.1"

    print("IP = " + ip)
    print("PORT = " + str(port))

    # Create interface object
    interface = ClientInterface(ip=ip, port=port)

    tableName = "test20"
    colunmNames = ["Col1", "col2", "col3", "col4"]
    msgJson = interface.createTable(tableName=tableName, columnNames=colunmNames)
    
    startTime = round(time.time() * 1000)

    if (msgJson['Status'] == '200'):
        print("Database table has been created")
    elif (msgJson['Status'] == '201'):
        print("Database table already exists")
    else:
        print("Database table could not be created")
        return 0

    msgJson = interface.getTableSize(tableName)
    print("#Rows:" + str(msgJson['Data'][0]) + ", " + "#Cols:" + str(msgJson['Data'][1]))

    data = []
    msgJson.clear()
    iterationMax = 10
    for i in range(1,iterationMax,1):
        #data = ['data' + str(i),'data' + str(i+1),'data' + str(i+2),'data' + str(i+3)]
        #msgJson = interface.InsertRow(tableName=tableName,rowData=data)

        msgJson = interface.GetRowByIndex(tableName=tableName, index=i)
        if (msgJson["Status"] == '200'):
            data.append(msgJson['Data'])
            print("data: " + str(msgJson['Data']))
        else:
            break


    endTime = round(time.time() * 1000)
    print("Total time[s] = " + str((endTime - startTime) / 1000))
    print("Time/operation[ms] = " + str(((endTime - startTime) / iterationMax)))

    print("len(data): " + str(len(data)))
    

main()