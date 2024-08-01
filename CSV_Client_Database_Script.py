from CSV_DatabaseLib import ClientInterface # type: ignore
import ipaddress
import socket
import time
import sys


def getIpFromDeviceName(ipAddr: str, searchRange: range) -> str:
    remoteDeviceIP = ""
    for i in range(1,256):
        try:
            ipStr = "192.168.0." + str(i)
            print(ipStr)
            net4 = ipaddress.ip_network(ipStr)
            hostname = socket.gethostbyaddr(str(net4.hosts()[0]))[0]
            print(hostname)
            if (hostname == "raspberrypi"):
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
        #ip = getIpFromDeviceName("192.168.0", (1,255))
        #ip = "192.168.0.56"
        ip = "127.0.0.1"


    print("IP = " + ip)
    print("PORT = " + str(port))

    # Create interface object
    interface = ClientInterface(ip=ip, port=port)

    tableName = "test20"
    colunmNames = ["Col1", "col2", "col3", "col4"]
    msgJson = interface.createTable(tableName=tableName, columnNames=colunmNames)

    if (msgJson['Status'] == '200'):
        print("Database table has been created")
    elif (msgJson['Status'] == '201'):
        print("Database table already exists")
    else:
        print("Database table could not be created")
        exit()
    
    data = []
    msgJson.clear()
    iterationMax = 10
    for i in range(1,iterationMax,1):
        """Insert Row data"""
        #data = ['data' + str(i),'data' + str(i+1),'data' + str(i+2),'data' + str(i+3)]
        #msgJson = interface.InsertRow(tableName=tableName,rowData=data)

        """Retrive Row by index"""
        msgJson = interface.GetRowByIndex(tableName=tableName, index=i)
        if (msgJson["Status"] == '200'):
            data.append(msgJson['Data'])
            print("data: " + str(msgJson['Data']))
        else:
            break
        
    
#############################
#### Code starting point ####
if __name__ =="__main__":
    main()