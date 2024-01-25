import csv
import socket
import json
import threading
from typing import List
from pathlib import Path


class Database:
    def __init__(self, filePath: str):
        self.filePath = filePath
        self.currentTableName = ""
        self.currentTableData = []

    def writeCsvFile(self, fileName: str, rowData: list):
        with open(self.filePath + fileName, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerows(self.currentTableData)
                csvfile.close()

    def readCsvFile(self, tableName: str):
        rows = []

        # Open the CSV file for reading
        fileName = tableName + ".csv"
        with open(self.filePath + fileName, mode='r') as csvfile:
            # Create a CSV reader with DictReader
            csvreader = csv.reader(csvfile)
            # extracting each data row one by one
            for row in csvreader:
                rows.append(row)
        
            csvfile.close()
        return rows

    def getColumnNames(self, tableName: str):
        
        if (len(tableName) > 0):
            tmp_rowData = self.readCsvFile(tableName=tableName)
            return tmp_rowData[0]        
        return ['']

    def createTable(self, tableName: str, columnNames: List[str]):

        fileName = tableName + ".csv"
        path = Path(self.filePath + fileName)
        if (len(tableName) <= 0):
            return "No Tablename"      
        elif (path.is_file()):
            return "Already Exists"     
        else:
            with open(self.filePath + fileName, 'w', newline='') as csvfile:
                # creating a csv dict writer object
                writer = csv.DictWriter(csvfile, fieldnames=columnNames)
                writer.writeheader()
                csvfile.close()

            self.currentTableName = tableName
            self.currentTableData = self.readCsvFile(tableName=tableName)
            return "Created"

    def insertRow(self, tableName: str, rowData: List[str]):

        fileName = tableName + ".csv"
        path = Path(self.filePath + fileName)
        if (len(tableName) <= 0):
            return "No Tablename" 
        elif (len(rowData) <= 0):
            return "No Data"
        elif (not path.is_file()):
            return "Does Not Exists"
        else:

            if (tableName != self.currentTableName):
                self.currentTableData = self.readCsvFile(tableName=tableName)
            
            self.currentTableData.append(rowData)
            threading.Thread(target=self.writeCsvFile(fileName=fileName, rowData=self.currentTableData))

            return "Inserted"

    def updateRowByIndex(self, tableName: str, rowNumber: int, rowData: List[str]):

        fileName = tableName + ".csv"
        path = Path(self.filePath + fileName)
        if (len(tableName) <= 0):
            return "No Tablename" 
        elif (rowNumber <= 0):
            return "Invalid rowNumber"
        elif (not path.is_file()):
            return "Does Not Exists"
        else:
            
            if (tableName != self.currentTableName):
                self.currentTableData = self.readCsvFile(tableName=tableName)

            if (len(self.currentTableData) > rowNumber -1):
                
                self.currentTableData[rowNumber] = rowData
                threading.Thread(target=self.writeCsvFile(fileName=fileName, rowData=self.currentTableData))

                return "Updated"
            else:
                return "No Rows present"  

    def updateRowByKeyValue(self, tableName: str, key: str, equals: str, rowData: List[str]):
        
        fileName = tableName + ".csv"
        path = Path(self.filePath + fileName)
        if (len(tableName) <= 0):
            return "No Tablename" 
        elif (len(key) <= 0):
            return "Invalid key"
        elif (not path.is_file()):
            return "Does Not Exists"
        else:
            
            if (tableName != self.currentTableName):
                self.currentTableData = self.readCsvFile(tableName=tableName)

            tableColumnNames = self.getColumnNames(tableName=tableName)
            columnIndex = tableColumnNames.index(key)
            
            for row in self.currentTableData:
                if (row[columnIndex] == equals):
                    row = rowData

            threading.Thread(target=self.writeCsvFile(fileName=fileName, rowData=self.currentTableData))

            return "Updated" 

    def getRowByIndex(self, tableName: str, rowNumber: int):

        fileName = tableName + ".csv"
        path = Path(self.filePath + fileName)
        if (len(tableName) <= 0):
            return "No Tablename" 
        elif (rowNumber <= 0):
            return "Invalid key"
        elif (not path.is_file()):
            return "Does Not Exists"
        else:

            if (tableName != self.currentTableName):
                self.currentTableData = self.readCsvFile(tableName=tableName)

            if (len(self.currentTableData) >= rowNumber -1):
                return self.currentTableData[rowNumber]
            
        return ['']    
    
    def getRowsByKeyValue(self, tableName: str, key: str, equals: str):
        
        fileName = tableName + ".csv"
        path = Path(self.filePath + fileName)
        if (len(tableName) <= 0):
            return "No Tablename" 
        elif (len(key) <= 0):
            return "Invalid key"
        elif (len(equals) <= 0):
            return "Invalid equals"
        elif (not path.is_file()):
            return "Does Not Exists"
        else:

            if (tableName != self.currentTableName):
                self.currentTableData = self.readCsvFile(tableName=tableName)

            tableColumnNames = self.getColumnNames(tableName=tableName)
            columnIndex = tableColumnNames.index(key)

            retData = []
            for row in self.currentTableData:
                if (row[columnIndex] == equals):
                    retData.append(row)

            return retData   

    def deleteRowByIndex(self, tableName: str, rowNumber: int):

        fileName = tableName + ".csv"
        path = Path(self.filePath + fileName)
        if (len(tableName) <= 0):
            return "No Tablename" 
        elif (rowNumber <= 0):
            return "Invalid rowNumber"
        elif (not path.is_file()):
            return "Does Not Exists"
        else:

            if (tableName != self.currentTableName):
                self.currentTableData = self.readCsvFile(tableName=tableName)

            if (len(self.currentTableData) >= rowNumber -1):
                del self.currentTableData[rowNumber]

                fileName = tableName + ".csv"
                threading.Thread(target=self.writeCsvFile(fileName=fileName, rowData=self.currentTableData))

                return "Deleted"  

    def deleteRowsByKeyValue(self, tableName: str, key: str, equals: str):
        
        fileName = tableName + ".csv"
        path = Path(self.filePath + fileName)
        if (len(tableName) <= 0):
            return "No Tablename" 
        elif (len(key) <= 0):
            return "Invalid key"
        elif (not path.is_file()):
            return "Does Not Exists"
        else:

            if (tableName != self.currentTableName):
                self.currentTableData = self.readCsvFile(tableName=tableName)

            tableColumnNames = self.getColumnNames(tableName=tableName)
            columnIndex = tableColumnNames.index(key)

            for row in self.currentTableData:
                if (row[columnIndex] == equals):
                    del row

            fileName = tableName + ".csv"
            threading.Thread(target=self.writeCsvFile(fileName=fileName, rowData=self.currentTableData))

            return "Deleted"



class ServerInterface:
    def __init__(self, ip: str, port: int):
        self.IP = ip
        self.PORT = port
        self.Connection = socket 

    def createSocket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        s.bind(('', self.PORT))
        
        # put the socket into listening mode 
        s.listen(5)
        #print ("socket is listening")
        
        # Establish connection with client. 
        c, addr = s.accept()
        self.Connection = c

    def receiveRequest(self):
        
        msg = self.Connection.recv(1024).decode()
        #print(msg)
        cmd = json.loads(msg)

        return cmd

    def executeRequest(self, dataBase: Database, json: str):

        retMsgDict = {}

        match json["Operation"]:
            case "Create":
                status = dataBase.createTable(json["TableName"],json["ColumnNames"])
                retMsgDict["Operation"] = json["Operation"]
                if (status == "Created"):
                    retMsgDict["Status"] = "200"
                elif (status == "Already Exists"):
                    retMsgDict["Status"] = "201"
                else:
                    retMsgDict["Status"] = "001"
            
            case "Insert":
                status = dataBase.insertRow(json["TableName"], json["rowData"])
                retMsgDict["Operation"] = json["Operation"]
                if (status):
                    retMsgDict["Status"] = "200"
                else:
                    retMsgDict["Status"] = "002"
            
            case "UpdateByIndex":
                status = dataBase.updateRowByIndex(json["TableName"], json["Index"], json["rowData"])
                retMsgDict["Operation"] = json["Operation"]
                if (status):
                    retMsgDict["Status"] = "200"
                else:
                    retMsgDict["Status"] = "003"
            
            case "UpdateByKeyValue":
                status = dataBase.updateRowByKeyValue(json["TableName"], json["Key"], json["Equals"], json["rowData"])
                retMsgDict["Operation"] = json["Operation"]
                if (status):
                    retMsgDict["Status"] = "200"
                else:
                    retMsgDict["Status"] = "004"
            
            case "GetRowByIndex":
                rowData = dataBase.getRowByIndex(json["TableName"], json["Index"])
                retMsgDict["Operation"] = json["Operation"]
                if (len(rowData) > 0):
                    retMsgDict["Status"] = "200"
                else:
                    retMsgDict["Status"] = "005"

                retMsgDict["Data"] = rowData
            
            case "GetRowsByKeyValue":
                rowData = dataBase.getRowsByKeyValue(json["TableName"], json["Key"], json["Equals"])
                retMsgDict["Operation"] = json["Operation"]
                if (len(rowData) > 0):
                    retMsgDict["Status"] = "200"
                else:
                    retMsgDict["Status"] = "006"

                retMsgDict["Data"] = rowData
            
            case "DeleteRowByIndex":
                status = dataBase.deleteRowByIndex(json["TableName"], json["Index"])
                retMsgDict["Operation"] = json["Operation"]
                if (status):
                    retMsgDict["Status"] = "200"
                else:
                    retMsgDict["Status"] = "007"
            
            case "DeleteRowsByKeyValue":
                status = dataBase.deleteRowsByKeyValue(json["TableName"], json["Key"], json["Equals"])
                retMsgDict["Operation"] = json["Operation"]
                if (status):
                    retMsgDict["Status"] = "200"
                else:
                    retMsgDict["Status"] = "008"
            
            case default:
                retMsgDict["Operation"] = json["Operation"]
                retMsgDict["Status"] = "000"

        return retMsgDict

    def sendRequestData(self, responceJson: dict):

        # send a thank you message to the client. encoding to send byte type. 
        jsonStr = json.dumps(responceJson)
        self.Connection.send(jsonStr.encode()) 
            
        # Close the connection with the client 
        self.Connection.close()



class ClientInterface:
    def __init__(self, ip: str, port: int):
        self.IP = ip
        self.PORT = port

    def sendRequest(self, msgJson: dict):
        # Create a socket object 
        s = socket.socket()                      
        
        # connect to the server on local computer 
        s.connect((self.IP, self.PORT)) 
        
        # send data after it is converted from dict to json string
        s.send( json.dumps(msgJson).encode() )
        
        # receive data from the server and decoding to get the string.
        msgJson = json.loads( s.recv(1024).decode() )

        # close the connection 
        s.close()     
        return msgJson

    def createTable(self, tableName: str, columnNames: list):
        jsonDict = {}
        jsonDict['Operation'] = "Create"
        jsonDict['TableName'] = tableName
        jsonDict['ColumnNames'] = columnNames
        return self.sendRequest(jsonDict)

    def InsertRow(self, tableName: str, rowData: list):
        jsonDict = {}
        jsonDict['Operation'] = "Insert"
        jsonDict['TableName'] = tableName
        jsonDict['rowData'] = rowData
        return self.sendRequest(jsonDict)

    def UpdateRowByIndex(self, tableName: str, rowData: list, index: int):
        jsonDict = {}
        jsonDict['Operation'] = "UpdateRowByIndex"
        jsonDict['TableName'] = tableName
        jsonDict['rowData'] = rowData
        jsonDict['Index'] = index
        return self.sendRequest(jsonDict)

    def UpdateRowsByKeyValue(self, tableName: str, rowData: list, key: str, equals: str):
        jsonDict = {}
        jsonDict['Operation'] = "UpdateRowByKeyValue"
        jsonDict['TableName'] = tableName
        jsonDict['rowData'] = rowData
        jsonDict['Key'] = key
        jsonDict['Equals'] = equals
        return self.sendRequest(jsonDict)

    def GetRowByIndex(self, tableName: str, index: int):
        jsonDict = {}
        jsonDict['Operation'] = "GetRowByIndex"
        jsonDict['TableName'] = tableName
        jsonDict['Index'] = index
        return self.sendRequest(jsonDict)

    def GetRowsByKeyValue(self, tableName: str, key: str, equals: str):
        jsonDict = {}
        jsonDict['Operation'] = "GetRowsByKeyValue"
        jsonDict['TableName'] = tableName
        jsonDict['Key'] = key
        jsonDict['Equals'] = equals
        return self.sendRequest(jsonDict)

    def DeleteRowByIndex(self, tableName: str, index: int):
        jsonDict = {}
        jsonDict['Operation'] = "DeleteRowByIndex"
        jsonDict['TableName'] = tableName
        jsonDict['Index'] = index
        return self.sendRequest(jsonDict)

    def DeleteRowsByKeyValue(self, tableName: str, key: str, equals: str):
        jsonDict = {}
        jsonDict['Operation'] = "DeleteRowsByKeyValue"
        jsonDict['TableName'] = tableName
        jsonDict['Key'] = key
        jsonDict['Equals'] = equals
        return self.sendRequest(jsonDict)

