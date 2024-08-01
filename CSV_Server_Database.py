import pathlib
import socket
import time
import sys
from CSV_DatabaseLib import Database  # type: ignore
from CSV_DatabaseLib import ServerInterface # type: ignore


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't have to be reachable
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
        #ip = "127.0.0.1"
        print("IP = " + ip)
        print("PORT = " + str(port))
    
    # Create interface object
    interface = ServerInterface(ip=ip, port=port)

    # Get path to current file's directory
    filePath = str(pathlib.Path(__file__).parent.resolve()) + "\\"

    # Create database object
    database = Database(filePath=filePath) 

    while (True):
           
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