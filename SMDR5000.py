import socket
import sys
import pymssql

#connec to the sql server database
conn = pymssql.connect(server="192.168.1.248", user="",password="", port=1433)  # You can lookup the port number inside SQL server.
conn.autocommit(True)
cursor = conn.cursor()

#function to format the data from the smdr stream for input into the database
def FormatSQLInsert(data):
    if str(data[8:13]).strip() != "EXT#":
        sql = "insert into gim.dbo.tblCallLog (typ, ext, number, call_time, duration,update_date) values ("
        sql = sql + "'" + str(data[4:7]).strip() + "',"
        sql = sql + "'"+ str(data[8:13]).strip()+"',"
        sql = sql + "'" + data[20:40].strip() + "',"
        sql = sql + "'" + data[49:54].strip() + "',"
        tmp = data[55:84]
        tmp = tmp.replace("$00.00","")
        tmp = tmp.replace("S=","")
        tmp = tmp.replace("*", "")
        sql = sql + "'" + str(tmp).strip() + "',getdate())"
        return sql
    else:
        return ""

def WriteRecordtoDB(record):
    sql = FormatSQLInsert(record)
    if len(sql)>0:
        cursor.execute(sql)

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Connect the socket to the port where the server is listening
server_address = ('192.168.1.9', 4000)
print >>sys.stderr, 'connecting to %s port %s' % server_address
sock.connect(server_address)
#create and send the login message
message = chr(2)+chr(0)+chr(0)+chr(0)+chr(132)+chr(0)
print >>sys.stderr, 'sending "%s"' % message
sock.sendall(message)
data = sock.recv(86)
while True:
    data = sock.recv(86)
    #print >>sys.stderr, data
    if data:
        print (data)
        WriteRecordtoDB(data)
    else:
        print >>sys.stderr, 'no more data from', server_address
        conn.close()
        break