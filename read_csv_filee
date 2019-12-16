import csv
import mysql.connector
from mysql.connector import Error

try:
   
    mydb = mysql.connector.connect(host="localhost",user="root",passwd="3396",database="studentdetails")
    mycursor = mydb.cursor()
    
    with open('F:\pythonfiles\Day1.csv', 'r') as csvfile1:
     #if we use dictreader then column name is used as key
        reader1 = csv.DictReader(csvfile1)
        for row in reader1:

            #insert for student table
            sqlquery = "SELECT * FROM student WHERE Id = %s"
            val = (row['Id'], )
            mycursor.execute(sqlquery,val)
            result=mycursor.fetchall()
            checkct=mycursor.rowcount
            
            if checkct == 0:
                sqlquery = "INSERT INTO student(Id, Nm) VALUES (%s, %s)"
                val = (row['Id'], row['Nm'])
                mycursor.execute(sqlquery, val)
                mydb.commit()
            #if already present then just update name if there is any spell change     
            else:
                sqlquery = "UPDATE student SET Nm = %s WHERE Id = %s"
                val = (row['Nm'], row['Id'], )
                mycursor.execute(sqlquery, val)
                mydb.commit()
                                     
            #insert for address table
            sqlquery = "SELECT * FROM address WHERE AddrId = %s AND Id = %s"
            val = (row['AddrId'], row['Id'], )
            mycursor.execute(sqlquery,val)
            myresult=mycursor.fetchall()
            checkct=mycursor.rowcount
            
            if checkct == 0:
                sqlquery = "SELECT * FROM address WHERE Id = %s"
                val = (row['Id'], )
                mycursor.execute(sqlquery,val)
                myresult=mycursor.fetchall()
                checkidct=mycursor.rowcount

                #insert a new record for newly added student 
                if checkidct == 0:
                    sqlquery = "INSERT INTO address(AddrId, Addr1, Addr2, City, State, Country, PostalCd, ContactNumber, Id, active_ind) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    val = (row['AddrId'], row['Addr1'], row['Addr2'], row['City'], row['State'], row['Country'], row['PostalCd'], row['ContactNumber'], row['Id'], "Y")
                    mycursor.execute(sqlquery, val)
                    mydb.commit()

                #change old address as 'N' and insert new address as new record with 'Y'  
                else:
                    sqlquery = "UPDATE address SET active_ind='N' WHERE Id = %s"
                    val = (row['Id'], )
                    mycursor.execute(sqlquery, val)
                    mydb.commit()
                     
                    sqlquery = "INSERT INTO address(AddrId, Addr1, Addr2, City, State, Country, PostalCd, ContactNumber, Id, active_ind) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    val = (row['AddrId'], row['Addr1'], row['Addr2'], row['City'], row['State'], row['Country'], row['PostalCd'], row['ContactNumber'], row['Id'], "Y")
                    mycursor.execute(sqlquery, val)
                    mydb.commit()
            
            #change any of the old address with 'N' as 'Y' and make current address with 'Y' as 'N'
            else:
                sqlquery = "SELECT * FROM address WHERE AddrId = %s AND Id = %s AND active_ind = 'N'"
                val = (row['AddrId'], row['Id'], )
                mycursor.execute(sqlquery,val)
                myresult=mycursor.fetchall()
                checkactive=mycursor.rowcount

                if checkactive == 1:
                    sqlquery = "UPDATE address SET active_ind='N' WHERE Id = %s"
                    val = (row['Id'], )
                    mycursor.execute(sqlquery, val)
                    mydb.commit()

                    sqlquery = "UPDATE address SET active_ind='Y' WHERE Id = %s AND AddrId = %s"
                    val = (row['Id'],row['AddrId'], )
                    mycursor.execute(sqlquery, val)
                    mydb.commit()
                    
            #insert for attendance table
            sqlquery = "SELECT * FROM attendance WHERE AttendanceKey = %s"
            val = (row['AttendanceKey'], )
            mycursor.execute(sqlquery,val)
            result=mycursor.fetchall()
            checkct=mycursor.rowcount
            
            if checkct == 0:                
                sqlquery = "INSERT INTO attendance(AttendanceKey, AttendanceDate, AttendedYesNo, Id) VALUES (%s, %s, %s, %s)"
                val = (row['AttendanceKey'], row['AttendanceDate'], row['AttendedYesNo'], row['Id'])
                mycursor.execute(sqlquery, val)
                mydb.commit()

            #if record already present then just update only attendance     
            else:
                sqlquery = "UPDATE attendance SET AttendedYesNo = %s WHERE AttendanceKey = %s"
                val = (row['AttendanceKey'], )
                mycursor.execute(sqlquery, val)
                mydb.commit()
                                                            
except Error as e:
    print("Error inserting the data from MySQL table", e)
finally:
    if (mydb.is_connected()):
        mydb.close()
        mycursor.close()


