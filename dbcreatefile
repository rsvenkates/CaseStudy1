import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="3396"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE studentdetails")

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="3396",
  database="studentdetails"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE TABLE student (Id INT, Nm VARCHAR(255), PRIMARY KEY (Id));")

mycursor.execute("CREATE TABLE address (AddrId INT, Addr1 VARCHAR(255), Addr2 VARCHAR(255), City VARCHAR(255), State VARCHAR(255), Country VARCHAR(255), PostalCd INT(255), ContactNumber VARCHAR(255), Id INT, active_ind VARCHAR(3), PRIMARY KEY (AddrId), FOREIGN KEY (Id) REFERENCES student(Id));")

mycursor.execute("CREATE TABLE attendance (AttendanceKey INT, AttendanceDate VARCHAR(255), AttendedYesNo VARCHAR(3), Id INT, PRIMARY KEY (AttendanceKey), FOREIGN KEY (Id) REFERENCES student(Id));")


