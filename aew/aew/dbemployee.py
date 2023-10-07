import pyodbc

connString_user='Driver={ODBC Driver 17 for SQL Server};Server=tcp:aew-sql.database.windows.net,1433;Database=AEWEmployees;Uid=kleung300db;Pwd=300Junction;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

connUser=pyodbc.connect(connString_user,autocommit=True)

cursorUser=connUser.cursor()

def checkUser(pemail):
    cursorUser.execute('SELECT * FROM Employees WHERE email = ? ',(pemail))
    return cursorUser.rowcount

def addUser(email, password, fname, lname):
    params=(email, password,fname, lname,1)
    cursorUser.execute('INSERT INTO employees(email,upassword,fname,lname,active) values (?,?,?,?,?)',params)
    return cursorUser.rowcount

def loginUser(email):
    cursorUser.execute('SELECT * FROM Employees WHERE email = ? ',(email))
    return cursorUser.fetchone()
