from datetime import datetime
from flask import render_template, Blueprint, request, flash, redirect, url_for, session
from werkzeug.security import generate_password_hash, check_password_hash
from aew import dbemployee
from aew.cdata.dbCData import dropTempTable


auth=Blueprint('auth', __name__)

@auth.route('/register',methods=['GET', 'POST'])
def register():
    if request.method=='POST':
        email=request.form.get('email')
        fname=request.form.get('firstName')
        lname=request.form.get('lastName')
        pword1=request.form.get('password1')
        pword2=request.form.get('password2')

        rowCount=dbemployee.checkUser(email)

        if rowCount!=0:
            flash('eMail already registered.', category='error')
        elif len(email)==0:
            flash('Provide eMail.', category='error')
        elif email.find("@aeweng.com")==-1:
            flash('Invalid eMail', category='error')
        elif len(fname)==0:
            flash('Fill in first name.', category='error')
        elif len(lname)==0:
            flash('Fill in last name.', category='error')
        elif pword1!= pword2:
            flash('Passwords don\'t match', category='error')
        elif len(pword1)<8:
            flash('Password must be at least 8 characters.', category='error')
        else:
            new_pword=generate_password_hash(pword1, method='sha256', salt_length=40)

            result=dbemployee.addUser(email, new_pword, fname, lname)

            if result==1:
                flash('Account created', category='success')
                session['loggedin']=True
                session['username']=fname + " "+ lname
                session['tempuname']=lname

                return redirect(url_for('views.home'))


    return render_template("register.html")



@auth.route('/login',methods=['GET','POST'])
def login():
    if request.method=='POST':
        
        email=request.form.get('email')
        pword=request.form.get('password')

        dbresult=dbemployee.loginUser(email)

        if dbresult!=None:
                    
            if check_password_hash(dbresult[2],pword):
                flash('Log in successfully', category="success")
            
                session['loggedin']=True
                session['username']=dbresult[3] + " " + dbresult[4]
                session['tempuname']=dbresult[4]
            
                return redirect(url_for('views.home'))
            
            else:
                flash('Incorrect passowrd, try again', category='error')
                
        else:
            flash('No email registered', category='error')

    return render_template("login.html")

@auth.route('/logout')
def logout():
 #  myTable='tempLabData_'+session.get('tempuname',None)
    
 #   dropTempTable(myTable)
    session['loggedin']=False
    session['username']=""
    return redirect(url_for('views.home'))