"""
Routes and views for the flask application.
"""

from datetime import datetime
from flask import render_template, Blueprint

views=Blueprint('views', __name__)

@views.route('/')
def home():
    return render_template("index.html")

