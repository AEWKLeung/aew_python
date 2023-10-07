"""
The flask application package.
"""

from flask import Flask
from aew.cdata.cdata import cdata_bp


def create_app():
    app=Flask(__name__)
    app.config['SECRET_KEY']= '3490939342 0dppklldert'
    from .views import views
    from .auth import auth

    app.register_blueprint(views, url_prefix='/')
    app.register_blueprint(auth, url_prefix='/')
    app.register_blueprint(cdata_bp, url_prefix='/')

    return app
    
