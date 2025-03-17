import sys
import os

from fastapi import FastAPI

from .app import app, initialize_app

application = None

def init_application() -> FastAPI:
    """Initialize the WSGI application if not already initialized"""
    global application
    if application is None:
        application = initialize_app()
    return application

init_application()