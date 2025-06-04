# Test for sub-folder changes
from fastapi import FastAPI, Depends, Path, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from dotenv import load_dotenv
import logging


import pydggsapi.request_example as request_example
from pydggsapi.routers import dggs_api
from pydggsapi.routers import tiles_api

import os

load_dotenv()

app = FastAPI()
# initialize logging for Fastapi

# Setting up CORS
origins = os.environ.get('CORS', [])

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(dggs_api.router, prefix='/dggs-api/v1-pre')
app.include_router(tiles_api.router, prefix='/tiles-api')


# set up logging for app as console output
logging.basicConfig(level=logging.INFO)


# set up a landing page GET /

######################################################
# openapi
# https://swagger.io/docs/specification/describing-parameters/#header-parameters
# https://www.linode.com/docs/guides/documenting-a-fastapi-app-with-openapi/
######################################################

def my_schema():
    openapi_schema = get_openapi(
        title="HyTruck Spatial Planning Toolkit",
        version="1.0-beta",
        routes=app.routes
    )

    openapi_schema["info"] = {
        "title" : "HyTruck Spatial Planning Toolkit",
        "version" : "1.0-beta",
        "description" : "The HyTruck project helps public authorities design a network of hydrogen refuelling stations for large trucks, bringing the region closer to zero–emissions in road freight transport.",
        "termsOfService": "https://interreg-baltic.eu/project/hytruck/",
        "contact": {
            "name": "Contact project lead",
            "url": "https://landscape-geoinformatics.ut.ee/projects/hytruck/",
            "email": "alexander.kmoch@ut.ee"
        },
        "license": {
            "private": True,
            "name": "All rights reserved.",
            "url": "https://landscape-geoinformatics.ut.ee/team/"
        },
    }

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = my_schema


