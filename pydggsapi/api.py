# Test for sub-folder changes
from fastapi import FastAPI, Depends, Path, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from dotenv import load_dotenv
import logging


import pydggsapi.request_example as request_example
from pydggsapi.routers import dggs_api

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

# mapdemopath = os.environ.get('mapdemo', './mapdemo')
# app.mount("/mapdemo", StaticFiles(directory=mapdemopath), name="mapdemo")

#app.include_router(tiles_api.router, prefix='/tiles-api/v1')
app.include_router(dggs_api.router, prefix='/dggs-api/v1-pre')
# TODO: add an asychronous db session pool for tile serving
# ...


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
        title="pydggsapi: A python FastAPI OGC DGGS API implementation",
        version="1.0-beta",
        routes=app.routes
    )

    openapi_schema["info"] = {
        "title" : "pydggsapi: A python FastAPI OGC DGGS API implementation",
        "version" : "1.0-beta",
        "description" : "A python FastAPI OGC DGGS API implementation",
        "termsOfService": "https://creativecommons.org/licenses/by/4.0/",
        "contact": {
            "name": "Contact project lead",
            "url": "https://landscape-geoinformatics.ut.ee/expertise/dggs/",
            "email": "alexander.kmoch@ut.ee"
        },
        "license": {
            "name": "AGPL-3.0",
            "url": "https://www.gnu.org/licenses/agpl-3.0.en.html"
        },
    }

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = my_schema


