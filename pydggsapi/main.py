from gunicorn.app.wsgiapp import WSGIApplication
# from gunicorn.app.base import BaseApplication
from dotenv import load_dotenv
from pydggsapi.dependencies.authentication import session
from multiprocessing import Manager
import os

# Reference from :
# https://stackoverflow.com/questions/70396641/how-to-run-gunicorn-inside-python-not-as-a-command-line


class StandaloneApplication(WSGIApplication):
    def __init__(self, app_uri, options=None, storage=None):
        self.options = options or {}
        self.app_uri = app_uri
        session.initial_data(storage)
        super().__init__()

    def load_config(self):
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)


def run():
    load_dotenv()
    bind = os.environ.get('bind', '0.0.0.0:8454')
    workers = os.environ.get('workers', 4)
    options = {
        "bind": bind,
        "workers": workers,
        "worker_class": "uvicorn.workers.UvicornWorker",
    }
    StandaloneApplication("dataserv.api:app", options, Manager().dict()).run()


if __name__ == '__main__':
    load_dotenv()
    bind = os.environ.get('bind', '0.0.0.0:8454')
    workers = os.environ.get('workers', 4)
    options = {
        "bind": bind,
        "workers": workers,
        "worker_class": "uvicorn.workers.UvicornWorker",
    }
    StandaloneApplication("pydggsapi.api:app", options, Manager().dict()).run()
