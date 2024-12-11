# here should be DGGRID related functions and methods
# DGGRID ISEA7H resolutions
from abc import ABC, abstractmethod


class AbstractDGGRS(ABC):

    @abstractmethod
    def zoneinfo(self, cellIds: list, dggrsId):
        pass

