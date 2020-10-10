# -*- coding: utf-8 -*-
"""
Provide a way to request the Legifrance API.

Only one instance of this class SHOULD use the same login infos at the same
time, or quotas MIGHT be violated.

Classes
-------
LegiConnector
"""
import requests
from requests_oauthlib import OAuth2Session
from time import time, sleep

class LegiConnector:
    """"
    Establish a connection to the Legifrance API to handle requests.
    
    Methods
    -------
    isReady():
        Verify that the connection to the Legifrance API is up and running.
    post(path, payload):
        Send a POST query to the Legifrance API.
    """
    _HOST = "https://sandbox-api.aife.economie.gouv.fr/dila/legifrance-beta/lf-engine-app"
    _TOKEN_URL = 'https://sandbox-oauth.aife.economie.gouv.fr/api/oauth/token'
    _PERIOD = 60 #number of seconds of the quota
    _QUOTA_LIMIT = 100 #max number of requests in a time period
    def __init__(self, client_id, client_secret, dummy=False):
        """
        Establish a connection to the Legifrance API.
        
        Perform the OAuth2 authentication requested by the Legifrance API. The
        object then stands ready to send requests to Legifrance.
        If the provided args do not form a valid pair, the error will not be 
        handled gracefully.

        Parameters
        ----------
        client_id : str
            Client id to request an OAuth2 token from the Legifrance API.
        client_secret : str
            Client secret to request an OAuth2 token from the Legifrance API.

        Returns
        -------
        A LegiConnector object ready to make requests, assuming that the args
        are valid.
        """
        ### TODO: handle invalid login infos
        ### TODO: handle token renewal
        self._dummy = dummy
        self._id = client_id
        self._secret = client_secret
        if not self._dummy:
            res = requests.post(
              self._TOKEN_URL,
              data={
                "grant_type": "client_credentials",
                "client_id": self._id,
                "client_secret": self._secret,
                "scope": "openid"
              }
            )
            token = res.json()
            self._client = OAuth2Session(self._id, token=token)
        self._quotas = []
    
    def _waitIfNeeded(self, path):
        """
        Check if the quota is exceeded and wait the appropriate amount of time.
        
        When this method returns, the ongoing query will respect the server's
        quota.

        Parameters
        ----------
        path : str
            Path to the resource being queried.

        Returns
        -------
        None.
        """
        self._checkQuotas()
        if(len(self._quotas) >= self._QUOTA_LIMIT):
            sleep(self._quotas[0] - int(time()) + 1)
        self._checkQuotas()
        self._quotas.append(int(time()) + self._PERIOD)
    
    def _checkQuotas(self):
        """
        Update the state of the object as regards the quota for the API.
        
        This utility method should only be called by _waitIfNeeded.
        
        Returns
        -------
        None.
        """
        i = 0
        now = time()
        while i < len(self._quotas) and self._quotas[i] < now:
            i += 1
        self._quotas = self._quotas[i:]
    
    def isReady(self):
        """
        Verify that the connection to the Legifrance API is up and running.

        Returns
        -------
        bool
            True if the connection is OK, false otherwise.
        """
        self._waitIfNeeded("/consult/ping")
        return self._client.get(LegiConnector._HOST + "/consult/ping")\
            .status_code == 200
        
    def post(self, path, payload):
        """
        Send a POST query to the Legifrance API.

        Parameters
        ----------
        path : str
            Path to the resource to query. The value SHOULD be one of the 
            keys in the "paths" dict of the SWAGGER description of the API.
        payload : dict
            Valid dict representation of the JSON payload parameter expected
            by the queried resource.

        Returns
        -------
        dict
            Dict representation of the JSON response of the server.
        """
        self._waitIfNeeded(path)
        if self._dummy:
            return self._dummyResults(path, payload)
        else:
            return self._client.post(
                LegiConnector._HOST + path, json=payload).json()
    
    def _dummyResults(self, path, payload):
        """
        Produce dummy results to a query.

        Parameters
        ----------
        path : str
            Path to the ressource to mock.
        payload : dict
            Valid dict representation of the JSON payload parameter expected by
            the mocked resource.

        Returns
        -------
        dict
            Dict representation of a mock JSON response.
        """
        import dummies
        if path == "/search":
            return dummies.getCidList(payload["recherche"]["pageNumber"], 
                                      payload["recherche"]["pageSize"])
        elif path == "/consult/jorf":
            return dummies.getText(payload["textCid"])