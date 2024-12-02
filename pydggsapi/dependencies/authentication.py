from keycloak import KeycloakOpenID
from keycloak.exceptions import KeycloakError
from fastapi.security import HTTPBearer
from fastapi import Request, HTTPException
import jwt
import secrets

from dataserv.schemas.authentication import KeycloakPayload, JWTPayload, KeycloakUserInfo
from typing import Optional, Dict
from dotenv import load_dotenv
from datetime import timedelta, datetime, timezone
import os
import json
from copy import deepcopy

import logging
logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


def keycloak_verify(payload: KeycloakPayload) -> Optional[KeycloakUserInfo]:
    try:
        tmp = deepcopy(payload)
        token = tmp.token
        del tmp.token
        keycloak = KeycloakOpenID(**vars(tmp))
        return KeycloakUserInfo(**keycloak.userinfo(token))
    except KeycloakError as e:
        logging.error(f'{__name__} Failed to verify with Keycloak: {e}')
        raise HTTPException(status_code=401, detail=f'Failed to verify with Keycloak: {e}')


def keycloak_decode(payload: KeycloakPayload) -> Optional[Dict]:
    try:
        tmp = deepcopy(payload)
        token = tmp.token
        del tmp.token
        keycloak = KeycloakOpenID(**vars(tmp))
        return keycloak.decode_token(token)
    except KeycloakError as e:
        logging.error(f'{__name__} Failed to decode with Keycloak: {e}')
        raise HTTPException(status_code=401, detail=f'Failed to decode with Keycloak: {e}')


class Session():
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def initial_data(self, storage):
        self.data = storage

    def get(self, uuid):
        data = self.data[uuid] if self.data.get(uuid) else None
        return data

    def set(self, uuid, data):
        self.data[uuid] = data

    def delete(self, uuid):
        del self.data[uuid]


session = Session()


class JWTHTTPBearer(HTTPBearer):

    def __init__(self):
        self.session = session
        super().__init__()

    def generateJWT(self, keycloak_userinfo: KeycloakUserInfo, keycloak_payload: KeycloakPayload) -> Optional[str]:
        if (self.session.get(keycloak_userinfo.sub)):
            time2expire = self.session.get(keycloak_userinfo.sub)["exp"] - datetime.now(timezone.utc).timestamp()
            if (time2expire > (60 * 5)):
                logging.info(f'{__name__} JWT resend, kuuid: {keycloak_userinfo.sub}, TimeToExpire: {time2expire}')
                return self.session.get(keycloak_userinfo.sub)['token']
        if (os.environ.get('jwt_sign_key')):
            if (keycloak_verify(keycloak_payload)):
                token_exp = keycloak_decode(keycloak_payload)['exp']
                sign_key = os.environ['jwt_sign_key']
                payload = {'kuuid': keycloak_userinfo.sub,
                           'name': keycloak_userinfo.name,
                           'email': keycloak_userinfo.email,
                           'exp': token_exp}
                payload.update(vars(keycloak_payload))
                jwttoken = jwt.encode(payload, sign_key, 'HS256')
                self.session.set(keycloak_userinfo.sub, {'exp': token_exp, 'token': jwttoken})
                logging.info(f'{__name__} JWT Generated, kuuid: {payload["kuuid"]}')
                return jwttoken
            logging.error(f'{__name__} JWT Generate failed, keycloak verify fail')
            raise HTTPException(status_code=401, detail='JWT Generate failed, keycloak verify fail')
        logging.error(f'{__name__} JWT Service not avaliable.')
        raise HTTPException(status_code=401, detail='JWT Service not avaliable.')

    def deleteJWT(self, kuuid):
        self.data.delete(kuuid)

    # to handle decode
    async def __call__(self, request: Request) -> Optional[JWTPayload]:
        load_dotenv()
        body = None
        # To handle cases for endpoints weight_criterias and feedback that don' require auth
        if (('weight_criterias' in request.url.path) or ('feedback' in request.url.path)):
            try:
                body = await request.json()
            except json.decoder.JSONDecodeError:
                return None  # if request body is None, skip checking
            # query default weight_criterias, skip checking
            if ((body['action'] == 'query') and ('weight_criterias' in request.url.path) and
               (request.headers.get("Authorization") is None)):
                return None
        if (os.environ.get('jwt_sign_key')):
            sign_key = os.environ['jwt_sign_key']
            try:
                auth = await super().__call__(request)
                payload = JWTPayload(**(jwt.decode(auth.credentials, sign_key, 'HS256')))
                keycloak_payload = KeycloakPayload(**{'server_url': payload.server_url,
                                                      'realm_name': payload.realm_name,
                                                      'client_id': payload.client_id,
                                                      'token': payload.token})
                keycloak_verify(keycloak_payload)
                return payload
            except jwt.exceptions.InvalidTokenError as e:
                logging.error(f'{__name__} Failed to decode JWT: {e}')
                raise HTTPException(status_code=401, detail=f'Failed to decode JWT: {e}')
        logging.error(f'{__name__} JWT Service not avaliable.')
        raise HTTPException(status_code=401, detail='JWT Service not avaliable.')





