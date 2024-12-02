from pydantic import BaseModel
from typing import Dict


class KeycloakPayload(BaseModel):
    server_url: str
    client_id: str
    realm_name: str
    token: str


class KeycloakUserInfo(BaseModel):
    sub: str
    email_verified: bool
    name: str
    preferred_username: str
    given_name: str
    family_name: str
    email: str


class JWTPayload(BaseModel):
    kuuid: str
    name: str
    email: str
    exp: int
    server_url: str
    client_id: str
    realm_name: str
    token: str
