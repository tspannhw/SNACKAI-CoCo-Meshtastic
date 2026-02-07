#!/usr/bin/env python3
import jwt
import time
import logging
import requests
from datetime import datetime, timedelta, timezone
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from typing import Dict, Optional
from hashlib import sha256

logger = logging.getLogger(__name__)


class SnowflakeJWTAuth:
    def __init__(self, config: Dict):
        self.config = config
        self.account = config['account'].upper()
        self.user = config['user'].upper()
        
        if 'pat' in config and config['pat']:
            self.auth_method = 'pat'
            self.pat = config['pat']
            logger.info(f"PAT authentication initialized for user: {self.user}")
        elif 'private_key_file' in config and config['private_key_file']:
            self.auth_method = 'jwt'
            self.private_key = self._load_private_key()
            self.qualified_username = f"{self.account}.{self.user}"
            logger.info(f"JWT auth initialized for user: {self.qualified_username}")
        else:
            raise ValueError(
                "No authentication method configured. "
                "Provide either 'pat' (Programmatic Access Token) or 'private_key_file' in config."
            )
    
    def _load_private_key(self):
        private_key_file = self.config['private_key_file']
        
        try:
            with open(private_key_file, 'rb') as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None,
                    backend=default_backend()
                )
            
            logger.info(f"Private key loaded from {private_key_file}")
            return private_key
            
        except FileNotFoundError:
            logger.error(f"Private key file not found: {private_key_file}")
            raise
        except Exception as e:
            logger.error(f"Error loading private key: {e}")
            raise
    
    def generate_jwt_token(self) -> str:
        public_key_bytes = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        public_key_fp = 'SHA256:' + sha256(public_key_bytes).hexdigest().upper()
        
        now = datetime.now(timezone.utc)
        iat = int(now.timestamp())
        exp = int((now + timedelta(hours=1)).timestamp())
        
        payload = {
            'iss': f"{self.qualified_username}.{public_key_fp}",
            'sub': self.qualified_username,
            'iat': iat,
            'exp': exp
        }
        
        logger.debug(f"JWT payload - iss: {payload['iss'][:50]}...")
        logger.debug(f"JWT payload - sub: {payload['sub']}")
        
        token = jwt.encode(
            payload,
            self.private_key,
            algorithm='RS256'
        )
        
        logger.debug("JWT token generated")
        return token
    
    def get_scoped_token(self, scope: str = None) -> str:
        if self.auth_method == 'pat':
            logger.info("Using Programmatic Access Token (PAT)")
            return self.pat
        
        elif self.auth_method == 'jwt':
            return self._get_jwt_oauth_token(scope)
        
        else:
            raise ValueError(f"Unknown auth method: {self.auth_method}")
    
    def _get_jwt_oauth_token(self, scope: str = None) -> str:
        logger.info("Exchanging JWT for OAuth token...")
        
        jwt_token = self.generate_jwt_token()
        
        account = self.config['account'].lower()
        token_url = f"https://{account}.snowflakecomputing.com/oauth/token"
        
        logger.debug(f"Token URL: {token_url}")
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        role = self.config.get('role', 'PUBLIC').upper()
        
        if scope is None:
            scope = f'session:role:{role}'
        
        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': jwt_token,
            'scope': scope
        }
        
        logger.debug(f"Requesting token with scope: {scope}")
        
        try:
            response = requests.post(
                token_url,
                headers=headers,
                data=data,
                timeout=30
            )
            response.raise_for_status()
            
            token_data = response.json()
            access_token = token_data.get('access_token')
            
            if not access_token:
                raise ValueError("No access_token in response")
            
            logger.info("OAuth token obtained successfully")
            return access_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get OAuth token: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            
            logger.error("\nTroubleshooting JWT Auth:")
            logger.error("1. Verify the public key is registered in Snowflake:")
            logger.error(f"   ALTER USER {self.user} SET RSA_PUBLIC_KEY='<your_key>';")
            logger.error("2. Ensure the private key matches the registered public key")
            logger.error(f"3. Check the user exists: {self.user}")
            logger.error(f"4. Verify account identifier: {self.account}")
            logger.error("\nOR switch to Programmatic Access Token (PAT):")
            logger.error("  Add 'pat' to your snowflake_config.json")
            
            raise


def main():
    import json
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        with open('snowflake_config.json', 'r') as f:
            config = json.load(f)
        
        auth = SnowflakeJWTAuth(config)
        token = auth.get_scoped_token()
        
        print(f"Successfully obtained token (length: {len(token)})")
        print(f"Token prefix: {token[:50]}...")
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)


if __name__ == '__main__':
    main()
