import os
import json
import boto3
from typing import Dict, Optional
from functools import lru_cache

class SecretsManager:
    """Manages interaction with AWS Secrets Manager with local development support."""
    
    def __init__(self):
        """Initialize the secrets manager."""
        self.stage = os.environ.get('STAGE', 'dev')
        self.is_local = os.environ.get('LOCAL_DEVELOPMENT', 'false').lower() == 'true'
        
        if self.is_local:
            self.secrets_file = os.environ.get(
                'LOCAL_SECRETS_FILE', 
                os.path.join(os.getcwd(), '.secrets', f'{self.stage}.json')
            )
        else:
            self.client = boto3.client('secretsmanager')
            
        self._secrets_cache: Dict[str, str] = {}
        
    @lru_cache(maxsize=100)
    def get_secret(self, secret_name: str) -> str:
        """
        Get a secret value by name.
        
        Args:
            secret_name: Name of the secret
            
        Returns:
            The secret value
            
        Raises:
            ValueError: If the secret is not found
        """
        if self.is_local:
            return self._get_local_secret(secret_name)
        return self._get_aws_secret(secret_name)
        
    def _get_local_secret(self, secret_name: str) -> str:
        """Get secret from local file."""
        try:
            if not os.path.exists(self.secrets_file):
                raise ValueError(
                    f"Local secrets file not found at {self.secrets_file}"
                )
                
            with open(self.secrets_file, 'r') as f:
                secrets = json.load(f)
                
            if secret_name not in secrets:
                raise ValueError(
                    f"Secret {secret_name} not found in {self.secrets_file}"
                )
                
            return secrets[secret_name]
        except Exception as e:
            raise ValueError(f"Error reading local secret: {str(e)}")
            
    def _get_aws_secret(self, secret_name: str) -> str:
        """Get secret from AWS Secrets Manager."""
        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            if 'SecretString' in response:
                return response['SecretString']
            raise ValueError(f"Secret {secret_name} has no string value")
        except self.client.exceptions.ResourceNotFoundException:
            raise ValueError(f"Secret {secret_name} not found")
        except Exception as e:
            raise ValueError(f"Error getting AWS secret: {str(e)}")
            
    def put_secret(self, secret_name: str, secret_value: str):
        """
        Store a secret value.
        
        Args:
            secret_name: Name of the secret
            secret_value: Value to store
        """
        if self.is_local:
            self._put_local_secret(secret_name, secret_value)
        else:
            self._put_aws_secret(secret_name, secret_value)
            
    def _put_local_secret(self, secret_name: str, secret_value: str):
        """Store secret in local file."""
        try:
            os.makedirs(os.path.dirname(self.secrets_file), exist_ok=True)
            
            secrets = {}
            if os.path.exists(self.secrets_file):
                with open(self.secrets_file, 'r') as f:
                    secrets = json.load(f)
                    
            secrets[secret_name] = secret_value
            
            with open(self.secrets_file, 'w') as f:
                json.dump(secrets, f, indent=2)
        except Exception as e:
            raise ValueError(f"Error writing local secret: {str(e)}")
            
    def _put_aws_secret(self, secret_name: str, secret_value: str):
        """Store secret in AWS Secrets Manager."""
        try:
            self.client.create_secret(
                Name=secret_name,
                SecretString=secret_value
            )
        except self.client.exceptions.ResourceExistsException:
            self.client.update_secret(
                SecretId=secret_name,
                SecretString=secret_value
            )
        except Exception as e:
            raise ValueError(f"Error putting AWS secret: {str(e)}")
