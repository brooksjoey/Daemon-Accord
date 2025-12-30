import hashlib
import hmac
import base64
import secrets
from typing import Optional, Tuple
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
import os

class CryptoUtils:
    """Cryptographic utilities for the automation engine"""
    
    def __init__(self, secret_key: Optional[str] = None):
        self.secret_key = secret_key or os.getenv("SECRET_KEY", "default-insecure-key-change-me")
        self.fernet_key = self._derive_fernet_key(self.secret_key)
        self.fernet = Fernet(self.fernet_key)
    
    def _derive_fernet_key(self, password: str, salt: bytes = None) -> bytes:
        """Derive Fernet key from password"""
        if salt is None:
            salt = b"browser_automation_salt"  # Should be unique per installation
        
        kdf = PBKDF2(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key
    
    def encrypt(self, data: str) -> str:
        """Encrypt string data"""
        encrypted = self.fernet.encrypt(data.encode())
        return base64.urlsafe_b64encode(encrypted).decode()
    
    def decrypt(self, encrypted_data: str) -> str:
        """Decrypt string data"""
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode())
        decrypted = self.fernet.decrypt(encrypted_bytes)
        return decrypted.decode()
    
    def hash_api_key(self, api_key: str) -> str:
        """Hash API key for storage"""
        # Use HMAC with secret key
        hmac_obj = hmac.new(
            self.secret_key.encode(),
            api_key.encode(),
            hashlib.sha256
        )
        return hmac_obj.hexdigest()
    
    def verify_api_key(self, api_key: str, stored_hash: str) -> bool:
        """Verify API key against stored hash"""
        computed_hash = self.hash_api_key(api_key)
        return hmac.compare_digest(computed_hash, stored_hash)
    
    def generate_api_key(self, length: int = 32) -> str:
        """Generate secure random API key"""
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return ''.join(secrets.choice(alphabet) for _ in range(length))
    
    def generate_signed_token(self, data: dict, expires_in: int = 3600) -> str:
        """Generate signed token with expiration"""
        import json
        import time
        
        payload = {
            "data": data,
            "exp": int(time.time()) + expires_in,
            "iat": int(time.time())
        }
        
        payload_json = json.dumps(payload)
        payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode()
        
        # Create signature
        signature = hmac.new(
            self.secret_key.encode(),
            payload_b64.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return f"{payload_b64}.{signature}"
    
    def verify_signed_token(self, token: str) -> Optional[dict]:
        """Verify and decode signed token"""
        try:
            parts = token.split(".")
            if len(parts) != 2:
                return None
            
            payload_b64, signature = parts
            
            # Verify signature
            expected_signature = hmac.new(
                self.secret_key.encode(),
                payload_b64.encode(),
                hashlib.sha256
            ).hexdigest()
            
            if not hmac.compare_digest(signature, expected_signature):
                return None
            
            # Decode payload
            payload_json = base64.urlsafe_b64decode(payload_b64.encode()).decode()
            payload = json.loads(payload_json)
            
            # Check expiration
            if payload.get("exp", 0) < time.time():
                return None
            
            return payload.get("data")
        except:
            return None
    
    def generate_key_pair(self) -> Tuple[str, str]:
        """Generate RSA key pair for advanced crypto"""
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        
        public_key = private_key.public_key()
        
        # Serialize private key
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ).decode()
        
        # Serialize public key
        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode()
        
        return private_pem, public_pem
    
    def encrypt_with_public_key(self, public_key_pem: str, data: str) -> str:
        """Encrypt data with RSA public key"""
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives import hashes
        
        public_key = serialization.load_pem_public_key(public_key_pem.encode())
        
        encrypted = public_key.encrypt(
            data.encode(),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        
        return base64.urlsafe_b64encode(encrypted).decode()
    
    def decrypt_with_private_key(self, private_key_pem: str, encrypted_data: str) -> str:
        """Decrypt data with RSA private key"""
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives import hashes
        
        private_key = serialization.load_pem_private_key(
            private_key_pem.encode(),
            password=None
        )
        
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode())
        
        decrypted = private_key.decrypt(
            encrypted_bytes,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        
        return decrypted.decode()
    
    def create_hmac_signature(self, data: str, key: str = None) -> str:
        """Create HMAC signature for data"""
        if key is None:
            key = self.secret_key
        
        hmac_obj = hmac.new(
            key.encode(),
            data.encode(),
            hashlib.sha256
        )
        
        return hmac_obj.hexdigest()
    
    def verify_hmac_signature(self, data: str, signature: str, key: str = None) -> bool:
        """Verify HMAC signature"""
        expected = self.create_hmac_signature(data, key)
        return hmac.compare_digest(expected, signature)
    
    @staticmethod
    def generate_random_bytes(length: int = 32) -> bytes:
        """Generate cryptographically secure random bytes"""
        return secrets.token_bytes(length)
    
    @staticmethod
    def generate_random_hex(length: int = 32) -> str:
        """Generate cryptographically secure random hex string"""
        return secrets.token_hex(length // 2)
