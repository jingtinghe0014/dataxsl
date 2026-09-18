"""Credential compatibility helpers. No secrets are written to logs."""
import logging
import os
import re

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad, pad

# Compatibility only; new ENC values should use DATAXSL_ENCRYPTION_KEY.
LEGACY_KEY = b'TplEasT220241218'


def decrypt(content: bytes, word: bytes):
    cipher = AES.new(word, AES.MODE_CBC, content[:AES.block_size])
    return unpad(cipher.decrypt(content[AES.block_size:]), AES.block_size).decode()


def encrypt(content: str, word: bytes):
    cipher = AES.new(word, AES.MODE_CBC)
    return cipher.iv + cipher.encrypt(pad(content.encode(), AES.block_size))


def resolve_secret(value):
    if not value or not isinstance(value, str) or not value.startswith('ENC('):
        return value
    match = re.fullmatch(r'ENC\(([0-9a-fA-F]+)\)', value)
    if not match:
        raise ValueError('Invalid ENC credential format')
    configured = os.environ.get('DATAXSL_ENCRYPTION_KEY')
    try:
        if configured is None:
            logging.getLogger(__name__).warning(
                'Legacy ENC key in use; migrate to DATAXSL_ENCRYPTION_KEY')
            secret = LEGACY_KEY
        else:
            secret = bytes.fromhex(configured[4:]) if configured.startswith('hex:') else configured.encode()
        return decrypt(bytes.fromhex(match[1]), secret)
    except Exception:
        raise ValueError('Unable to decrypt ENC credential; check encryption key') from None


def redact(message, secrets):
    for secret in secrets:
        if isinstance(secret, str) and secret:
            message = message.replace(secret, '***')
    return message

if __name__ == '__main__':

    # plaintext = '123456'
    plaintext = 'HJbvr_8hgjKwe'
    key = b'TplEasT220241218'
    ciphertext = encrypt(plaintext, key)
    ciphertext_hex = ciphertext.hex()
    print("加密结果:", ciphertext_hex)

    decrypted_text = decrypt(bytes.fromhex(ciphertext_hex), key)
    print("解密结果:", decrypted_text)