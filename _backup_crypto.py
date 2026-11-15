# /_backup_crypto.py
# CrossWatch - Authenticated streaming backup encryption
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from pathlib import Path

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

MAGIC = b"CWBACK01"
HEADER_SIZE = len(MAGIC) + 16 + 12
CHUNK_SIZE = 1024 * 1024


def _key(password: str, salt: bytes) -> bytes:
    if not password:
        raise ValueError("Enter the backup password or recovery key.")
    return Scrypt(salt=salt, length=32, n=32768, r=8, p=1).derive(password.encode("utf-8"))


def encrypt(source: Path, target: Path, password: str) -> None:
    if len(password) < 12:
        raise ValueError("Use a backup password with at least 12 characters.")
    salt, nonce = os.urandom(16), os.urandom(12)
    header = MAGIC + salt + nonce
    cipher = Cipher(algorithms.AES(_key(password, salt)), modes.GCM(nonce)).encryptor()
    cipher.authenticate_additional_data(header)
    created = False
    try:
        with source.open("rb") as src, target.open("xb") as dst:
            created = True
            os.chmod(target, 0o600)
            dst.write(header)
            while chunk := src.read(CHUNK_SIZE):
                dst.write(cipher.update(chunk))
            dst.write(cipher.finalize())
            dst.write(cipher.tag)
            dst.flush()
            os.fsync(dst.fileno())
    except Exception:
        if created:
            target.unlink(missing_ok=True)
        raise


def decrypt(source: Path, target: Path, password: str) -> None:
    created = False
    try:
        with source.open("rb") as src:
            header = src.read(HEADER_SIZE)
            if len(header) != HEADER_SIZE or not header.startswith(MAGIC):
                raise ValueError("Unknown encrypted backup format.")
            remaining = source.stat().st_size - HEADER_SIZE - 16
            if remaining < 0:
                raise ValueError("The encrypted backup is incomplete.")
            src.seek(-16, os.SEEK_END)
            tag = src.read(16)
            src.seek(HEADER_SIZE)
            cipher = Cipher(algorithms.AES(_key(password, header[8:24])), modes.GCM(header[24:], tag)).decryptor()
            cipher.authenticate_additional_data(header)
            with target.open("xb") as dst:
                created = True
                os.chmod(target, 0o600)
                while remaining:
                    chunk = src.read(min(CHUNK_SIZE, remaining))
                    if not chunk:
                        raise ValueError("The encrypted backup is incomplete.")
                    dst.write(cipher.update(chunk))
                    remaining -= len(chunk)
                dst.write(cipher.finalize())
                dst.flush()
                os.fsync(dst.fileno())
    except InvalidTag as exc:
        target.unlink(missing_ok=True)
        raise ValueError("The backup password is incorrect or the archive was changed.") from exc
    except Exception:
        if created:
            target.unlink(missing_ok=True)
        raise
