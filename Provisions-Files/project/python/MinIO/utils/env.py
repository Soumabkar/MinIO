# utils/env.py  — centralise la lecture des variables
import os

def env(key: str) -> str:
    return os.getenv(key).strip()

def env_int(key: str) -> int:
    return int(env(key))   

def env_bool(key: str) -> bool:
    return env(key).lower() in ("true") 