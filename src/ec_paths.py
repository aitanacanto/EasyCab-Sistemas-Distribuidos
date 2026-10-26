"""Rutas compartidas, independientes del directorio desde el que se ejecuta Python."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RUNTIME_DIR = PROJECT_ROOT / "runtime"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

ARCHIVO_MAPA = DATA_DIR / "maps" / "mapa.txt"
ARCHIVO_DESTINOS = DATA_DIR / "locations" / "EC_locations.json"
ARCHIVO_TAXI = RUNTIME_DIR / "taxi.txt"
ARCHIVO_TOKENS = RUNTIME_DIR / "token.txt"
ARCHIVO_CLAVES = RUNTIME_DIR / "claves.txt"
ARCHIVO_AUDITORIA = RUNTIME_DIR / "audit_log.txt"
