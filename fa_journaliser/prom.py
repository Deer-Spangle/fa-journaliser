import os

DEFAULT_PROMETHEUS_PORT = 7074

def get_prometheus_port() -> int | None:
    prom_port = os.getenv("PROM_PORT")
    if prom_port is None:
        return DEFAULT_PROMETHEUS_PORT
    if prom_port == "":
        return None
    return int(prom_port)
