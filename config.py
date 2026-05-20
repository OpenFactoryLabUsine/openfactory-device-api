import os


class Config:
    websocket_host: str = os.getenv("WEBSOCKET_HOST", "0.0.0.0")
    websocket_port: int = int(os.getenv("WEBSOCKET_PORT", "8000"))
    ping_interval: int = int(os.getenv("PING_INTERVAL", "30"))
    ping_timeout: int = int(os.getenv("PING_TIMEOUT", "10"))
