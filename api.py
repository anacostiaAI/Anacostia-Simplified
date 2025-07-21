from fastapi import FastAPI, status
import asyncio
import httpx
from pydantic import BaseModel



class RPCConnectionModel(BaseModel):
    url: str


class BaseServer(FastAPI):
    def __init__(
        self, 
        name: str,
        host: str,
        port: int,
        client_url: str = None,
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.name = name
        self.host = host
        self.port = port
        self.client_url = client_url
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_ca_certs = ssl_ca_certs

        self.loop: asyncio.AbstractEventLoop = None

        if self.client_url is not None:
            if self.ssl_ca_certs is None or self.ssl_certfile is None or self.ssl_keyfile is None:
                # If no SSL certificates are provided, create a client without them
                self.client = httpx.AsyncClient(base_url=self.client_url)
                self.scheme = "http"
            else:
                # If SSL certificates are provided, use them to create the client
                try:
                    self.client = httpx.AsyncClient(base_url=self.client_url, verify=self.ssl_ca_certs, cert=(self.ssl_certfile, self.ssl_keyfile))
                    self.scheme = "https"
                except httpx.ConnectError as e:
                    raise ValueError(f"Failed to create HTTP client with SSL certificates: {e}")

        @self.get("/")
        async def root():
            return {"message": f"Welcome to the {self.name} server!"}

        @self.get("/health", status_code=status.HTTP_200_OK)
        async def health_check():
            print(f"Health check for {self.name} server")
            return {"status": "healthy"}

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Set the event loop for the server. This is done to ensure the server uses the same event loop as the connector.
        """
        self.loop = loop

    def get_server_prefix(self) -> str:
        """
        Get the server prefix for the server URL.
        """
        return f"/{self.name}/api/server"

    def get_server_url(self):
        # sample output: http://127.0.0.1:8000/metadata/api/server
        return f"{self.scheme}://{self.host}:{self.port}{self.get_server_prefix()}"
    
    async def connect(self) -> None:
        if self.client_url is not None:
            parsed_url = httpx.URL(self.client_url)
            if parsed_url.scheme != "https":
                raise ValueError(f"Invalid client URL scheme: {self.client_url}. Must be 'https' when SSL certificates are provided.")

            response = await self.client.post(f"/api/client/connect", json={"url": self.get_server_url()})
            message = response.json()["message"]
            print(f"Server {self.name} connected to client at {self.client_url}: {message}")



class BaseClient(FastAPI):
    def __init__(
        self, 
        name: str,
        host: str = "localhost",
        port: int = 8000,
        server_url: str = None, 
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.name = name
        self.host = host
        self.port = port
        self.server_url = server_url
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_ca_certs = ssl_ca_certs

        self.loop: asyncio.AbstractEventLoop = None
        
        if self.server_url is None:
            @self.post("/connect", status_code=status.HTTP_200_OK)
            async def connect(server: RPCConnectionModel):
                """
                Endpoint to connect to the server. This can be used to establish a connection with the server.
                """
                self.server_url = server.url
                self.create_client()
                print(f"Connected to server at {server.url}")
                return {"message": f"client 'http://{self.host}:{self.port}/{self.name}/api/client' connected to server at '{server.url}'"}

    def get_client_prefix(self):
        return f"/{self.name}/api/client"

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Set the event loop for the client. This is done to ensure the client uses the same event loop as the server.
        """
        self.loop = loop

    def set_credentials(self, host: str, port: int, ssl_keyfile: str, ssl_certfile: str, ssl_ca_certs: str) -> None:
        self.host = host
        self.port = port
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_ca_certs = ssl_ca_certs

        if self.server_url is not None:
            self.create_client()

    def create_client(self):
        if self.ssl_ca_certs is None or self.ssl_certfile is None or self.ssl_keyfile is None:
            # If no SSL certificates are provided, create a client without them
            self.client = httpx.AsyncClient(base_url=self.server_url)
        else:
            # If SSL certificates are provided, use them to create the client
            try:
                self.client = httpx.AsyncClient(base_url=self.server_url, verify=self.ssl_ca_certs, cert=(self.ssl_certfile, self.ssl_keyfile))
            except httpx.ConnectError as e:
                raise ValueError(f"Failed to create HTTP client with SSL certificates: {e}")

    def health_check(self):
        """
        Perform a health check on the client. This can be overridden by subclasses to implement specific health checks.
        """

        async def _health_check():
            try:
                response = await self.client.get(f"/health")
                if response.status_code == status.HTTP_200_OK:
                    return response.json()
                else:
                    raise ValueError(f"Health check failed with status code {response.status_code}")
            except httpx.HTTPError as e:
                raise ValueError(f"Health check failed: {e}")

        task = asyncio.run_coroutine_threadsafe(_health_check(), self.loop)
        return task.result()