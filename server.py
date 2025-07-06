import threading
import signal
import asyncio
from pydantic import BaseModel
from urllib.parse import urlparse
from typing import List, Dict

from fastapi import FastAPI, status
import uvicorn
import httpx
from contextlib import asynccontextmanager

from node import Connector
from pipeline import Pipeline


class PipelineConnectionModel(BaseModel):
    predecessor_url: str


class PipelineServer(FastAPI):
    def __init__(
        self, 
        name: str, 
        pipeline: Pipeline, 
        host: str = "127.0.0.1", port: int = 8000, 
        ssl_ca_certs: str = None, ssl_keyfile: str = None, ssl_certfile: str = None, 
        *args, **kwargs
    ):

        @asynccontextmanager
        async def lifespan(app: PipelineServer):
            await app.connect()     # Connect to the leaf services
            yield
            await app.disconnect()  # Disconnect from the leaf services

        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.pipeline = pipeline
        self.host = host
        self.port = port
        self.ssl_ca_certs = ssl_ca_certs
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile

        config = uvicorn.Config(
            app=self, 
            host=self.host, 
            port=self.port,
            ssl_ca_certs=ssl_ca_certs,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            access_log=True
        )
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)

        if ssl_ca_certs is None or ssl_certfile is None or ssl_keyfile is None:
            # If no SSL certificates are provided, create a client without them
            self.client = httpx.AsyncClient()
        else:
            # If SSL certificates are provided, use them to create the client
            self.client = httpx.AsyncClient(verify=ssl_ca_certs, cert=(ssl_certfile, ssl_keyfile))

        # get the successor ip addresses
        self.successor_ip_addresses = []
        for node in self.pipeline.nodes:
            for url in node.remote_successors:
                parsed = urlparse(url)
                base_url = f"{parsed.scheme}://{parsed.netloc}"

                if ssl_ca_certs is not None and ssl_certfile is not None and ssl_keyfile is not None:
                    # Ensure the base URL is using HTTPS if SSL certificates are provided
                    if parsed.scheme != "https":
                        raise ValueError(f"Invalid URL scheme for successor: {url}. Must be 'https' when SSL is enabled.")

                if base_url not in self.successor_ip_addresses:
                    self.successor_ip_addresses.append(base_url)

        self.connectors: List[Connector] = []
        for node in self.pipeline.nodes:
            connector: Connector = node.setup_connector(
                host=self.host, port=self.port, 
                ssl_ca_certs=ssl_ca_certs, ssl_certfile=ssl_certfile, ssl_keyfile=ssl_keyfile
            )
            self.mount(connector.get_connector_prefix(), connector)          # mount the BaseNodeApp to PipelineWebserver
            self.connectors.append(connector)
        
        self.predecessor_ip_addresses = []

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(connection: PipelineConnectionModel):
            if connection.predecessor_url not in self.predecessor_ip_addresses:
                self.predecessor_ip_addresses.append(connection.predecessor_url)

        @self.post("/finish_connect", status_code=status.HTTP_200_OK)
        async def finish_connect():
            for node in self.pipeline.nodes:
                node.start_node_lifecycle.set()  # Set each node's start_node_lifecycle event to allow them to start executing

    def get_pipeline_url(self):
        """
        Returns the URL of the pipeline server.
        """
        if self.ssl_ca_certs is None or self.ssl_certfile is None or self.ssl_keyfile is None:
            # If no SSL certificates are provided, use HTTP
            return f"http://{self.host}:{self.port}"
        else:
            # If SSL certificates are provided, use HTTPS
            return f"https://{self.host}:{self.port}"
    
    async def connect(self):
        # Connect to leaf pipeline
        task = []
        for successor_url in self.successor_ip_addresses:
            pipeline_server_model = PipelineConnectionModel(predecessor_url=self.get_pipeline_url()).model_dump()
            task.append(self.client.post(f"{successor_url}/connect", json=pipeline_server_model))
        await asyncio.gather(*task)

        for connector in self.connectors:
            await connector.connect(client=self.client)

        # Start the nodes on the successor pipeline before allowing the nodes to start executing
        if len(self.successor_ip_addresses) > 0:
            task = []
            for successor_url in self.successor_ip_addresses:
                task.append(self.client.post(f"{successor_url}/finish_connect"))
            await asyncio.gather(*task)

            for node in self.pipeline.nodes:
                node.start_node_lifecycle.set()

    async def disconnect(self):
        print("Closing clients...")
        await self.client.aclose()
        print("All remote clients closed.")
    
    def shutdown(self):
        """
        Shutdown the server and close all connections.
        """
        print(f"Shutting down {self.name} Webservice...")
        self.server.should_exit = True
        self.fastapi_thread.join()
        print(f"Anacostia Webservice {self.name} Shutdown...")

        print("Killing pipeline...")
        self.pipeline.terminate_nodes()
        print("Pipeline Killed.")

    def run(self):
        # start the server in a separate thread
        self.fastapi_thread.start()

        # Launch the root pipeline
        self.pipeline.launch_nodes()

        # keep the main thread open; this is done to avoid an error in python 3.12 "RuntimeError: can't create new thread at interpreter shutdown"
        # and to avoid "RuntimeError: can't register atexit after shutdown" in python 3.9
        for thread in threading.enumerate():
            if thread.daemon or thread is threading.current_thread():
                continue
            thread.join()