import threading
import signal
import asyncio
from pydantic import BaseModel
from urllib.parse import urlparse

from fastapi import FastAPI, status
import uvicorn
import httpx
from contextlib import asynccontextmanager

from node import Connector
from pipeline import Pipeline


class PipelineConnectionModel(BaseModel):
    predecessor_host: str
    predecessor_port: int


class PipelineServer(FastAPI):
    def __init__(self, name: str, pipeline: Pipeline, host: str = "127.0.0.1", port: int = 8000, *args, **kwargs):

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

        config = uvicorn.Config(self, host=self.host, port=self.port)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)

        # get the successor ip addresses
        self.successor_ip_addresses = []
        for node in self.pipeline.nodes:
            for url in node.remote_successors:
                parsed = urlparse(url)
                base_url = f"{parsed.scheme}://{parsed.netloc}"

                if base_url not in self.successor_ip_addresses:
                    self.successor_ip_addresses.append(base_url)

        for node in self.pipeline.nodes:
            subapp: Connector = node.setup_connector()
            subapp.set_host(self.host)
            subapp.set_port(self.port)
            self.mount(subapp.get_connector_prefix(), subapp)          # mount the BaseNodeApp to PipelineWebserver
        
        self.predecessor_host = None
        self.predecessor_port = None
        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(connection: PipelineConnectionModel):
            self.predecessor_host = connection.predecessor_host
            self.predecessor_port = connection.predecessor_port
        
        @self.post("/finish_connect", status_code=status.HTTP_200_OK)
        async def finish_connect():
            for node in self.pipeline.nodes:
                node.connection_event.set()  # Set the connection event for each node

    async def connect(self):
        async with httpx.AsyncClient() as client:
            # Connect each node to its remote successors
            task = []
            for node in self.pipeline.nodes:
                for connection in node.remote_successors:
                    json = {
                        "node_url": f"http://{self.host}:{self.port}/{node.name}",
                        "node_type": type(node).__name__
                    }
                    task.append(client.post(f"{connection}/connect", json=json))

            responses = await asyncio.gather(*task)

            # Extract the leaf URLs from the responses, connection is now established
            leaf_urls = []
            for response in responses:
                if response.status_code == 200:
                    response_json = response.json()
                    leaf_urls.append(response_json["node_url"])
            
            task = []
            for leaf_ip_address in self.successor_ip_addresses:
                task.append(client.post(f"{leaf_ip_address}/finish_connect"))

            await asyncio.gather(*task)
            print(f"Root pipeline connected to these leaf URLs: {leaf_urls}")
            
    async def disconnect(self):
        print("Disconnecting from leaf nodes...")

    def run(self):
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            print("Killing pipeline...")
            self.pipeline.terminate_nodes()
            print("Pipeline Killed.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        self.fastapi_thread.start()

        # Launch the root pipeline
        self.pipeline.launch_nodes()

        # keep the main thread open; this is done to avoid an error in python 3.12 "RuntimeError: can't create new thread at interpreter shutdown"
        # and to avoid "RuntimeError: can't register atexit after shutdown" in python 3.9
        for thread in threading.enumerate():
            if thread.daemon or thread is threading.current_thread():
                continue
            thread.join()