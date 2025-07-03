import threading
import signal
import asyncio
from pydantic import BaseModel
from urllib.parse import urlparse
from typing import List

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
            for conn in node.remote_successors:
                parsed = urlparse(conn['node_url'])
                base_url = f"{parsed.scheme}://{parsed.netloc}"

                if base_url not in self.successor_ip_addresses:
                    self.successor_ip_addresses.append(base_url)

        self.connectors: List[Connector] = []
        for node in self.pipeline.nodes:
            connector: Connector = node.setup_connector(host=self.host, port=self.port)
            self.mount(connector.get_connector_prefix(), connector)          # mount the BaseNodeApp to PipelineWebserver
            self.connectors.append(connector)
        
        self.predecessor_ip_addresses = []
        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(connection: PipelineConnectionModel):
            if f"http://{connection.predecessor_host}:{connection.predecessor_port}" not in self.predecessor_ip_addresses:
                self.predecessor_ip_addresses.append(
                    f"http://{connection.predecessor_host}:{connection.predecessor_port}"
                ) 

        @self.post("/finish_connect", status_code=status.HTTP_200_OK)
        async def finish_connect():
            for node in self.pipeline.nodes:
                node.start_node_lifecycle.set()  # Set each node's start_node_lifecycle event to allow them to start executing

    async def connect(self):
        async with httpx.AsyncClient() as client:
            # Connect to leaf pipeline
            task = []
            for successor_ip_address in self.successor_ip_addresses:
                pipeline_server_model = PipelineConnectionModel(predecessor_host=self.host, predecessor_port=self.port).model_dump()
                task.append(client.post(f"{successor_ip_address}/connect", json=pipeline_server_model))
            await asyncio.gather(*task)

            # Set each node's establish_connection event to allow them to connect to their remote predecessors
            print("Setting all nodes to establish connection...")
            for node in self.pipeline.nodes:
                node.start_establishing_connection.set()

            # Wait for all nodes to finish establishing connection
            print("Waiting for all nodes to finish establishing connection...")
            for node in self.pipeline.nodes:
                node.finished_establishing_connection.wait()

            # Start the nodes on the successor pipeline before allowing the nodes to start executing
            if len(self.successor_ip_addresses) > 0:
                task = []
                for successor_ip_address in self.successor_ip_addresses:
                    task.append(client.post(f"{successor_ip_address}/finish_connect"))
                await asyncio.gather(*task)

                for node in self.pipeline.nodes:
                    node.start_node_lifecycle.set()

    async def disconnect(self):
        for connector in self.connectors:
            await connector.close_all_clients()
        print("All remote clients closed.")

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