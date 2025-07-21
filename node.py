from typing import List, Dict, Coroutine
from threading import Event, Thread
import random
import asyncio
import httpx
from pydantic import BaseModel
from urllib.parse import urlparse
import time

from fastapi import FastAPI, status
from api import BaseServer



class ConnectionModel(BaseModel):
    node_url: str
    node_type: str



class Connector(FastAPI):
    def __init__(
        self, node: 'BaseNode', 
        host: str, port: int, 
        remote_predecessors: List[str] = None, 
        remote_successors: List[str] = None, 
        ssl_ca_certs: bool | str = False, ssl_certfile: str = None, ssl_keyfile: str = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port
        self.ssl_ca_certs = ssl_ca_certs
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile

        self.remote_predecessors: List[str] = remote_predecessors if remote_predecessors is not None else []
        self.remote_successors: List[str] = remote_successors if remote_successors is not None else []

        # Note: the client will be bound to the PipelineServer's event loop;
        # this happens when the Connector is initialized when PipelineServer call node.setup_connector()
        if self.ssl_ca_certs is None or self.ssl_certfile is None or self.ssl_keyfile is None:
            # If no SSL certificates are provided, create a client without them
            self.client = httpx.AsyncClient()
        else:
            # If SSL certificates are provided, use them to create the client
            try:
                self.client = httpx.AsyncClient(verify=self.ssl_ca_certs, cert=(self.ssl_certfile, self.ssl_keyfile))
            except httpx.ConnectError as e:
                raise ValueError(f"Failed to create HTTP client with SSL certificates: {e}")

        # If SSL certificates are provided, validate the URLs of remote predecessors
        for predecessor_url in self.remote_predecessors:
            parsed_url = urlparse(predecessor_url)
            if parsed_url.scheme != "https":
                raise ValueError(f"Invalid URL scheme for predecessor: {predecessor_url}. Must be 'https'.")
        
        for successor_url in self.remote_successors:
            parsed_url = urlparse(successor_url)
            if parsed_url.scheme != "https":
                raise ValueError(f"Invalid URL scheme for successor: {successor_url}. Must be 'https'.")

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(root: ConnectionModel) -> ConnectionModel:
            self.node.add_remote_predecessor(root.node_url)
            print(f"'{self.node.name}' connected to remote predecessor {root.node_url}")
            return ConnectionModel(node_url=self.get_node_url(), node_type=type(self.node).__name__)
        
        @self.post("/forward_signal", status_code=status.HTTP_200_OK)
        async def forward_signal(root: ConnectionModel):
            self.node.predecessors_events[root.node_url].set()
            print(f"'{self.node.name}' signalled by remote successors {root.node_url}")
            return {"message": "Signalled predecessors"}

        @self.post("/backward_signal", status_code=status.HTTP_200_OK)
        async def backward_signal(leaf: ConnectionModel):
            self.node.successor_events[leaf.node_url].set()
            return {"message": "Signalled predecessors"}
    
    def get_connector_prefix(self):
        return f"/{self.node.name}"
    
    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Set the event loop for the connector. This is done to ensure the connector uses the same event loop as the server.
        """
        self.loop = loop

    def get_node_url(self) -> str:
        return f"https://{self.host}:{self.port}/{self.node.name}"

    async def connect(self) -> List[Coroutine]:
        """
        Connect to all remote predecessors and successors.
        Returns a list of coroutines that can be awaited to perform the connection.
        """
        tasks = []
        for successor_url in self.node.remote_successors:
            json = {
                "node_url": self.get_node_url(),
                "node_type": type(self.node).__name__
            }
            tasks.append(self.client.post(f"{successor_url}/connect", json=json))

        responses = await asyncio.gather(*tasks)
        return responses
    
    def signal_remote_successors(self) -> List[Coroutine]:
        """
        Signal all remote successors that the node has finished processing.
        Returns a list of coroutines that can be awaited to perform the signaling.
        """

        async def _signal_remote_successors() -> List[Coroutine]:
            tasks = []
            for successor_node_url in self.remote_successors:
                json = {
                    "node_url": self.get_node_url(),
                    "node_type": type(self.node).__name__
                }
                tasks.append(self.client.post(f"{successor_node_url}/forward_signal", json=json))

            responses = await asyncio.gather(*tasks)
            return responses

        asyncio.run_coroutine_threadsafe(_signal_remote_successors(), self.loop)
    
    def signal_remote_predecessors(self) -> List[Coroutine]:
        """
        Signal all remote predecessors that the node has finished processing.
        Returns a list of coroutines that can be awaited to perform the signaling.
        """

        async def _signal_remote_predecessors() -> List[Coroutine]:
            tasks = []
            for predecessor_node_url in self.remote_predecessors:
                json = {
                    "node_url": self.get_node_url(),
                    "node_type": type(self.node).__name__
                }
                tasks.append(self.client.post(f"{predecessor_node_url}/backward_signal", json=json))

            responses = await asyncio.gather(*tasks)
            return responses
        
        asyncio.run_coroutine_threadsafe(_signal_remote_predecessors(), self.loop)


class BaseNode(Thread):
    def __init__(
        self, name: str, 
        predecessors: List['BaseNode'] = None, 
        remote_predecessors: List[str] = None, 
        remote_successors: List[str] = None,
        client_url: str = None,
        wait_for_connection: bool = False
    ):
        if remote_predecessors is not None or remote_successors is not None:
            if wait_for_connection is False:
                raise ValueError("Cannot set wait_for_connection to False when either remote_predecessors or remote_successors are provided.")

        self.wait_for_connection = wait_for_connection

        self.loop: asyncio.AbstractEventLoop = None

        self.predecessors = list() if predecessors is None else predecessors
        self.remote_predecessors = list() if remote_predecessors is None else remote_predecessors
        self.predecessors_events: Dict[str, Event] = {predecessor.name: Event() for predecessor in self.predecessors}

        self.successors: List[BaseNode] = list()
        self.remote_successors = list() if remote_successors is None else remote_successors
        self.successor_events: Dict[str, Event] = {successor_url: Event() for successor_url in self.remote_successors}
        self.client_url = client_url

        for event in self.successor_events.values():
            event.set()

        # add node to each predecessor's successors list and create an event for each predecessor's successor_events
        for predecessor in self.predecessors:
            predecessor.successors.append(self)
            predecessor.successor_events[name] = Event()

        self.exit_event = Event()
        self.pause_event = Event()

        self.start_node_lifecycle = Event()
        if not self.wait_for_connection:
            self.start_node_lifecycle.set()
        
        self.pause_event.set()

        super().__init__(name=name)

    def add_remote_predecessor(self, url: str):
        if url not in self.remote_predecessors:
            self.remote_predecessors.append(url)
            self.predecessors_events[url] = Event()

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Set the event loop for the connector. This is done to ensure the connector uses the same event loop as the server.
        """
        self.loop = loop

    def setup_connector(
        self, host: str = None, port: int = None, 
        ssl_ca_certs: str = None, ssl_certfile: str = None, ssl_keyfile: str = None
    ) -> Connector:

        self.connector = Connector(
            node=self, host=host, port=port, 
            remote_predecessors=self.remote_predecessors, remote_successors=self.remote_successors,
            ssl_ca_certs=ssl_ca_certs, ssl_certfile=ssl_certfile, ssl_keyfile=ssl_keyfile
        )
        return self.connector
    
    def setup_server(
        self, host: str = None, port: int = None, 
        ssl_ca_certs: str = None, ssl_certfile: str = None, ssl_keyfile: str = None
    ) -> BaseServer:

        self.server = BaseServer(
            name=self.name, host=host, port=port, client_url=self.client_url,
            ssl_ca_certs=ssl_ca_certs, ssl_certfile=ssl_certfile, ssl_keyfile=ssl_keyfile
        )
        return self.server

    def signal_successors(self):
        if len(self.successors) > 0:
            for successor in self.successors:
                successor.predecessors_events[self.name].set()
        
        try:
            self.connector.signal_remote_successors()
            print(f"'{self.name}' finished signalling remote successors")
        except httpx.ConnectError:
            print(f"'{self.name}' failed to signal successors from {self.name}")
            self.exit()

    def wait_for_successors(self):
        for event in self.successor_events.values():
            event.wait()
        
        for event in self.successor_events.values():
            event.clear()
    
    def signal_predecessors(self):
        if len(self.predecessors) > 0: 
            for predecessor in self.predecessors:
                predecessor.successor_events[self.name].set()

        try:
            self.connector.signal_remote_predecessors()
            print(f"'{self.name}' finished signalling remote predecessors")
        except httpx.ConnectError:
            print(f"'{self.name}' failed to signal predecessors from {self.name}")
            self.exit()
            
    def wait_for_predecessors(self):
        for event in self.predecessors_events.values():
            event.wait()
        
        for event in self.predecessors_events.values():
            event.clear()
    
    def exit(self):
        # set all events so loop can continue to next checkpoint and break out of loop
        self.start_node_lifecycle.set()
        self.pause_event.set()
        self.exit_event.set()

        for event in self.successor_events.values():
            event.set()
        
        for event in self.predecessors_events.values():
            event.set()
    
    def action(self):
        """
        This method should be overridden by subclasses to define the node's action.
        """
        time.sleep(random.randint(1, 3))
    
    def node_lifecycle(self):
        if self.wait_for_connection:
            self.start_node_lifecycle.wait()
            print(f'{self.name} connection established, proceeding to run')

        while self.exit_event.is_set() is False:

            if self.exit_event.is_set(): break
            print(f'{self.name} waiting for predecessors')
            self.wait_for_predecessors()

            if self.exit_event.is_set(): break
            print(f'{self.name} is running')
            self.action()
            print(f'{self.name} is done')

            if self.exit_event.is_set(): break
            print(f'{self.name} signalling successors')
            self.signal_successors()

            if self.exit_event.is_set(): break
            print(f'{self.name} waiting for successors')
            self.wait_for_successors()

            if self.exit_event.is_set(): break
            print(f'{self.name} signalling predecessors')
            self.signal_predecessors()

    def run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.node_lifecycle()