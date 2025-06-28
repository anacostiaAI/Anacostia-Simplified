from typing import List, Dict
from threading import Event, Thread
import random
import asyncio
import httpx
from pydantic import BaseModel

from fastapi import FastAPI, status



class ConnectionModel(BaseModel):
    node_url: str
    node_type: str


class Connector(FastAPI):
    def __init__(
        self, node: 'BaseNode', 
        host: str, port: int, 
        remote_predecessors: List[Dict[str, str]] = None, 
        remote_successors: List[Dict[str, str]] = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port

        self.remote_predecessors: List[Dict[str, str]] = remote_predecessors if remote_predecessors is not None else []
        self.remote_successors: List[Dict[str, str]] = remote_successors if remote_successors is not None else []

        if self.remote_predecessors is None:
            self.remote_predecessors = []

        self.remote_predecessors_clients = {
            conn['node_url']: httpx.AsyncClient(
                base_url=conn["node_url"], 
                #verify=conn['ssl_ca_certs'], 
                #cert=(conn['ssl_certfile'], conn['ssl_keyfile'])
            ) 
            for conn in self.remote_predecessors
        }

        if self.remote_successors is None:
            self.remote_successors = []

        self.remote_successors_clients = {
            conn['node_url']: httpx.AsyncClient(
                base_url=conn["node_url"], 
                #verify=conn['ssl_ca_certs'], 
                #cert=(conn['ssl_certfile'], conn['ssl_keyfile'])
            ) 
            for conn in self.remote_successors
        }

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(root: ConnectionModel) -> ConnectionModel:
            self.node.add_remote_predecessor(root.node_url)
            return ConnectionModel(node_url=f"http://{self.host}:{self.port}/{self.node.name}", node_type=type(self.node).__name__)
        
        @self.post("/forward_signal", status_code=status.HTTP_200_OK)
        async def forward_signal(root: ConnectionModel):
            self.node.predecessors_events[root.node_url].set()
            return {"message": "Signalled predecessors"}

        @self.post("/backward_signal", status_code=status.HTTP_200_OK)
        async def backward_signal(leaf: ConnectionModel):
            self.node.successor_events[leaf.node_url].set()
            return {"message": "Signalled predecessors"}
    
    def set_host(self, host: str):
        self.host = host
    
    def set_port(self, port: int):
        self.port = port

    def get_connector_prefix(self):
        return f"/{self.node.name}"
    


class BaseNode(Thread):
    def __init__(
        self, name: str, 
        predecessors: List['BaseNode'] = None, 
        remote_predecessors: List[Dict[str, str]] = None, 
        remote_successors: List[Dict[str, str]] = None,
        wait_for_connection: bool = False
    ):
        self.wait_for_connection = wait_for_connection

        self.predecessors = list() if predecessors is None else predecessors
        self.remote_predecessors = list() if remote_predecessors is None else remote_predecessors
        self.predecessors_events: Dict[str, Event] = {predecessor.name: Event() for predecessor in self.predecessors}

        self.successors: List[BaseNode] = list()
        self.remote_successors = list() if remote_successors is None else remote_successors
        self.successor_events: Dict[str, Event] = {conn['node_url']: Event() for conn in self.remote_successors}

        for event in self.successor_events.values():
            event.set()

        # add node to each predecessor's successors list and create an event for each predecessor's successor_events
        for predecessor in self.predecessors:
            predecessor.successors.append(self)
            predecessor.successor_events[name] = Event()

        self.exit_event = Event()
        self.pause_event = Event()
        self.connection_event = Event()
        self.pause_event.set()

        super().__init__(name=name)

    def add_remote_predecessor(self, url: str):
        if url not in self.remote_predecessors:
            self.remote_predecessors.append(url)
            self.predecessors_events[url] = Event()
    
    def setup_connector(self, host: str = None, port: int = None) -> Connector:
        self.connector = Connector(self, host=host, port=port)
        return self.connector
    
    async def signal_successors(self):
        if len(self.successors) > 0:
            for successor in self.successors:
                successor.predecessors_events[self.name].set()
        
        if len(self.remote_successors) > 0:
            try:
                async with httpx.AsyncClient() as client:
                    tasks = []
                    for conn in self.remote_successors:
                        json = {
                            "node_url": f"http://{self.connector.host}:{self.connector.port}/{self.name}",
                            "node_type": type(self).__name__
                        }
                        tasks.append(client.post(f"{conn['node_url']}/forward_signal", json=json))
                    
                    await asyncio.gather(*tasks)
                    print(f"Done signalling remote successors from {self.name}")
            except httpx.ConnectError:
                print(f"Failed to signal successors from {self.name}")
                self.exit()

    def wait_for_successors(self):
        for event in self.successor_events.values():
            event.wait()
        
        for event in self.successor_events.values():
            event.clear()
    
    async def signal_predecessors(self):
        if len(self.predecessors) > 0: 
            for predecessor in self.predecessors:
                predecessor.successor_events[self.name].set()

        if len(self.remote_predecessors) > 0:
            try:
                async with httpx.AsyncClient() as client:
                    tasks = []
                    for predecessor_url in self.remote_predecessors:
                        json = {
                            "node_url": f"http://{self.connector.host}:{self.connector.port}/{self.name}",
                            "node_type": type(self).__name__
                        }
                        tasks.append(client.post(f"{predecessor_url}/backward_signal", json=json))

                    await asyncio.gather(*tasks)
                    print(f"Done signalling remote predecessors from {self.name}")
            except httpx.ConnectError:
                print(f"Failed to signal predecessors from {self.name}")
                self.exit()
            
    def wait_for_predecessors(self):
        for event in self.predecessors_events.values():
            event.wait()
        
        for event in self.predecessors_events.values():
            event.clear()
    
    def exit(self):
        # set all events so loop can continue to next checkpoint and break out of loop
        self.pause_event.set()
        self.exit_event.set()

        for event in self.successor_events.values():
            event.set()
        
        for event in self.predecessors_events.values():
            event.set()
    
    async def node_lifecycle(self):
        if self.wait_for_connection:
            print(f'{self.name} waiting for connection')
            self.connection_event.wait()
            print(f'{self.name} connection established, proceeding to run')

        while self.exit_event.is_set() is False:

            if self.exit_event.is_set(): return
            print(f'{self.name} waiting for predecessors')
            self.wait_for_predecessors()

            if self.exit_event.is_set(): return
            print(f'{self.name} is running')
            await asyncio.sleep(random.randint(1, 3))
            print(f'{self.name} is done')

            if self.exit_event.is_set(): return
            print(f'{self.name} signalling successors')
            await self.signal_successors()

            if self.exit_event.is_set(): return
            print(f'{self.name} waiting for successors')
            self.wait_for_successors()

            if self.exit_event.is_set(): return
            print(f'{self.name} signalling predecessors')
            await self.signal_predecessors()
    
    def run(self) -> None:
        asyncio.run(self.node_lifecycle())