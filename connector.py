import asyncio
from node import BaseNode



class LeafConnectorNode(BaseNode):
    def __init__(self, name: str):
        super().__init__(name=name)
        self.remote_predecessors = list()
    
    async def run_async(self):
        print(f'{self.name} waiting for root predecessors to connect')

        while len(self.predecessors_events) <= 0:
            await asyncio.sleep(0.1)
            if self.exit_event.is_set(): return

        print(f'{self.name} connected to root predecessors {list(self.predecessors_events.keys())}')

        while self.exit_event.is_set() is False:

            if self.exit_event.is_set(): return
            print(f'{self.name} waiting for predecessors')
            self.wait_for_predecessors()

            if self.exit_event.is_set(): return
            print(f'{self.name} signalling successors')
            await self.signal_successors()

            if self.exit_event.is_set(): return
            print(f'{self.name} waiting for successors')
            self.wait_for_successors()

            if self.exit_event.is_set(): return
            print(f'{self.name} signalling predecessors')
            await self.signal_predecessors()
    