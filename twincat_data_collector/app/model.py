import pyads
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Tuple, List, Union, TypeVar
from ads_communication import AdsPortConnection
from iotdb_utils import IoTTimeSeriesBase
import asyncio

T = TypeVar('T', bound=Union[Tuple[Tuple], pyads.PLCTYPE_BOOL, pyads.PLCTYPE_BYTE, pyads.PLCTYPE_DWORD, pyads.PLCTYPE_INT, pyads.PLCTYPE_DINT, pyads.PLCTYPE_LINT, pyads.PLCTYPE_UDINT, pyads.PLCTYPE_ULINT, pyads.PLCTYPE_REAL, pyads.PLCTYPE_LREAL, pyads.PLCTYPE_STRING, pyads.PLCTYPE_WSTRING])

@dataclass
class EventTaskBase(ABC):
    """ADS Notification event handler abstruct class"""
    subscriber : AdsPortConnection
    mapping_model : T
    watch_symbol : str
    reconnect : bool = field(default_factory=bool)
    queue : asyncio.Queue = field(default=None)

    def __post_init__(self):
        publisher = self.subscriber.reg_notification(symbol=self.watch_symbol, model=self.mapping_model)
        self.queue = publisher.queue

    @abstractmethod
    async def observable(self):
        pass

    async def observer_task(self):
        while True:
            if await self.observable():
                self.reconnect = False

    async def alive_check_task(self):
       while True:
           if self.reconnect:
                print(f"{self.watch_symbol} : Connection lost. Attempting to reconnect...")
                self.subscriber.port_close()
                await asyncio.sleep(1)  # Wait before trying to reconnect
                self.subscriber.port_open()
           try:
               if self.subscriber.connection.is_open:
                    module_state = self.subscriber.connection.read_state()
                    print(f"Port {self.subscriber.ads_port} : ADS State {module_state[0]}, Device state : {module_state[1]}")
                    if (module_state[0] != 5 or module_state[1] != 0):
                        print(f"{self.watch_symbol} : Watch Dog Error")
                        self.reconnect = True
               else:
                   print(f"{self.watch_symbol} : Could not open.")
                   self.reconnect = True
           except pyads.pyads_ex.ADSError as e:
               print(f"{self.watch_symbol} : ADSError : {e}")
               self.reconnect = True
           await asyncio.sleep(10)  # Check every 10 seconds


@dataclass
class IoTDBRecorder(EventTaskBase):
    time_series_manager : IoTTimeSeriesBase = field(default=None, init=True)
    
    """IoTDB recorder"""
    def __post_init__(self):
        super().__post_init__()

    async def observable(self):
        data_count = 0
        while not self.queue.empty():
            data_count += 1
            record = await self.queue.get()
            record["timestamp"] = record["timestamp"].astimezone(ZoneInfo("Japan"))
            new_record = {i: record[i] for i in record if isinstance(record[i], (int, float, bool, str, datetime))}
            if self.time_series_manager.write_data(new_record):
                break
        print(f"{self.watch_symbol} data write count : {data_count}/{self.time_series_manager.chunk_size}")
        if  self.queue.qsize() > 0:
            self.time_series_manager.chunk_size += self.queue.qsize()
        elif self.time_series_manager.chunk_size > data_count:
            self.time_series_manager.chunk_size -= (self.time_series_manager.chunk_size - data_count)
        await asyncio.sleep(1)
        return data_count > 0


@dataclass
class TwinCATStructSymbol:
    type_def: Tuple
    symbols : List[str] = field(default_factory=list)


