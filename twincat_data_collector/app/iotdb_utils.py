import numpy as np
from iotdb.Session import Session
from iotdb.SessionPool import PoolConfig, SessionPool
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.NumpyTablet import ColumnType, NumpyTablet
from iotdb.utils.exception import StatementExecutionException, IoTDBConnectionException
from dataclasses import dataclass, field
from typing import Tuple, Union, TypeVar
from abc import ABC, abstractmethod
from enum import Enum
from collections import deque
import time
import pyads
from pprint import pprint

class ctsDataType(Enum):
    c_bool = TSDataType.BOOLEAN
    c_byte = TSDataType.INT32
    c_short = TSDataType.INT32
    c_int = TSDataType.INT32
    c_long = TSDataType.INT64
    c_longlong = TSDataType.INT64
    c_ubyte = TSDataType.INT32
    c_ushort = TSDataType.INT32
    c_uint = TSDataType.INT64
    c_ulong = TSDataType.BLOB
    c_ulonglong = TSDataType.BLOB
    c_double = TSDataType.DOUBLE
    c_float = TSDataType.FLOAT
    c_char = TSDataType.TEXT


@dataclass
class IoTDBClientSession:
    host: str = '127.0.0.1'
    port: int = 6667
    username: str = 'root'
    password: str = 'root'
    session_pool : SessionPool = field(default=None)

    def __post_init__(self):
        # 夏時間が有効かどうかのフラグ
        is_dst = time.daylight
        # 時差を計算（秒）
        offset = -time.altzone if is_dst else -time.timezone
        offset = int(offset / 3600)
        pool_config = PoolConfig(
            host=self.host,
            port=self.port,
            user_name=self.username,
            password=self.password,
            fetch_size=1024,
            time_zone=f"UTC{offset:+d}",
            enable_redirection=True
        )
        self.session_pool = SessionPool(pool_config, max_pool_size=5, wait_timeout_in_ms=3000)
        
    def close(self):
        if self.session is not None:
            self.session.close()
            self.session = None


T = TypeVar('T', bound=Union[Tuple[Tuple], pyads.PLCTYPE_BOOL, pyads.PLCTYPE_BYTE, pyads.PLCTYPE_DWORD, pyads.PLCTYPE_INT, pyads.PLCTYPE_DINT, pyads.PLCTYPE_LINT, pyads.PLCTYPE_UDINT, pyads.PLCTYPE_ULINT, pyads.PLCTYPE_REAL, pyads.PLCTYPE_LREAL, pyads.PLCTYPE_STRING, pyads.PLCTYPE_WSTRING])

@dataclass
class IoTTimeSeriesBase(ABC):
    session_manager : IoTDBClientSession
    plc_data_model : T
    storage_group_name : str
    time_series_name : str
    chunk_size : int = field(default=5)
    queue : deque = field(default_factory=deque)

    def __post_init__(self):
        self.create_storage_group()
        self.measurements_list = list()
    
    @classmethod
    def session(cls, func):
        def wrapper(self, *args, **kwargs):
            try:
                session = self.session_manager.session_pool.get_session()
                func(self,  session, *args, **kwargs)
                self.session_manager.session_pool.put_back(session)
            except IoTDBConnectionException as e:
                print(f"Connection failed for {self.session_manager.host}:{self.session_manager.port}: {e}")
                exit(1)
        return wrapper

    @abstractmethod
    def create_storage_group(self, session : Session):
        pass


    def write_data(self, data) -> bool:
        self.queue.append(data)
        if len(self.queue) > self.chunk_size:
            self.insert_data()
            return True
        else:
            return False 

    @abstractmethod
    def insert_data(self, session : Session):
        pass

    def q_size(self):
        return len(self.queue)


class MultiTimeSeries(IoTTimeSeriesBase):
    
    def __post_init__(self):
        super().__post_init__()
        if not isinstance(self.plc_data_model, tuple) or len(self.plc_data_model) <= 1 or not isinstance(self.plc_data_model[0], tuple):
            raise ValueError("plc_data_model must be a tuple of tuples for MultiTimeSeries")
        
        self.ts_type_dict = dict()

    @IoTTimeSeriesBase.session
    def create_storage_group(self, session : Session):
        try:
            session.set_storage_group(self.storage_group_name)
        except StatementExecutionException as e:
            pass
        except IoTDBConnectionException as e:
            print(f"Connection fail {self.session_manager.host}, {self.session_manager.port}")

    
    @IoTTimeSeriesBase.session
    def create_aligned_time_series(self, session : Session):
        try:
            if isinstance(self.plc_data_model, tuple) and len(self.plc_data_model) > 1 and isinstance(self.plc_data_model[0], tuple):
                transported = [list(row) for row in zip(*self.plc_data_model)]
                self.ts_type_dict = {
                    transported[0][i] : ctsDataType[v.__name__].value for i, v in enumerate(transported[1])
                }
                self.measurements_list = [i for i in transported[0] if i != "timestamp"]
                ts_type_list = [self.ts_type_dict[k] for k in self.measurements_list]
                ts_path_list = [f"{self.storage_group_name}.{self.time_series_name}.{item}" for item in transported[0]]
                encoding_lst = [TSEncoding.PLAIN for _ in range(len(ts_path_list))]
                compressor_lst = [Compressor.SNAPPY for _ in range(len(ts_path_list))]
                session.create_multi_time_series(
                    ts_path_list, ts_type_list, encoding_lst, compressor_lst
                )
            else:
                raise ValueError("plc_data_model must be a tuple of tuples for MultiTimeSeries")
            print(f"Created aligned time series: {self.storage_group_name}.{self.time_series_name} with measurements {self.measurements_list}")
        except StatementExecutionException as e:
            pass
    
    @IoTTimeSeriesBase.session
    def insert_data(self, session : Session):
        try:
            if isinstance(self.plc_data_model, tuple) and len(self.plc_data_model) > 1 and isinstance(self.plc_data_model[0], tuple):
                chunk = list(self.queue) 
            else:
                raise ValueError("plc_data_model must be a tuple of tuples for MultiTimeSeries")
            times_list = np.array([int(r["timestamp"].timestamp() * 10**6) for r in chunk],  TSDataType.INT64.np_dtype())
            measurements_list = [(i, v) for i, v in enumerate(self.measurements_list) if v in chunk[0]]
            values_list = [
                np.array([l[v] for l in chunk], self.ts_type_dict[v].np_dtype()) 
                for i, v in measurements_list
            ]
            self.queue.clear()
            tablet = NumpyTablet(
                f"{self.storage_group_name}.{self.time_series_name}",
                [v for i, v in measurements_list],
                [self.ts_type_dict[v] for i, v in measurements_list],
                values_list,
                times_list
            )
            session.insert_tablet(tablet)
        except Exception as e:
            pprint(chunk)
            raise(e)
        

class SingleTimeSeries(IoTTimeSeriesBase):

    def __post_init__(self):
        super().__post_init__()
        if isinstance(self.plc_data_model, tuple) and len(self.plc_data_model) > 1 and isinstance(self.plc_data_model[0], tuple):
            raise ValueError("plc_data_model must be a single type for SingleTimeSeries")
        self.ts_type_dict = {self.time_series_name : ctsDataType[self.plc_data_model.__name__].value}
        self.measurements_list = [self.time_series_name]


    @IoTTimeSeriesBase.session
    def create_storage_group(self, session : Session):
        try:
            session.set_storage_group(self.storage_group_name)
        except StatementExecutionException as e:
            pass
        except IoTDBConnectionException as e:
            print(f"Connection fail {self.session_manager.host}, {self.session_manager.port}")

    @IoTTimeSeriesBase.session
    def create_aligned_time_series(self, session : Session):
        pass
        try:
            if isinstance(self.plc_data_model, tuple) and len(self.plc_data_model) > 1 and isinstance(self.plc_data_model[0], tuple):
                raise ValueError("plc_data_model must be a single type for SingleTimeSeries")
            else:
                session.create_time_series(
                    f"{self.storage_group_name}.{self.time_series_name}", 
                    ctsDataType[self.plc_data_model.__name__].value, 
                    TSEncoding.PLAIN, 
                    Compressor.SNAPPY
                )
            print(f"Created aligned time series: {self.storage_group_name}.{self.time_series_name}")
        except StatementExecutionException as e:
            pass

    @IoTTimeSeriesBase.session
    def insert_data(self, session : Session):
        try:
            if isinstance(self.plc_data_model, tuple) and len(self.plc_data_model) > 1 and isinstance(self.plc_data_model[0], tuple):
                raise ValueError("plc_data_model must be a single type for SingleTimeSeries")
            else:
                chunk = list(self.queue)
            times_list = np.array([int(r["timestamp"].timestamp() * 10**6) for r in chunk],  TSDataType.INT64.np_dtype())
            keys = [k for k in chunk[0] if k != "timestamp"]
            values_list = np.array([r[keys[0]] for r in chunk], self.ts_type_dict[self.time_series_name].np_dtype())
            self.queue.clear()
            tablet = NumpyTablet(
                f"{self.storage_group_name}",
                [self.time_series_name],
                [self.ts_type_dict[self.time_series_name]],
                [values_list],
                times_list
            )
            print(f"Tablet: {self.ts_type_dict}, {self.measurements_list}, {len(times_list)}, {values_list}")
            session.insert_tablet(tablet)
        except Exception as e:
            pprint(chunk)
            raise(e)