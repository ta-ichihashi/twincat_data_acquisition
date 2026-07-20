from dataclasses import dataclass, field
import asyncio
from model import IoTDBRecorder
from plc_data_types import job_event_structure, axis_to_plc_structure
from ads_communication import AdsPortConnection
import os
from iotdb_utils import SingleTimeSeries, MultiTimeSeries, IoTDBClientSession
import pyads


@dataclass
class TwinCATData:
    motion_observer : IoTDBRecorder = field(default = None)
    job_observer    : IoTDBRecorder = field(default = None)

    def __post_init__(self):
        ams_net_id = os.getenv('TARGET_AMSID', default='15.15.15.15.1.1')
        router = os.getenv('ROUTER_ADDRESS', default='127.0.0.1')

        self.motion_connector = AdsPortConnection(ams_net_id=ams_net_id,
                                    ads_port=501)

        self.plc_connector = AdsPortConnection(ams_net_id=ams_net_id,
                                    ads_port=851)

        self.iotdb_session_manager = IoTDBClientSession(host=os.getenv('IOTDB_HOST', default='127.0.0.1'))
        self.motion_time_series = MultiTimeSeries(
            self.iotdb_session_manager,
            plc_data_model=axis_to_plc_structure, 
            storage_group_name="root.demo1",
            time_series_name="axis1",
            chunk_size = 500
            )
        self.motion_time_series.create_aligned_time_series()

        self.job_time_series = MultiTimeSeries(
            self.iotdb_session_manager, 
            plc_data_model=job_event_structure, 
            storage_group_name="root.demo1",
            time_series_name="job",
            chunk_size = 1
            )
        self.job_time_series.create_aligned_time_series()
        self.main_state_machine = SingleTimeSeries(
            self.iotdb_session_manager, 
            plc_data_model=pyads.PLCTYPE_UINT, 
            storage_group_name="root.demo1.machine_state",
            time_series_name="sequence_state",
            chunk_size = 1
            )
        self.main_state_machine.create_aligned_time_series()
    async def data_collection_task(self):
        self.motion_observer = IoTDBRecorder(
            subscriber=self.motion_connector,
            mapping_model=axis_to_plc_structure,
            watch_symbol='Axes.Axis 1.ToPlc',
            time_series_manager=self.motion_time_series
        )

        self.job_observer = IoTDBRecorder(
            subscriber=self.plc_connector,
            mapping_model=job_event_structure,
            watch_symbol='demo3.runner.event_message',
            time_series_manager=self.job_time_series
        )
        self.main_state_machine_observer = IoTDBRecorder(
            subscriber=self.plc_connector,
            mapping_model=pyads.PLCTYPE_UINT,
            watch_symbol='demo3._state',
            time_series_manager=self.main_state_machine
        )

        await asyncio.gather(
            self.motion_observer.observer_task(),
            self.job_observer.observer_task(),
            self.motion_observer.alive_check_task(),
            self.job_observer.alive_check_task(),
            self.main_state_machine_observer.observer_task(),
            self.main_state_machine_observer.alive_check_task()
        )


def main():    
    twincat = TwinCATData()
    asyncio.run(twincat.data_collection_task())

if __name__ == '__main__':
    main()
