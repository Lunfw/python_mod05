#!/usr/bin/env python3

from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.process_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        self.process_count += 1

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.total_readings = 0
        self.sum_temp = 0.0
        self.readings_count = 0
        self.critical_alerts = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        return f"Stream ID: {data_batch}, Type: Environmental Data"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        super().filter_data(data_batch, criteria)

        if criteria == "high-priority":
            critical = []
            for reading in data_batch:
                if reading.get('type') == 'temp':
                    temp_value = reading.get('value', 0)
                    if temp_value > 30 or temp_value < 15:
                        critical.append(reading)
            return critical

        for reading in data_batch:
            if reading['type'] == 'temp':
                self.sum_temp += reading['value']
        self.total_readings += len(data_batch)
        formatted = ", ".join(
            [f"{item['type']}:{item['value']}" for item in data_batch]
        )
        return f"Processing sensor batch: [{formatted}]"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        avg_temp = (
            f"Sensor analysis: {self.total_readings} readings processed, "
            f"avg temp: {self.sum_temp}°C\n"
        )
        return avg_temp


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.buy = 0
        self.sell = 0
        self.total = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        return f"Stream ID: {data_batch}, Type: Financial Data"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        super().filter_data(data_batch, criteria)

        if criteria == "high-priority":
            large = []
            for reading in data_batch:
                if reading.get('value', 0) > 100:
                    large.append(reading)
            return large

        for reading in data_batch:
            if reading['type'] == 'buy':
                self.buy += reading['value']
            elif reading['type'] == 'sell':
                self.sell += reading['value']
        self.total += len(data_batch)
        formatted = ", ".join(
            [f"{item['type']}:{item['value']}" for item in data_batch]
        )
        return f"Processing sensor batch: [{formatted}]"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        flow = self.buy - self.sell
        if flow > 0:
            operator = "+"
        else:
            operator = "-"
        result = (
            f"Transaction analysis: {self.total} operations, "
            f"net flow: {operator}{flow} units\n"
        )
        return result


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.error = 0
        self.len = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        return f"Stream ID: {data_batch}, Type: System Events"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        super().filter_data(data_batch, criteria)
        for reading in data_batch:
            if reading['type'] == 'error':
                self.error += 1
        self.len += len(data_batch)
        formatted = ", ".join([f"{item['type']}" for item in data_batch])
        return f"Processing sensor batch: [{formatted}]"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        result = (
            f"Event analysis: {self.len} events, "
            f"{self.error} error detected"
        )
        return result


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        if isinstance(stream, DataStream):
            self.streams.append(stream)

    def process_all_streams(
        self,
        data_batches: List[List[Any]]
    ) -> List[str]:
        results = []
        for i, stream in enumerate(self.streams):
            if i < len(data_batches):
                try:
                    result = stream.filter_data(data_batches[i])
                    results.append(str(result))
                except Exception as e:
                    results.append(f"Error: {e}")
        return results

    def get_all_stats(self) -> Dict[str, Dict[str, Union[str, int, float]]]:
        stats = {}
        for stream in self.streams:
            stats[stream.stream_id] = stream.get_stats()
        return stats


def main():
    print("== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor_stream = SensorStream("ENSOR_001")
    process_stream = sensor_stream.process_batch("ENSOR_001")
    print(process_stream)

    data_batch = [
        {"type": "temp", "value": 22.5},
        {"type": "humidity", "value": 65},
        {"type": "pressure", "value": 1013}
    ]

    sensor_batch = sensor_stream.filter_data(data_batch)
    print(sensor_batch)
    sensor_stats = sensor_stream.get_stats()
    print(sensor_stats)

    print("Initializing Transaction Stream...")
    transaction_stream = TransactionStream("TRANS_001")
    process_transaction = transaction_stream.process_batch("TRANS_001")
    print(process_transaction)

    data_filter = [
        {"type": "buy", "value": 100},
        {"type": "sell", "value": 150},
        {"type": "buy", "value": 75}
    ]

    transaction_filter = transaction_stream.filter_data(data_filter)
    print(transaction_filter)
    transaction_stats = transaction_stream.get_stats()
    print(transaction_stats)

    print("Initializing Event Stream...")
    event_stream = EventStream("EVENT_001")
    process_event = event_stream.process_batch("EVENT_001")
    print(process_event)

    data_event = [
        {"type": "login"},
        {"type": "error"},
        {"type": "logout"}
    ]

    filter_event = event_stream.filter_data(data_event)
    print(filter_event)
    print(event_stream.get_stats(), "\n")

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    processor = StreamProcessor()
    s = SensorStream("SENSOR_002")
    t = TransactionStream("TRANS_002")
    e = EventStream("EVENT_002")

    processor.add_stream(s)
    processor.add_stream(t)
    processor.add_stream(e)

    processor.process_all_streams([
        [{"type": "temp", "value": 25}, {"type": "humidity", "value": 70}],
        [
            {"type": "buy", "value": 50},
            {"type": "sell", "value": 30},
            {"type": "buy", "value": 80},
            {"type": "sell", "value": 40}
        ],
        [{"type": "login"}, {"type": "warning"}, {"type": "logout"}]
    ])

    print("Batch 1 Results:")
    print(f"- Sensor data: {s.total_readings} readings processed")
    print(f"- Transaction data: {t.total} operations processed")
    print(f"- Event data: {e.len} events processed\n")

    print("Stream filtering active: High-priority data only")

    sensor_filter_data = [
        {"type": "temp", "value": 35.0},
        {"type": "temp", "value": 10.0},
        {"type": "temp", "value": 22.0}
    ]

    trans_filter_data = [
        {"type": "buy", "value": 150},
        {"type": "sell", "value": 50}
    ]

    sensor_critical = s.filter_data(
        sensor_filter_data,
        criteria="high-priority"
    )
    trans_large = t.filter_data(trans_filter_data, criteria="high-priority")

    filtered_msg = (
        f"Filtered results: {len(sensor_critical)} critical sensor "
        f"alerts, {len(trans_large)} large transaction\n"
    )
    print(filtered_msg)

    print("All streams processed successfully. Nexus throughput optimal.")


main()
