from pydantic import BaseModel
from typing import List
from enum import Enum


class AGGREGATION_TYPES(Enum):
    SUM = 1
    AVERAGE = 2


class Record(BaseModel):
    id_: int
    value: int


class RawRecord(Record):
    group: int


class Dataset(BaseModel):
    id_: int
    data: List[Record] = []

    def sum(self, data: List) -> dict:

        grouped_sums = {}

        for datum in data:
            (group, value) = datum

            if group not in grouped_sums:
                grouped_sums[group] = value
            else:
                grouped_sums[group] += value

        return grouped_sums

    def average(self, data: List) -> dict:

        grouped_averages = {}

        for datum in data:
            (group, value) = datum

            if group not in grouped_averages:
                grouped_averages[group] = {"total": value, "count": 1}
            else:
                grouped_averages[group]["total"] += value
                grouped_averages[group]["count"] += 1

        return {
            group: calculation_components["total"] / calculation_components["count"]
            for group, calculation_components in grouped_averages.items()
        }

    def aggregate(self, on_field: str, _type: AGGREGATION_TYPES):

        aggregation_methods = {
            AGGREGATION_TYPES.SUM: self.sum,
            AGGREGATION_TYPES.AVERAGE: self.average,
        }

        extracted_data = [
            (getattr(record, on_field), record.value) for record in self.data
        ]

        aggregation_pointer = aggregation_methods.get(_type)

        if aggregation_pointer:
            result = aggregation_pointer(data=extracted_data)
            print(result)


if __name__ == "__main__":
    raw_data: List[RawRecord] = [
        RawRecord(id_=1111, group=1, value=100),
        RawRecord(id_=2222, group=2, value=200),
        RawRecord(id_=3333, group=2, value=300),
    ]

    raw_dataset: Dataset = Dataset(id_=99, data=raw_data)

    raw_dataset.aggregate(on_field="group", _type=AGGREGATION_TYPES.SUM)

    raw_dataset.aggregate(on_field="group", _type=AGGREGATION_TYPES.AVERAGE)
