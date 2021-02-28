import uuid
from pydantic import BaseModel, Field
from typing import List
from enum import Enum


class AGGREGATION_TYPES(Enum):
    SUM = 1
    AVERAGE = 2


class Record(BaseModel):
    id_: uuid.UUID = Field(default_factory=uuid.uuid4)
    group: int
    value: int


class Dataset(BaseModel):
    id_: uuid.UUID = Field(default_factory=uuid.uuid4)
    name_: str
    data: List[Record] = []

    def ingest(self, data):
        for group, value in data.items():
            self.data.append(Record(group=group, value=value))

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
            return result


if __name__ == "__main__":
    raw_data: List[Record] = [
        Record(group=1, value=100),
        Record(group=2, value=200),
        Record(group=2, value=300),
    ]

    raw_dataset: Dataset = Dataset(name_="my_raw_data", data=raw_data)

    summed_data = raw_dataset.aggregate(on_field="group", _type=AGGREGATION_TYPES.SUM)

    averaged_data = raw_dataset.aggregate(
        on_field="group", _type=AGGREGATION_TYPES.AVERAGE
    )

    summed_dataset = Dataset(name_="my_summed_data")
    summed_dataset.ingest(data=summed_data)

    averaged_dataset = Dataset(name_="my_averaged_data")
    averaged_dataset.ingest(data=averaged_data)

    print(summed_dataset)
    print()
    print(averaged_dataset)