import argparse
import uuid

from pydantic import BaseModel, Field
from typing import List, TypedDict
from enum import Enum


class AGGREGATION_TYPES(Enum):
    SUM = 1
    AVERAGE = 2


# TODO: to make this flexible, allow users to define "schemas" up front so they're writing type-validated records
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

        # TODO: when grouping on multiple columns, create a "composite key"
        extracted_data = [
            (getattr(record, on_field), record.value) for record in self.data
        ]

        aggregation_pointer = aggregation_methods.get(_type)

        if aggregation_pointer:
            result = aggregation_pointer(data=extracted_data)
            return result


if __name__ == "__main__":
    _parser = argparse.ArgumentParser(
        description="Specifying the column to aggregate over and the operation to perform"
    )

    _parser.add_argument(
        "OnField", metavar="on_field", type=str, help="column to group by"
    )
    _parser.add_argument(
        "AggType", metavar="agg_type", type=str, help="operation to perform"
    )

    args = _parser.parse_args()

    raw_records: List[Record] = [
        Record(group=1, value=100),
        Record(group=2, value=200),
        Record(group=2, value=300),
    ]

    raw_dataset: Dataset = Dataset(name_="my_raw_data", data=raw_records)

    aggregated_data = raw_dataset.aggregate(
        on_field=args.OnField, _type=AGGREGATION_TYPES[args.AggType.upper()]
    )

    aggregated_dataset = Dataset(name_="my_aggregated_data")
    aggregated_dataset.ingest(data=aggregated_data)

    print(aggregated_dataset)

    Movie = TypedDict("Movie", {"name": str, "year": int})

    print(Movie

    print(x)

    record_struct = TypedDict("rec", {"group": str, "value": int})
    record: record_struct = {"group": 1, "value": 888}

    print(record_struct)
