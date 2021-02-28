package main

import (
	"fmt"
)

// Record - Representation of a single record in a dataset
type Record struct {
	ID    int
	Group int
	Value float32
}

// Dataset - Representation of a collection of records
type Dataset struct {
	ID      int
	Records []Record
}

// func aggregate(onField, data Dataset) Dataset {
// 	fmt.Println("This is the aggregate function. It should take a collection of RawRecords and output a Dataset")
// 	return Dataset
// }

// go build etl.go && ./etl
func main() {
	fmt.Println("Welcome to EasierETL")

	Data := Dataset{
		ID: 999,
		Records: []Record{
			Record{
				ID:    11,
				Group: 1,
				Value: 100,
			},
			Record{
				ID:    22,
				Group: 2,
				Value: 200,
			},
			Record{
				ID:    33,
				Group: 2,
				Value: 300,
			},
		},
	}

	fmt.Println(Data)
}
