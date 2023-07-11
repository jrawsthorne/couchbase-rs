package main

import (
	"fmt"

	"github.com/couchbase/gocb/v2"
)

type Airline struct {
	Callsign string `json:"callsign"`
	Country  string `json:"country"`
	Iata     string `json:"iata"`
	Icao     string `json:"icao"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Key      string `json:"key"`
}

func main() {
	cluster, err := gocb.Connect("couchbase://127.0.0.1", gocb.ClusterOptions{
		Username: "Administrator",
		Password: "password",
		SecurityConfig: gocb.SecurityConfig{
			AllowedSaslMechanisms: []gocb.SaslMechanism{gocb.PlainSaslMechanism},
		},
	})
	if err != nil {
		panic(err)
	}

	bucket := cluster.Bucket("travel-sample")
	collection := bucket.DefaultCollection()

	airline := Airline{
		Callsign: "MILE-AIR",
		Country:  "United States",
		Iata:     "Q5",
		Icao:     "MLB",
		Name:     "40-Mile Air",
		Type:     "airline",
		Key:      "airline_10",
	}

	_, err = collection.Upsert(airline.Key, airline, nil)
	if err != nil {
		panic(err)
	}

	resp, err := collection.Get(airline.Key, nil)
	if err != nil {
		panic(err)
	}

	var val Airline
	err = resp.Content(&val)
	if err != nil {
		panic(err)
	}

	fmt.Println(val)
}
