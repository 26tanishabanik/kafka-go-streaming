package main

import (
	"context"
	"log"
	"encoding/json"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"
	"github.com/segmentio/kafka-go"
)

const (
	BootstrapServers string = "BOOTSTRAP_SERVERS"
	Topic string = "TOPIC"
	GroupID string = "GROUP_ID"
	// DelayMs : between sent messages
	DelayMs string = "DELAY_MS"
	Partition string = "PARTITION"
)

type Link struct {
	Link        string `json:"link"`
	Description string `json:"description"`
	Topic 		string `json:"topic"`
}

func GetEnv(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if exists {
		return value
	}
	return defaultValue
}


func main(){
	ctx, _ := context.WithCancel(context.Background())
	bootstrapServers := strings.Split(GetEnv(BootstrapServers, "localhost:9092"), ",")
	topic := GetEnv(Topic, "stackoverflow")
	groupID := GetEnv(GroupID, "onlyme1")
	var path = "/home/tanisha/Desktop/Projects/kafka-go-streaming/ImportantLinks.csv"
	
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil{
		fmt.Println("Error opening file: ", err)
	}
	defer f.Close()
	writercsv := csv.NewWriter(f)
    defer writercsv.Flush()

	config := kafka.ReaderConfig{
		Brokers:  bootstrapServers,
		GroupID:  groupID,
		Topic:    topic,
		MaxWait:  500 * time.Millisecond,
		}

	r := kafka.NewReader(config)

	defer func() {
		err := r.Close()
		if err != nil {
			fmt.Println("Error closing consumer: ", err)
			return
		}
		fmt.Println("Consumer closed")
	}()
	for {
		
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		var d Link
    	err = json.Unmarshal(msg.Value, &d)	
		if err != nil {
			fmt.Println(err)
		}
		record := []string{d.Link, d.Description, d.Topic}
		if _, err := f.WriteString(d.Link +string(',')+d.Description+string(',')+d.Topic+string('\n')); err != nil {
			log.Fatal(err)
		}
		fmt.Println(record)
		fmt.Println("received: ", string(msg.Value))
	}
	

}



