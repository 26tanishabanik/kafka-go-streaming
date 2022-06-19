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
	// "reflect"
	//"strconv"
	"github.com/segmentio/kafka-go"
	// "github.com/xuri/excelize/v2"
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
	var path = "ImportantLinks.xlsx"
	// f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE, os.ModeAppend)
	f, err := os.Create(path)
	// f, err := excelize.OpenFile(path)
	// f:= excelize.NewFile()
	if err != nil{
		fmt.Println("Error opening file: ", err)
	}
	defer f.Close()
	writercsv := csv.NewWriter(f)
    defer writercsv.Flush()
	// if err := writercsv.Error(); err != nil {
	// 	fmt.Println(err)
	// }
	// file := excelize.NewFile()

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
	var records [][]string
	// defer func() {
	// 	if err := f.SaveAs(path); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }()
	// i := 1
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
		// var record []string
        // record = append(record, d.Link)
        // record = append(record, d.Description)
        // record = append(record, d.Topic)
		record := []string{d.Link, d.Description, d.Topic}
		// records = append(records, record)
		// f.SetCellValue("Sheet1",strconv.Itoa(i),record)
		// i++
		if err := writercsv.Write(record); err != nil {
			log.Fatalln("error writing record to file", err)
		}
		fmt.Println(records)
		fmt.Println("received: ", string(msg.Value))
	}
	

}



