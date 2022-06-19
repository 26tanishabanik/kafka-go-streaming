package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"io/ioutil"
	"os"
	"time"
	// "strings"
	"strconv"
	"net/http"
	"github.com/gorilla/mux"

	"github.com/segmentio/kafka-go"
)

const (
	// BootstrapServers : bootstrap servers list
	BootstrapServers string = "BOOTSTRAP_SERVERS"
	// Topic : topic
	Topic string = "TOPIC"
	// GroupID : consumer group
	GroupID string = "GROUP_ID"
	// DelayMs : between sent messages
	DelayMs string = "DELAY_MS"
	// Partition : partition from which to consume
	Partition string = "PARTITION"
)


type Link struct {
	Link        string `json:"link"`
	Description string `json:"description"`
	Topic 		string `json:"topic"`
}

func Producermain() {

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/topiclinks", linksHandler).Methods("POST")

	log.Fatal(http.ListenAndServe(":9090", router))

}

func GetEnv(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if exists {
		return value
	}
	return defaultValue
}

func linksHandler(w http.ResponseWriter, r *http.Request) {

	//Retrieve body from http request
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		panic(err)
	}

	//Save data into Job struct
	var link Link
	err = json.Unmarshal(b, &link)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	SendLinktoproducer(link)

	//Convert job struct into json
	jsonString, err := json.Marshal(link)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	//Set content-type http header
	w.Header().Set("content-type", "application/json")

	//Send back data as response
	w.Write(jsonString)
}

func SendLinktoproducer(link Link) {

	ctx, _ := context.WithCancel(context.Background())
	// bootstrapServers := strings.Split(GetEnv(BootstrapServers, "localhost:9092"), ",")
	topic := GetEnv(Topic, "stackoverflow")
	delayMs, _ := strconv.Atoi(GetEnv(DelayMs, strconv.Itoa(1000)))

	// config := kafka.WriterConfig{
	// 	Brokers:      bootstrapServers,
	// 	Topic:        topic,
	// 	BatchTimeout: 1 * time.Millisecond}

	w := kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: topic,
		Balancer: &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}

	

	defer func() {
		err := w.Close()
		if err != nil {
			fmt.Println("Error closing producer: ", err)
			return
		}
		fmt.Println("Producer closed")
	}()
	// defer cancel()
	jsonString, err := json.Marshal(link)
	if err != nil{
		fmt.Println("Error marshalling json !!")
	}

	linkString := string(jsonString)

	for _, word := range []string{string(linkString)} {
		message := fmt.Sprintf("Message-%s", word)
		err := w.WriteMessages(ctx, kafka.Message{Value: []byte(word)})
		if err == nil {
			fmt.Println("Sent message: ", message)

		} else if err == context.Canceled {
			fmt.Println("Context canceled: ", err)
			break
		} else {
			fmt.Println("Error sending message: ", err)
		}
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}
}
