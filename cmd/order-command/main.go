package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

// --- Domain ---
type Order struct {
	ID        string    `json:"id"`
	Customer  string    `json:"customer"`
	CreatedAt time.Time `json:"created_at"`
}

type OrderCreatedEvent struct {
	OrderID   string `json:"order_id"`
	Customer  string `json:"customer"`
	Timestamp int64  `json:"timestamp"`
}

const (
	kafkaBroker = "localhost:9092"
	topic       = "orders"
)

// --- Kafka Producer ---
func newKafkaWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

// --- HTTP Handler ---
func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ID       string `json:"id"`
		Customer string `json:"customer"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	order := Order{
		ID:        input.ID,
		Customer:  input.Customer,
		CreatedAt: time.Now(),
	}

	log.Printf("[COMMAND] Order created: %+v", order)

	event := OrderCreatedEvent{
		OrderID:   order.ID,
		Customer:  order.Customer,
		Timestamp: time.Now().Unix(),
	}

	data, _ := json.Marshal(event)

	writer := newKafkaWriter()
	defer writer.Close()

	err := writer.WriteMessages(context.Background(),
		kafka.Message{Value: data},
	)
	if err != nil {
		log.Printf("Failed to publish event: %v", err)
		http.Error(w, "failed to publish event", 500)
		return
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(order)
}

func main() {
	http.HandleFunc("/orders", createOrderHandler)
	log.Println("Command service running on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
