package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

// --- Read Model ---
type OrderView struct {
	ID        string    `json:"id"`
	Customer  string    `json:"customer"`
	CreatedAt time.Time `json:"created_at"`
}

var ordersView = make(map[string]OrderView)

const (
	kafkaBroker = "localhost:9092"
	topic       = "orders"
	groupID     = "order-query-consumer"
)

// --- Kafka Consumer ---
func consumeEvents(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topic,
		GroupID: groupID,
	})
	defer r.Close()

	log.Println("[QUERY] Listening for order events...")
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var evt struct {
			OrderID   string `json:"order_id"`
			Customer  string `json:"customer"`
			Timestamp int64  `json:"timestamp"`
		}
		if err := json.Unmarshal(m.Value, &evt); err != nil {
			log.Printf("Invalid message: %v", err)
			continue
		}

		view := OrderView{
			ID:        evt.OrderID,
			Customer:  evt.Customer,
			CreatedAt: time.Unix(evt.Timestamp, 0),
		}

		ordersView[evt.OrderID] = view
		log.Printf("[QUERY] Projection updated: %+v", view)
	}
}

// --- HTTP Handler ---
func getOrdersHandler(w http.ResponseWriter, r *http.Request) {
	var list []OrderView
	for _, v := range ordersView {
		list = append(list, v)
	}
	_ = json.NewEncoder(w).Encode(list)
}

func main() {
	ctx := context.Background()
	go consumeEvents(ctx)

	http.HandleFunc("/orders", getOrdersHandler)
	log.Println("Query service running on :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}
