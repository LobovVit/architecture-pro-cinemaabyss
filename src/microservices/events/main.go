package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

type Server struct {
	writerUser    *kafka.Writer
	writerPayment *kafka.Writer
	writerMovie   *kafka.Writer
}

func main() {
	broker := getenv("KAFKA_BROKER", "kafka:9092")

	srv := &Server{
		writerUser:    newWriter(broker, "events.user"),
		writerPayment: newWriter(broker, "events.payment"),
		writerMovie:   newWriter(broker, "events.movie"),
	}

	// Запускаем консьюмеров в фоне
	go consumeLoop(broker, "events.user", "events-consumer-group")
	go consumeLoop(broker, "events.payment", "events-consumer-group")
	go consumeLoop(broker, "events.movie", "events-consumer-group")

	// HTTP маршруты
	http.HandleFunc("/api/events/user", srv.handleUserEvent)
	http.HandleFunc("/api/events/payment", srv.handlePaymentEvent)
	http.HandleFunc("/api/events/movie", srv.handleMovieEvent)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status": true}`))
	})

	http.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status": true}`))
	})

	port := getenv("EVENTS_PORT", "8081")
	log.Printf("Events service listening on :%s, broker=%s", port, broker)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("events server error: %v", err)
	}
}

func (s *Server) handleUserEvent(w http.ResponseWriter, r *http.Request) {
	s.handleEvent(w, r, s.writerUser, "User")
}

func (s *Server) handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	s.handleEvent(w, r, s.writerPayment, "Payment")
}

func (s *Server) handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	s.handleEvent(w, r, s.writerMovie, "Movie")
}

func (s *Server) handleEvent(
	w http.ResponseWriter,
	r *http.Request,
	writer *kafka.Writer,
	eventType string,
) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()
	var evt Event
	if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
		log.Printf("failed to decode %s event: %v", eventType, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(evt)
	if err != nil {
		log.Printf("failed to encode %s event: %v", eventType, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	msg := kafka.Message{
		Key:   []byte(evt.ID),
		Value: data,
	}

	if err := writer.WriteMessages(r.Context(), msg); err != nil {
		log.Printf("failed to send %s event to kafka: %v", eventType, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Printf("Produced %s event: id=%s payload=%s",
		eventType, evt.ID, string(evt.Payload))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated) // 201
	_, _ = w.Write([]byte(`{"status":"success"}`))
}

func newWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
}

func consumeLoop(broker, topic, groupID string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  groupID,
		MaxWait:  500 * time.Millisecond,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer r.Close()

	log.Printf("Consumer started for topic=%s group=%s", topic, groupID)
	ctx := context.Background()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("error reading message from %s: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf("[CONSUMED] topic=%s partition=%d offset=%d key=%s value=%s",
			m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
