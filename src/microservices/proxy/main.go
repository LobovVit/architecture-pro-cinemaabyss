package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"
)

type backend struct {
	Name   string
	Target *url.URL
	Proxy  *httputil.ReverseProxy
}

func newBackend(name, rawURL string) *backend {
	u, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("invalid %s url %s: %v", name, rawURL, err)
	}
	return &backend{
		Name:   name,
		Target: u,
		Proxy:  httputil.NewSingleHostReverseProxy(u),
	}
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// читаем переменные окружения (должны быть прописаны в docker-compose.yml)
	legacyURL := getenv("MONOLITH_URL", "http://movies-legacy:8080")
	newURL := getenv("MOVIES_SERVICE_URL", "http://movies:8080")
	percentStr := getenv("MOVIES_MIGRATION_PERCENT", "0")

	migrationPercent, err := strconv.Atoi(percentStr)
	if err != nil || migrationPercent < 0 || migrationPercent > 100 {
		log.Printf("invalid MOVIES_MIGRATION_PERCENT=%s, default to 0", percentStr)
		migrationPercent = 0
	}

	log.Printf("Starting proxy. Legacy=%s, New=%s, MigrationPercent=%d",
		legacyURL, newURL, migrationPercent)

	legacy := newBackend("legacy", legacyURL)
	modern := newBackend("movies", newURL)

	// Маршруты
	http.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		handleMovies(w, r, legacy, modern, migrationPercent)
	})

	http.HandleFunc("/api/movies/", func(w http.ResponseWriter, r *http.Request) {
		handleMovies(w, r, legacy, modern, migrationPercent)
	})

	// ДОБАВЛЯЕМ: прокси без миграции, всё в монолит
	http.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Routing %s %s to legacy (users)", r.Method, r.URL.Path)
		legacy.Proxy.ServeHTTP(w, r)
	})

	http.HandleFunc("/api/users/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Routing %s %s to legacy (users)", r.Method, r.URL.Path)
		legacy.Proxy.ServeHTTP(w, r)
	})

	http.HandleFunc("/api/payments", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Routing %s %s to legacy (payments)", r.Method, r.URL.Path)
		legacy.Proxy.ServeHTTP(w, r)
	})

	http.HandleFunc("/api/payments/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Routing %s %s to legacy (payments)", r.Method, r.URL.Path)
		legacy.Proxy.ServeHTTP(w, r)
	})

	// ПРОКСИ ДЛЯ ПОДПИСОК
	http.HandleFunc("/api/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Routing %s %s to legacy (subscriptions)", r.Method, r.URL.Path)
		legacy.Proxy.ServeHTTP(w, r)
	})

	http.HandleFunc("/api/subscriptions/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Routing %s %s to legacy (subscriptions)", r.Method, r.URL.Path)
		legacy.Proxy.ServeHTTP(w, r)
	})

	// healthcheck
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	port := getenv("PROXY_PORT", "8000")
	log.Printf("Listening on :%s ...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("proxy server error: %v", err)
	}
}

func handleMovies(
	w http.ResponseWriter,
	r *http.Request,
	legacy *backend,
	modern *backend,
	migrationPercent int,
) {
	// На каждый запрос кидаем "монетку" по проценту
	n := rand.Intn(100) // 0..99
	var target *backend
	if n < migrationPercent {
		target = modern
	} else {
		target = legacy
	}

	log.Printf("Routing %s %s to %s (rnd=%d, percent=%d)",
		r.Method, r.URL.Path, target.Name, n, migrationPercent)

	// Важно: чтобы backend видел исходный путь "/api/movies..."
	// можно проксировать как есть
	target.Proxy.ServeHTTP(w, r)
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
