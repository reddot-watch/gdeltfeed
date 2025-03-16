package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"html"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/reddot-watch/gdeltfeed/internal/gdelt"
	"github.com/reddot-watch/prefilter"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Config holds application configuration
type Config struct {
	Port                  int
	DBPath                string
	FeedTitle             string
	FeedLink              string
	FeedDescription       string
	NewsCollectionMinutes int
	FeedPruningHours      int
	MaxDBConnections      int
	RequestTimeout        time.Duration
	MaxRequestsPerMinute  int
	xmlFilePath           string
	xmlTempFilePath       string
}

// LoadConfig loads configuration from environment variables and flags
func LoadConfig() Config {
	var config Config

	// Define flags
	flag.IntVar(&config.Port, "port", getEnvInt("PORT", 8080), "Port to run the web server on")
	flag.StringVar(&config.DBPath, "db-path", getEnvStr("DB_PATH", "./gdelt_news_feed.db"), "Path to SQLite database")
	flag.StringVar(&config.FeedTitle, "feed-title", getEnvStr("FEED_TITLE", "GDELT Feed"), "Title of the RSS feed")
	flag.StringVar(&config.FeedLink, "feed-link", getEnvStr("FEED_LINK", "https://reddot.watch/gdelt"), "Link to the feed")
	flag.StringVar(&config.FeedDescription, "feed-desc", getEnvStr("FEED_DESCRIPTION", "A collection of GDELT news items from the last 24 hours"), "Feed description")
	flag.IntVar(&config.NewsCollectionMinutes, "collection-minutes", getEnvInt("COLLECTION_MINUTES", 10), "How often to collect news (in minutes)")
	flag.IntVar(&config.FeedPruningHours, "pruning-hours", getEnvInt("PRUNING_HOURS", 24), "How often to prune the feed (in hours)")
	flag.IntVar(&config.MaxDBConnections, "max-db-connections", getEnvInt("MAX_DB_CONNECTIONS", 10), "Maximum database connections")
	flag.DurationVar(&config.RequestTimeout, "request-timeout", getEnvDuration("REQUEST_TIMEOUT", 10*time.Second), "HTTP request timeout")
	flag.IntVar(&config.MaxRequestsPerMinute, "max-requests", getEnvInt("MAX_REQUESTS_PER_MINUTE", 60), "Maximum requests per minute")

	flag.Parse()

	config.xmlFilePath = filepath.Join(filepath.Dir(config.DBPath), "feed.xml")
	config.xmlTempFilePath = filepath.Join(filepath.Dir(config.DBPath), "feed.xml.tmp")

	return config
}

// Helper functions for environment variables
func getEnvStr(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if valueStr, exists := os.LookupEnv(key); exists {
		if value, err := strconv.Atoi(valueStr); err == nil {
			return value
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if valueStr, exists := os.LookupEnv(key); exists {
		if value, err := time.ParseDuration(valueStr); err == nil {
			return value
		}
	}
	return fallback
}

// NewsItem represents a single news article
type NewsItem struct {
	ID          int64     `xml:"-"`
	Title       string    `xml:"title"`
	Link        string    `xml:"link"`
	Description string    `xml:"description"`
	PubDate     time.Time `xml:"pubDate"`
	GUID        string    `xml:"guid"`
	Source      string    `xml:"source,omitempty"`
}

// Validate ensures the NewsItem has valid data
func (item *NewsItem) Validate() error {
	if item.Title == "" {
		return errors.New("news item title cannot be empty")
	}
	if item.Link == "" {
		return errors.New("news item link cannot be empty")
	}
	return nil
}

// Sanitize makes the news item XML-safe
func (item *NewsItem) Sanitize() {
	item.Title = html.EscapeString(item.Title)
	item.Description = html.EscapeString(item.Description)
	item.Source = html.EscapeString(item.Source)
}

// NewsFeed represents the RSS feed
type NewsFeed struct {
	XMLName      xml.Name `xml:"rss"`
	Version      string   `xml:"version,attr"`
	Channel      Channel  `xml:"channel"`
	newsItemsMux sync.RWMutex
	db           *sql.DB
	filter       *prefilter.Filter
	config       Config
	rateLimiter  *RateLimiter
	xmlFileMux   sync.RWMutex
}

// Channel contains feed metadata and items
type Channel struct {
	Title         string     `xml:"title"`
	Link          string     `xml:"link"`
	Description   string     `xml:"description"`
	Language      string     `xml:"language"`
	LastBuildDate time.Time  `xml:"lastBuildDate"`
	Items         []NewsItem `xml:"item"`
}

// RateLimiter implements a simple token bucket rate limiter
type RateLimiter struct {
	tokens        int
	maxTokens     int
	tokenInterval time.Duration
	mu            sync.Mutex
	lastRefill    time.Time
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(maxRequestsPerMinute int) *RateLimiter {
	return &RateLimiter{
		tokens:        maxRequestsPerMinute,
		maxTokens:     maxRequestsPerMinute,
		tokenInterval: time.Minute / time.Duration(maxRequestsPerMinute),
		lastRefill:    time.Now(),
	}
}

// Allow checks if a request is allowed based on rate limits
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Refill tokens based on elapsed time
	now := time.Now()
	elapsed := now.Sub(rl.lastRefill)
	newTokens := int(elapsed / rl.tokenInterval)

	if newTokens > 0 {
		rl.tokens = min(rl.tokens+newTokens, rl.maxTokens)
		rl.lastRefill = now
	}

	if rl.tokens > 0 {
		rl.tokens--
		return true
	}
	return false
}

// NewNewsFeed creates and initializes a new feed with database connection
func NewNewsFeed(ctx context.Context, config Config) (*NewsFeed, error) {
	db, err := sql.Open("sqlite3", config.DBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set connection pool limits
	db.SetMaxOpenConns(config.MaxDBConnections)
	db.SetMaxIdleConns(config.MaxDBConnections / 2)
	db.SetConnMaxLifetime(1 * time.Hour)

	// Test database connection
	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctxTimeout); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Create table if it doesn't exist
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS news_items (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			link TEXT NOT NULL UNIQUE,
			description TEXT NOT NULL,
			pub_date TEXT NOT NULL,
			guid TEXT NOT NULL UNIQUE,
			source TEXT
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	prefilter, err := prefilter.NewFilter("en", prefilter.DefaultOptions())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create filter: %w", err)
	}

	return &NewsFeed{
		Version: "2.0",
		Channel: Channel{
			Title:         config.FeedTitle,
			Link:          config.FeedLink,
			Description:   config.FeedDescription,
			Language:      "en-us",
			LastBuildDate: time.Now(),
			Items:         []NewsItem{},
		},
		db:          db,
		filter:      prefilter,
		config:      config,
		rateLimiter: NewRateLimiter(config.MaxRequestsPerMinute),
	}, nil
}

// Close closes the database connection
func (f *NewsFeed) Close() error {
	return f.db.Close()
}

// GenerateGUID creates a unique identifier for a news item
func GenerateGUID(item NewsItem) string {
	// Use UUID v4 (random)
	id := uuid.New().String()

	// Create deterministic ID based on content
	// This ensures the same article always gets the same GUID
	// and helps with deduplication
	h := sha256.New()
	h.Write([]byte(item.Link))
	h.Write([]byte(item.Title))
	contentHash := hex.EncodeToString(h.Sum(nil))

	// Combine both for extra uniqueness and determinism
	return fmt.Sprintf("%s-%s", id, contentHash[:8])
}

// LinkExists checks if a news item with the given link already exists
func (f *NewsFeed) LinkExists(ctx context.Context, link string) (bool, error) {
	var count int
	err := f.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM news_items WHERE link = ?", link).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check link existence: %w", err)
	}
	return count > 0, nil
}

// AddNewsItem adds a news item to the feed and database
func (f *NewsFeed) AddNewsItem(ctx context.Context, item NewsItem) (bool, error) {
	f.newsItemsMux.Lock()
	defer f.newsItemsMux.Unlock()

	if err := item.Validate(); err != nil {
		return false, fmt.Errorf("invalid news item: %w", err)
	}

	item.Sanitize()

	exists, err := f.LinkExists(ctx, item.Link)
	if err != nil {
		return false, err
	}

	if exists {
		log.Debug().Str("link", item.Link).Msg("Skipping item with duplicate link")
		return false, nil // Skip without error
	}

	if item.GUID == "" {
		item.GUID = GenerateGUID(item)
	}

	_, err = f.db.ExecContext(
		ctx,
		`INSERT INTO news_items (title, link, description, pub_date, guid, source) 
		 VALUES (?, ?, ?, ?, ?, ?)`,
		item.Title, item.Link, item.Description, item.PubDate.Format(time.RFC3339), item.GUID, item.Source,
	)
	if err != nil {
		return false, fmt.Errorf("failed to insert news item: %w", err)
	}

	return true, nil
}

// PruneFeed removes items older than the specified pruning hours
func (f *NewsFeed) PruneFeed(ctx context.Context) error {
	cutoffTime := time.Now().Add(-time.Duration(f.config.FeedPruningHours) * time.Hour).Format(time.RFC3339)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err := f.db.ExecContext(ctx, "DELETE FROM news_items WHERE pub_date < ?", cutoffTime)
	if err != nil {
		return fmt.Errorf("failed to prune old items: %w", err)
	}
	return nil
}

// GetNewsItems fetches all news items from the recent period
func (f *NewsFeed) GetNewsItems(ctx context.Context) ([]NewsItem, error) {
	f.newsItemsMux.RLock()
	defer f.newsItemsMux.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Get items from the last period
	cutoffTime := time.Now().Add(-time.Duration(f.config.FeedPruningHours) * time.Hour).Format(time.RFC3339)
	rows, err := f.db.QueryContext(
		ctx,
		`SELECT id, title, link, description, pub_date, guid, source 
		 FROM news_items 
		 WHERE pub_date > ? 
		 ORDER BY pub_date DESC`,
		cutoffTime,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query news items: %w", err)
	}
	defer rows.Close()

	var items []NewsItem
	for rows.Next() {
		var item NewsItem
		var pubDateStr string
		err := rows.Scan(&item.ID, &item.Title, &item.Link, &item.Description, &pubDateStr, &item.GUID, &item.Source)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		item.PubDate, err = time.Parse(time.RFC3339, pubDateStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse date '%s': %w", pubDateStr, err)
		}

		item.Sanitize()

		items = append(items, item)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return items, nil
}

// GetXML returns the XML representation of the feed
func (f *NewsFeed) GetXML(ctx context.Context) ([]byte, error) {
	items, err := f.GetNewsItems(ctx)
	if err != nil {
		return nil, err
	}

	// Update channel with current items
	f.Channel.LastBuildDate = time.Now()
	f.Channel.Items = items

	return xml.MarshalIndent(f, "", "  ")
}

// RateLimitMiddleware adds rate limiting to handlers
func (f *NewsFeed) RateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !f.rateLimiter.Allow() {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// New method to generate and save XML to file
func (f *NewsFeed) GenerateAndSaveXML(ctx context.Context) error {
	// Lock during file writing
	f.xmlFileMux.Lock()
	defer f.xmlFileMux.Unlock()

	xmlData, err := f.GetXML(ctx)
	if err != nil {
		return err
	}

	// Write to temporary file first
	if err := os.WriteFile(f.config.xmlTempFilePath, xmlData, 0644); err != nil {
		return fmt.Errorf("failed to write temp XML file: %w", err)
	}

	// Atomic rename to ensure consistency
	if err := os.Rename(f.config.xmlTempFilePath, f.config.xmlFilePath); err != nil {
		return fmt.Errorf("failed to replace XML file: %w", err)
	}

	log.Info().Str("path", f.config.xmlFilePath).Msg("XML feed file updated")
	return nil
}

// FeedHandler serves the static XML file
func (f *NewsFeed) FeedHandler(w http.ResponseWriter, r *http.Request) {
	// Add read lock to ensure file isn't being written during read
	f.xmlFileMux.RLock()
	defer f.xmlFileMux.RUnlock()

	if _, err := os.Stat(f.config.xmlFilePath); os.IsNotExist(err) {
		log.Error().Str("path", f.config.xmlFilePath).Msg("Feed file does not exist")
		// Return 503 Service Unavailable with a meaningful message
		w.Header().Set("Retry-After", "60") // Suggest client retry after 60 seconds
		http.Error(w, "Feed is currently being generated. Please try again later.",
			http.StatusServiceUnavailable)
		return
	}

	// Set caching headers
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Cache-Control", "public, max-age=300") // 5-minute cache

	http.ServeFile(w, r, f.config.xmlFilePath)
}

// HealthCheckHandler provides a basic health check endpoint
func (f *NewsFeed) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := f.db.PingContext(ctx); err != nil {
		log.Error().Err(err).Msg("Health check failed - database error")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "unhealthy - database error")
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "healthy")
}

// CollectNews fetches and processes news with retry logic
func (f *NewsFeed) CollectNews(ctx context.Context) (int, error) {
	log.Info().Msg("Collecting news...")

	// Define retry parameters
	maxRetries := 3
	backoffDuration := 5 * time.Second

	// Effectively collected news
	collected := 0

	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			log.Info().Int("attempt", attempt+1).Dur("backoff", backoffDuration).Msg("Retrying news collection")
			select {
			case <-time.After(backoffDuration):
				// Continue with retry
				backoffDuration *= 2 // Exponential backoff
			case <-ctx.Done():
				return collected, ctx.Err()
			}
		}

		// Fetch events with context and timeout
		fetchCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		opts := gdelt.DefaultOpts
		opts.Translingual = false
		events, err := gdelt.FetchLatestEvents(fetchCtx, opts)
		cancel()

		if err != nil {
			lastErr = fmt.Errorf("error fetching latest events (attempt %d): %w", attempt+1, err)
			log.Error().Err(err).Int("attempt", attempt+1).Msg("Failed to fetch events")
			continue // Try again
		}

		log.Info().Int("count", len(events)).Msg("Processing events")

		for _, event := range events {
			select {
			case <-ctx.Done():
				return collected, ctx.Err()
			default:
				// Continue processing
			}

			if !f.filter.Match(event.GKGArticle.Extras.PageTitle) {
				log.Debug().Str("title", event.GKGArticle.Extras.PageTitle).Msg("Skipping event due to filter")
				continue
			}

			newItem := NewsItem{
				Title:       event.GKGArticle.Extras.PageTitle,
				Link:        event.SourceURL,
				PubDate:     event.PublishedAt(),
				Source:      "GDELT",
				Description: "", // Empty description, could be filled with summary or excerpt
			}

			newItem.GUID = GenerateGUID(newItem)

			// Set a timeout for adding each item
			itemCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			inserted, err := f.AddNewsItem(itemCtx, newItem)
			cancel()

			if err != nil {
				log.Warn().Err(err).Str("title", newItem.Title).Msg("Error adding new item")
				// Continue with other items rather than failing completely
				continue
			} else if inserted {
				collected++
			}
		}

		log.Info().Int("count", collected).Msg("Collected new items")
		return collected, nil // Success, exit the retry loop
	}

	return collected, fmt.Errorf("failed to collect news after %d attempts: %w", maxRetries, lastErr)
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if os.Getenv("DEBUG") == "true" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	config := LoadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	feed, err := NewNewsFeed(ctx, config)
	if err != nil {
		log.Fatal().Err(err).Msg("Error initializing feed")
	}
	defer feed.Close()

	newsCollectionTicker := time.NewTicker(time.Duration(config.NewsCollectionMinutes) * time.Minute)
	feedPruningTicker := time.NewTicker(time.Duration(config.FeedPruningHours) * time.Hour)
	defer newsCollectionTicker.Stop()
	defer feedPruningTicker.Stop()

	updateFeed := func() {
		// Collect news immediately at startup
		collected, err := feed.CollectNews(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Error().Err(err).Msg("Initial news collection failed")
		}

		if collected > 0 {
			// Generate new XML file after successful collection
			if err := feed.GenerateAndSaveXML(ctx); err != nil {
				log.Error().Err(err).Msg("XML generation failed")
			}
		}
	}

	// Background worker for collection and pruning
	go func() {
		updateFeed()

		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("Shutting down background worker")
				return
			case <-newsCollectionTicker.C:
				updateFeed()
			case <-feedPruningTicker.C:
				if err := feed.PruneFeed(ctx); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					log.Error().Err(err).Msg("Feed pruning failed")
				}
			}
		}
	}()

	mux := http.NewServeMux()

	// Apply rate limiting middleware to feed endpoint
	feedHandler := feed.RateLimitMiddleware(http.HandlerFunc(feed.FeedHandler))
	mux.Handle("/feed.xml", feedHandler)

	// Add health check endpoint
	mux.HandleFunc("/health", feed.HealthCheckHandler)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", config.Port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info().Int("port", config.Port).Msg("Starting server")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("Server error")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("Shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("Server shutdown error")
	}

	log.Info().Msg("Server gracefully stopped")
}
