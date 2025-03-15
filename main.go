package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/xml"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/reddot-watch/gdeltfeed/internal/gdelt"
	"github.com/reddot-watch/prefilter"
	"github.com/rs/zerolog/log"
)

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

// NewsFeed represents the RSS feed
type NewsFeed struct {
	XMLName      xml.Name `xml:"rss"`
	Version      string   `xml:"version,attr"`
	Channel      Channel  `xml:"channel"`
	newsItemsMux sync.RWMutex
	db           *sql.DB
	filter       *prefilter.Filter
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

// NewNewsFeed creates and initializes a new feed with database connection
func NewNewsFeed(title, link, description, dbPath string) (*NewsFeed, error) {
	// Open SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Create table if it doesn't exist
	_, err = db.Exec(`
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
			Title:         title,
			Link:          link,
			Description:   description,
			Language:      "en-us",
			LastBuildDate: time.Now(),
			Items:         []NewsItem{},
		},
		db:     db,
		filter: prefilter,
	}, nil
}

// Close closes the database connection
func (f *NewsFeed) Close() error {
	return f.db.Close()
}

// GenerateGUID creates a unique identifier for a news item
func GenerateGUID(item NewsItem) string {
	// Method 1: Use UUID v4 (random)
	id := uuid.New().String()

	// Method 2: Create deterministic ID based on content
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
func (f *NewsFeed) LinkExists(link string) (bool, error) {
	var count int
	err := f.db.QueryRow("SELECT COUNT(*) FROM news_items WHERE link = ?", link).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check link existence: %w", err)
	}
	return count > 0, nil
}

// AddNewsItem adds a news item to the feed and database
func (f *NewsFeed) AddNewsItem(item NewsItem) (bool, error) {
	f.newsItemsMux.Lock()
	defer f.newsItemsMux.Unlock()

	// Check if link already exists
	exists, err := f.LinkExists(item.Link)
	if err != nil {
		return false, err
	}

	if exists {
		log.Printf("Skipping item with duplicate link: %s", item.Link)
		return false, nil // Skip without error
	}

	// Generate a truly unique GUID if one wasn't provided
	if item.GUID == "" {
		item.GUID = GenerateGUID(item)
	}

	// Insert into database - store date in ISO 8601 format
	_, err = f.db.Exec(
		`INSERT INTO news_items (title, link, description, pub_date, guid, source) 
		 VALUES (?, ?, ?, ?, ?, ?)`,
		item.Title, item.Link, item.Description, item.PubDate.Format(time.RFC3339), item.GUID, item.Source,
	)
	if err != nil {
		return false, fmt.Errorf("failed to insert news item: %w", err)
	}

	return true, nil
}

// PruneFeed removes items older than 24 hours
func (f *NewsFeed) PruneFeed() error {
	cutoffTime := time.Now().Add(-24 * time.Hour).Format(time.RFC3339)
	_, err := f.db.Exec("DELETE FROM news_items WHERE pub_date < ?", cutoffTime)
	if err != nil {
		return fmt.Errorf("failed to prune old items: %w", err)
	}
	return nil
}

// GetNewsItems fetches all news items from the last 24 hours
func (f *NewsFeed) GetNewsItems() ([]NewsItem, error) {
	f.newsItemsMux.RLock()
	defer f.newsItemsMux.RUnlock()

	// Get items from the last 24 hours
	cutoffTime := time.Now().Add(-24 * time.Hour).Format(time.RFC3339)
	rows, err := f.db.Query(
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

		// Parse the timestamp using RFC3339 format
		item.PubDate, err = time.Parse(time.RFC3339, pubDateStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse date '%s': %w", pubDateStr, err)
		}

		items = append(items, item)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return items, nil
}

// GetXML returns the XML representation of the feed
func (f *NewsFeed) GetXML() ([]byte, error) {
	items, err := f.GetNewsItems()
	if err != nil {
		return nil, err
	}

	// Update channel with current items
	f.Channel.LastBuildDate = time.Now()
	f.Channel.Items = items

	return xml.MarshalIndent(f, "", "  ")
}

// FeedHandler handles HTTP requests for the feed
func (f *NewsFeed) FeedHandler(w http.ResponseWriter, r *http.Request) {
	xmlData, err := f.GetXML()
	if err != nil {
		log.Printf("Error generating feed: %v", err)
		http.Error(w, "Error generating feed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Write(xmlData)
}

// CollectNews fetches and processes news
func (f *NewsFeed) CollectNews() error {
	log.Info().Msg("Collecting news...")

	opts := gdelt.DefaultOpts
	opts.Translingual = false
	events, err := gdelt.FetchLatestEvents(opts)
	if err != nil {
		return fmt.Errorf("error fetching latest events: %w", err)
	}

	log.Info().Int("count", len(events)).Msg("processing events")

	collected := 0
	for _, event := range events {
		if !f.filter.Match(event.GKGArticle.Extras.PageTitle) {
			log.Debug().Str("title", event.GKGArticle.Extras.PageTitle).Msg("skipping event due to filter")
			continue
		}

		newItem := NewsItem{
			Title:   event.GKGArticle.Extras.PageTitle,
			Link:    event.SourceURL,
			PubDate: event.PublishedAt(),
			Source:  "GDELT",
		}

		newItem.GUID = GenerateGUID(newItem)

		if inserted, err := f.AddNewsItem(newItem); err != nil {
			log.Warn().Err(err).Str("title", newItem.Title).Msg("error adding new item")
			// Continue with other items rather than failing completely
			continue
		} else if inserted {
			collected += 1
		}
	}

	log.Info().Int("count", collected).Msgf("collected %d items", collected)
	return nil
}

func main() {
	port := flag.Int("port", 8080, "Port to run the web server on")
	flag.Parse()

	feed, err := NewNewsFeed(
		"GDELT Feed",
		"https://reddot.watch/gdelt",
		"A collection of GDELT news items from the last 24 hours",
		"./gdelt_news_feed.db",
	)
	if err != nil {
		log.Err(err).Msg("error initializing feed")
	}
	defer feed.Close()

	newsCollectionTicker := time.NewTicker(10 * time.Minute)
	feedPruningTicker := time.NewTicker(24 * time.Hour)

	go func() {
		// Collect news immediately at startup
		if err := feed.CollectNews(); err != nil {
			log.Error().Err(err).Msg("initial news collection failed")
			// Consider if you want to exit here or continue with the tickers
		}

		for {
			select {
			case <-newsCollectionTicker.C:
				if err := feed.CollectNews(); err != nil {
					log.Error().Err(err).Msg("periodic news collection failed")
					// Continue execution, don't exit the goroutine
				}
			case <-feedPruningTicker.C:
				if err := feed.PruneFeed(); err != nil {
					log.Error().Err(err).Msg("feed pruning failed")
					// Continue execution, don't exit the goroutine
				}
			}
		}
	}()

	// Set up HTTP server to serve the feed
	http.HandleFunc("/feed.xml", feed.FeedHandler)

	// Start the server
	serverAddr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting server at http://localhost%s", serverAddr)
	log.Fatal().Err(http.ListenAndServe(serverAddr, nil)).Send()
}
