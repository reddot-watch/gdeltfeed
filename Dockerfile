# Builder stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install necessary build dependencies
RUN apk add --no-cache git gcc musl-dev sqlite-dev

# Copy only dependency files first to leverage Docker caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build with optimizations, CGO enabled for SQLite
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o gdeltfeed .

# Runtime stage
FROM alpine:3.19

# Add non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Install runtime dependencies only
RUN apk add --no-cache ca-certificates sqlite-libs tzdata

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/gdeltfeed /app/

# Create data directory with proper permissions
RUN mkdir -p /app/data && chown -R appuser:appgroup /app/data

# Use volume for database persistence
VOLUME /app/data

# Use non-root user
USER appuser

# Set environment variables
ENV DB_PATH=/app/data/gdelt_news_feed.db \
    PORT=8080 \
    COLLECTION_MINUTES=10 \
    PRUNING_HOURS=24 \
    MAX_DB_CONNECTIONS=10

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -qO- http://localhost:8080/health || exit 1

# Run the application
CMD ["/app/gdeltfeed"]