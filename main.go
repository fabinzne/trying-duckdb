package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-co-op/gocron"
	_ "github.com/marcboeker/go-duckdb"
)

// Domain models
type Deployment struct {
	ID          string    `json:"deployment_id" db:"deployment_id"`
	Team        string    `json:"team" db:"team"`
	Service     string    `json:"service" db:"service"`
	Timestamp   time.Time `json:"timestamp" db:"timestamp"`
	Duration    int       `json:"duration_minutes" db:"duration_minutes"`
	Status      string    `json:"status" db:"status"`
	Environment string    `json:"environment" db:"environment"`
	CommitHash  string    `json:"commit_hash" db:"commit_hash"`
}

type TeamMetrics struct {
	Team                  string  `json:"team"`
	TotalDeployments      int     `json:"total_deployments"`
	SuccessfulDeployments int     `json:"successful_deployments"`
	SuccessRate           float64 `json:"successful_rate_pct"`
	AvgDurationMinutes    float64 `json:"avg_duration_minutes"`
	DeploymentFrequency   float64 `json:"deployments_per_day"`
}

type DailyMetrics struct {
	Date        string  `json:"date"`
	Team        string  `json:"team"`
	Deployments int     `json:"deployments"`
	Successful  int     `json:"successful"`
	AvgDuration float64 `json:"avg_duration"`
}

// Services
type MetricsService struct {
	db *sql.DB
}

func NewMetricsService() (*MetricsService, error) {
	db, err := sql.Open("duckdb", "metrics.db")
	if err != nil {
		return nil, fmt.Errorf("Failed to open DuckDB: %v", err)
	}

	service := &MetricsService{db: db}

	if err := service.initializeSchema(); err != nil {
		return nil, fmt.Errorf("Failed to initialize schema: %v", err)
	}

	return service, nil
}

func (ms *MetricsService) initializeSchema() error {
	schemas := []string{
		`CREATE TABLE IF NOT EXISTS deployments (
			deployment_id VARCHAR PRIMARY KEY,
			team VARCHAR NOT NULL,
			service VARCHAR NOT NULL,
			timestamp TIMESTAMP NOT NULL,
			duration_minutes INTEGER NOT NULL,
			status VARCHAR NOT NULL,
			environment VARCHAR NOT NULL,
			commit_hash VARCHAR NOT NULL
		)`,

		`CREATE TABLE IF NOT EXISTS incidents (
			incident_id VARCHAR PRIMARY KEY,
			team VARCHAR NOT NULL,
			service VARCHAR NOT NULL,
			start_time TIMESTAMP NOT NULL,
			end_time TIMESTAMP NOT NULL,
			severity VARCHAR NOT NULL,
			resolved_by VARCHAR,
			root_cause VARCHAR
		)`,

		`CREATE TABLE IF NOT EXISTS pull_requests (
			pr_id VARCHAR PRIMARY KEY,
			team VARCHAR NOT NULL,
			author VARCHAR NOT NULL,
			created_at TIMESTAMP NOT NULL,
			merged_at TIMESTAMP,
			lines_added INTEGER,
			lines_deletes INTEGER,
			review_time_hours FLOAT,
			status VARCHAR NOT NULL
		)`,
	}

	for _, schema := range schemas {
		if _, err := ms.db.Exec(schema); err != nil {
			return fmt.Errorf("Failed to create schema: %v", err)
		}
	}

	return nil
}

func (ms *MetricsService) LoadSampleData() error {
	queries := []string{
		"DELETE FROM deployments",
		"DELETE FROM incidents",
		"DELETE FROM pull_requests",

		`INSERT INTO deployments
		 SELECT * FROM read_csv_auto('example-data/deployments.csv', header=true)`,

		`INSERT INTO incidents
		 SELECT * FROM read_csv_auto('example-data/incidents.csv', header=true)`,

		`INSERT INTO pull_requests
		 SELECT * FROM read_csv_auto('example-data/pull_requests.csv', header=true)`,
	}

	for _, query := range queries {
		if _, err := ms.db.Exec(query); err != nil {
			log.Printf("Warning: Dailed to execute query: %s, error: %v", query, err)
			return err
		}
	}

	log.Println("Sample data loaded successfully!")
	return nil
}

// Business Logic
func (ms *MetricsService) GetTeamMetrics() ([]TeamMetrics, error) {
	query := `
       SELECT 
           team,
           COUNT(*) as total_deployments,
           SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_deployments,
           ROUND(100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate,
           ROUND(AVG(CASE WHEN status = 'success' THEN duration_minutes END), 2) as avg_duration,
           ROUND(COUNT(*) * 1.0 / 7, 2) as deployments_per_day
       FROM deployments 
       GROUP BY team
       ORDER BY success_rate DESC`

	rows, err := ms.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %v", err)
	}
	defer rows.Close()

	var metrics []TeamMetrics
	for rows.Next() {
		var tm TeamMetrics
		err := rows.Scan(
			&tm.Team,
			&tm.TotalDeployments,
			&tm.SuccessfulDeployments,
			&tm.SuccessRate,
			&tm.AvgDurationMinutes,
			&tm.DeploymentFrequency,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %v", err)
		}
		metrics = append(metrics, tm)
	}

	return metrics, nil
}

func (ms *MetricsService) GetDailyMetrics(team string, days int) ([]DailyMetrics, error) {
	query := `
       SELECT 
           DATE_TRUNC('day', timestamp) as date,
           team,
           COUNT(*) as deployments,
           SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
           ROUND(AVG(duration_minutes), 2) as avg_duration
       FROM deployments 
       WHERE ($1 IS NULL OR team = $1)
       AND timestamp >= (CURRENT_DATE - INTERVAL '$2 days')
       GROUP BY DATE_TRUNC('day', timestamp), team
       ORDER BY date DESC, team`

	var teamParam interface{} = nil
	if team != "" {
		teamParam = team
	}

	// Use string interpolation for the interval since DuckDB doesn't accept parameters there
	finalQuery := strings.Replace(query, "$2", strconv.Itoa(days), 1)

	rows, err := ms.db.Query(finalQuery, teamParam)
	if err != nil {
		return nil, fmt.Errorf("query failed: %v", err)
	}
	defer rows.Close()

	var metrics []DailyMetrics
	for rows.Next() {
		var dm DailyMetrics
		var dateTime time.Time

		err := rows.Scan(&dateTime, &dm.Team, &dm.Deployments, &dm.Successful, &dm.AvgDuration)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %v", err)
		}

		dm.Date = dateTime.Format("2006-01-02")
		metrics = append(metrics, dm)
	}

	return metrics, nil
}

func (ms *MetricsService) aggregateMetrics() error {
	log.Println("Running metrics aggregation...")

	queries := []string{
		`CREATE OR REPLACE TABLE daily_team_summary AS
        SELECT
            DATE_TRUNC('day', timestamp) as date,
            team,
            COUNT(*) as total_deployments,
            COUNT(*) FILTER (WHERE status = 'success') as successful_deployments,
            ROUND(AVG(duration_minutes), 2) as avg_duration_minutes
        FROM deployments
        GROUP BY DATE_TRUNC('day', timestamp), team`,

		`CREATE OR REPLACE TABLE team_rankings AS
        SELECT
            team,
            RANK() OVER (ORDER BY COUNT(*) FILTER (WHERE status = 'success') * 100.0 / COUNT(*) DESC) as success_rank,
            RANK() OVER (ORDER BY COUNT(*) DESC) as velocity_rank
        FROM deployments
        GROUP BY team`,
	}

	for _, query := range queries {
		if _, err := ms.db.Exec(query); err != nil {
			return fmt.Errorf("aggregation query failed: %v", err)
		}
	}

	log.Println("Metrics aggregation completed")
	return nil
}

// HTTP Handlers
type Handler struct {
	service *MetricsService
}

func (h *Handler) getTeamMetrics(c *gin.Context) {
	metrics, err := h.service.GetTeamMetrics()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, metrics)
}

func (h *Handler) getDailyMetrics(c *gin.Context) {
	team := c.Query("team")
	daysStr := c.DefaultQuery("days", "30")

	days, err := strconv.Atoi(daysStr)
	if err != nil {
		days = 30
	}

	metrics, err := h.service.GetDailyMetrics(team, days)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, metrics)
}

func main() {
	service, err := NewMetricsService()
	if err != nil {
		log.Fatal("Failed to initialize service:", err)
	}
	defer service.db.Close()

	if err := service.LoadSampleData(); err != nil {
		log.Printf("Warning: Failed to load sample data: %v", err)
	}

	scheduler := gocron.NewScheduler(time.UTC)
	scheduler.Every(1).Hour().Do(func() {
		if err := service.aggregateMetrics(); err != nil {
			log.Printf("Aggregation failed: %v", err)
		}
	})
	scheduler.StartAsync()

	handler := &Handler{service: service}
	router := gin.Default()

	api := router.Group("/api/v1")
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	{
		api.GET("/metrics/teams", handler.getTeamMetrics)
		api.GET("/metrics/daily", handler.getDailyMetrics)
	}

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy", "database": "duckdb"})
	})

	log.Println("Starting server on :8080")
	if err := router.Run(":8080"); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}
