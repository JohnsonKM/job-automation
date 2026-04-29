# Job Automation System

A comprehensive job scraping, processing, and management system built with Python, FastAPI, PostgreSQL, and Redis. This project automates the collection, filtering, and presentation of job listings from various sources.

## Features

- **Automated Job Scraping**: Continuously scrapes job listings from multiple sources
- **Intelligent Filtering**: Uses AI-powered scoring to filter and rank job opportunities
- **Web Dashboard**: FastAPI-based web interface for viewing and managing jobs
- **Scalable Architecture**: Microservices design with Docker containerization
- **Real-time Processing**: Redis-based queuing system for efficient job processing
- **Persistent Storage**: PostgreSQL database for reliable job data storage
- **Notification System**: Automated alerts for new job matches
- **Application Automation**: Automated job application submission

## Architecture

The system consists of several microservices:

- **Scraper**: Collects job listings from various sources and queues them for processing
- **Worker**: Processes queued jobs, applies filtering and scoring algorithms
- **Dashboard**: Web interface for viewing and managing job listings
- **Notifier**: Sends notifications about new job opportunities
- **Applier**: Automates job application submissions
- **Database**: PostgreSQL for persistent job storage
- **Queue**: Redis for message queuing between services

## Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- Git

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd job-automation
   ```

2. **Set up environment variables:**
   Create a `.env` file in the root directory:
   ```env
   POSTGRES_DB=jobautomation
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=your_password
   REDIS_URL=redis://redis:6379
   ```

3. **Start the services:**
   ```bash
   docker-compose up --build
   ```

4. **Access the dashboard:**
   Open http://localhost:8001 in your browser

## Usage

### Running Locally

1. **Activate virtual environment:**
   ```bash
   source .venv/bin/activate
   ```

2. **Start the dashboard:**
   ```bash
   jobautomation
   ```

### Docker Deployment

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### API Endpoints

The dashboard provides the following API endpoints:

- `GET /jobs` - Retrieve recent job listings
- `GET /jobs/{id}` - Get specific job details
- `POST /jobs/apply/{id}` - Apply to a job
- `GET /stats` - System statistics and metrics

## Development

### Project Structure

```
job-automation/
├── db/
│   └── init.sql              # Database schema
├── services/
│   ├── applier/
│   │   ├── apply.py          # Job application logic
│   │   └── Dockerfile
│   ├── dashboard/
│   │   ├── app.py            # FastAPI application
│   │   └── Dockerfile
│   ├── notifier/
│   │   ├── notify.py         # Notification service
│   │   └── Dockerfile
│   ├── scraper/
│   │   ├── scraper.py        # Job scraping logic
│   │   ├── sources.py        # Job source configurations
│   │   └── Dockerfile
│   └── worker/
│       ├── worker.py         # Job processing logic
│       ├── filter.py         # Job filtering algorithms
│       ├── scorer.py         # Job scoring algorithms
│       ├── llm.py            # AI/LLM integration
│       └── Dockerfile
├── shared/
│   ├── db.py                 # Database connection utilities
│   └── __init__.py
├── tests/
│   └── chaos_pipeline.py     # Testing utilities
├── docker-compose.yml        # Service orchestration
├── .env                      # Environment variables
├── .gitignore               # Git ignore rules
└── jobautomation            # Launcher script
```

### Adding New Job Sources

1. Add source configuration in `services/scraper/sources.py`
2. Implement scraping logic in `services/scraper/scraper.py`
3. Update worker filters if needed

### Customizing Scoring

Modify `services/worker/scorer.py` to implement custom job scoring algorithms.

## Testing

Run the test suite:

```bash
python -m pytest tests/
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes and add tests
4. Run tests: `python -m pytest`
5. Commit your changes: `git commit -am 'Add feature'`
6. Push to the branch: `git push origin feature-name`
7. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions or issues, please open an issue on the GitHub repository.