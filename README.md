# Personal Health Optimizer

**Device-Agnostic Health Data Warehouse**

The Personal Health Optimizer is a centralized platform designed to solve the problem of siloed health data. By ingesting and normalizing exports from distinct providers—including Garmin, Oura, and MyFitnessPal—this system enables users to discover cross-platform correlations and optimize their well-being through data-driven insights.

## Architecture

The system operates on a robust 5-tier architecture designed for scalability and asynchronous processing:

1.  **Frontend**: A **Next.js** application providing a responsive, server-rendered dashboard for data visualization.
2.  **API Layer**: A **Django REST Framework** service handling authentication, data retrieval, and upload requests.
3.  **Database**: **PostgreSQL** serving as the persistent relational store for normalized health metrics.
4.  **Message Broker**: **Redis** managing task queues for asynchronous data processing.
5.  **Background Workers**: **Celery** workers responsible for executing long-running parsing and normalization tasks without blocking the main API thread.

## Key Challenge: The Normalization Engine

A core component of this project is the **Normalization Engine**. Health data exports vary wildly in format (CSV, JSON, XML) and structure across providers. The engine's responsibility is to:

*   **Ingest**: Accept raw files from Garmin, Oura, and MyFitnessPal.
*   **Parse**: Handle inconsistent delimiters, nested JSON structures, and proprietary XML schemas.
*   **Normalize**: Transform disparate metrics (e.g., "Deep Sleep" vs. "Stage 4 Sleep") into a single, standardized schema optimized for time-series analysis.

## DevOps & Local Development

We utilize **Docker Compose** to containerize the entire stack, ensuring environment parity between local development and production.

*   **Local Dev**: `docker-compose up` spins up the frontend, backend, database, redis, and worker containers.
*   **CI/CD**: **GitHub Actions** pipelines run linting, unit tests, and build checks on every pull request to maintain code quality.

## Roadmap

### Phase 1: File Ingestion & Foundation
*   Implement Django + Next.js scaffolding.
*   Develop the Normalization Engine for Garmin (CSV) and MyFitnessPal (CSV) exports.
*   Establish PostgreSQL schema for common metrics (Heart Rate, Sleep Duration, Calories).

### Phase 2: Statistical Analysis
*   Integrate **Pandas** for server-side data manipulation.
*   Develop correlation algorithms (e.g., "How does Oura sleep score impact Garmin VO2 Max trends?").
*   Visualize trends using frontend charting libraries.

### Phase 3: Social & Protocols
*   Implement privacy-first sharing capabilities.
*   Allow users to publish "Health Protocols" (routine sets) and compare anonymized outcomes.

