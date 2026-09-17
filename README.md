# AeroSync 

AeroSync is a real-time, event-driven aviation telemetry tracking dashboard. It ingests flight data, processes it through a decoupled event streaming pipeline, and broadcasts it to a reactive React frontend with a glass-morphism UI.

##  Architecture & Tech Stack

This project is built with a production-grade, containerized microservices architecture:

* **Frontend:** React (Vite), Leaflet.js, WebSockets
* **Backend:** Java 21, Spring Boot (WebFlux)
* **Message Broker:** Apache Kafka (Event-driven ingestion)
* **Primary Database:** PostgreSQL (R2DBC reactive persistence)
* **Write-Through Cache:** Valkey (High-speed active state retrieval)
* **Infrastructure:** Docker & Docker Compose

##  Quick Start

The entire infrastructure is fully Dockerized. You do not need to install Java, Node, Kafka, or PostgreSQL on your local machine to run this project.

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running.
* Git

### Installation & Launch

1. **Clone the repository**
   ```bash
   git clone [https://github.com/bishnu2005/AeroSync.git](https://github.com/bishnu2005/AeroSync.git)
   cd AeroSync
