# BBC Top Stories Data Pipeline with Airflow

## Description

This application is developed to demonstrate and practice using Apache Airflow for scheduled data processing jobs. It features a scheduled task that retrieves the top ten stories from the BBC main page. Based on these stories, the application defines topics and further stories, which are then processed and stored in an SQL database. The entire workflow, from data fetching to processing and storing, is managed by a single DAG file in Airflow, showcasing a practical implementation of Airflow tasks for real-world data processing needs.

Built with Python and containerized using Docker, this project provides a streamlined setup and execution environment, making it easy to deploy and run across different systems without compatibility issues.

## Technology Stack

- **Apache Airflow**: Used for scheduling and orchestrating the data processing workflow.
- **Python**: The primary programming language for writing the application logic.
- **Docker**: Utilized for containerization, ensuring the application and its dependencies are easily managed and deployed.
- **SQL Database**: Stores the processed data, with the schema designed to accommodate stories and their associated metadata.

## Setup and Installation

Before proceeding, ensure Docker and Docker Compose are installed on your system. This application relies on Docker for deployment and Airflow for workflow management.

### Clone the Repository
