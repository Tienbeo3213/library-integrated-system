# Integrated Library System

**Short Description:**  
This project delivers a fully integrated library solution end-to-end, combining open-source platforms, data pipelines, BI, and intelligent recommendations:

- **Koha** (OPAC & circulation) with custom suggestion plugins and UI enhancements
- **DSpace** (digital repository)
- **VuFind + Solr** (discovery layer)
- **Apache Airflow** (automated ETL pipelines for Koha & DSpace)
- **Data Warehouse** implemented with a Galaxy schema
- **SSAS** cube (MOLAP/Tabular) and **Power BI** dashboards
- **Recommender Service:** SBERT embeddings + KNN model

---

## 📑 Table of Contents
- [Overview](#overview)
- [Architecture & Technology Stack](#architecture--technology-stack)
- [Manual Installation & Deployment](#manual-installation--deployment)
- [Features & Usage](#features--usage)
- [Example Commands](#example-commands)
- [Repository Structure](#repository-structure)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

This system integrates multiple library services into a unified platform with:
1. **Koha** extended with recommendation plugins and end-to-end UI customization
2. **DSpace** for digital asset management
3. **VuFind + Solr** as the search & discovery interface
4. **Automated data extraction pipelines** that ingest and normalize data into Koha and DSpace
5. **ETL workflows** managed by Apache Airflow loading into a Galaxy-designed Data Warehouse
6. **OLAP analytics** via SSAS cubes and interactive Power BI reports
7. **Machine learning recommendations** served by a Python SBERT + KNN microservice

---

## Architecture & Technology Stack
![image](https://github.com/user-attachments/assets/6f5163c6-81cf-4ad7-8b63-e3593d25a878)

| Component                        | Technology                                    |
|----------------------------------|-----------------------------------------------|
| Library Management               | Koha (Perl) + custom recommendation plugins + UI tweaks |
| Digital Repository               | DSpace (Java + Angular)                       |
| Search & Discovery               | VuFind (PHP) + Solr                           |
| Data Extraction & Transformation | Apache Airflow (Python)                       |
| Data Warehouse                   | PostgreSQL / SQL Server (Galaxy schema)       |
| OLAP & BI                        | SSAS cube (MOLAP/Tabular) + Power BI           |
| Recommendation Engine            | Python (SBERT embeddings + scikit-learn KNN)  |
---

## Manual Installation & Deployment

> **Note:** This guide assumes you are not using Docker and will install each component on Linux or WSL2.

### Prerequisites
- Linux (Ubuntu 20.04+) or WSL2 on Windows
- Python 3.8+ and pip
- Java 11+
- PostgreSQL or SQL Server or MySQL
- SQL Server Analysis Services (if using SSAS)
- Node.js & npm (for DSpace)

