# Integrated Library System

Short Description:This project delivers a fully integrated library solution end-to-end, combining open-source platforms, data pipelines, BI, and intelligent recommendations:

Koha (OPAC & circulation) with custom suggestion plugins and UI enhancements

DSpace (digital repository)

VuFind + Solr (discovery layer)

Apache Airflow (automated ETL pipelines for Koha & DSpace)

Data Warehouse implemented with a Galaxy schema

SSAS cube (MOLAP/Tabular) and Power BI dashboards

Recommender Service: SBERT embeddings + KNN model

📑 Table of Contents

Overview

Architecture & Technology Stack

Manual Installation & Deployment

Features & Usage

Example Commands

Repository Structure

Contributing

License

Overview

This system integrates multiple library services into a unified platform with:

Koha extended with recommendation plugins and end-to-end UI customization

DSpace for digital asset management

VuFind + Solr as the search & discovery interface

Automated data extraction pipelines that ingest and normalize data into Koha and DSpace

ETL workflows managed by Apache Airflow loading into a Galaxy-designed Data Warehouse

OLAP analytics via SSAS cubes and interactive Power BI reports

Machine learning recommendations served by a Python SBERT + KNN microservice

Architecture & Technology Stack



Component

Technology

Library Management

Koha (Perl) + custom recommendation plugins + UI tweaks

Digital Repository

DSpace (Java + Angular)

Search & Discovery

VuFind (PHP) + Solr

Data Extraction & Transformation

Apache Airflow (Python)

Data Warehouse

PostgreSQL / SQL Server (Galaxy schema)

OLAP & BI

SSAS cube (MOLAP/Tabular) + Power BI

Recommendation Engine

Python (SBERT embeddings + scikit-learn KNN)

Infrastructure as Code (optional)

Terraform

Continuous Integration & Delivery

GitHub Actions (lint, test, build)

Manual Installation & Deployment

Note: This guide assumes you are not using Docker and will install each component on Linux or WSL2.

Prerequisites

Linux (Ubuntu 20.04+) or WSL2 on Windows

Python 3.8+ and pip

Java 11+

PostgreSQL or SQL Server

SQL Server Analysis Services (if using SSAS)

Node.js & npm (for DSpace)

Features & Usage

Recommendation Plugin & UI: Users receive real-time book recommendations directly in the Koha interface.

Automated Extraction: Metadata and items from external sources are ingested into Koha and DSpace automatically via Airflow.

Full Data Pipeline: ETL, DWH, SSAS cube, and BI dashboards provide end-to-end analytics.

Customizable UI: Front-end enhancements applied to Koha and VuFind for a modern user experience.
