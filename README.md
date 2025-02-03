# Instacart Recommendation System

## Overview
Instacart Recommendation System is an automated data pipeline built on AWS to facilitate efficient data processing, transformation, and machine learning model deployment for personalized recommendations. This project leverages AWS services such as Glue, IAM, SageMaker, S3, Systems Manager (SSM), WAF, and CloudFormation to automate infrastructure provisioning and ETL processes.

## Repository Structure
```
Instamarket-Recommendation-System/
│-- .github/
│   ├── Workflows for GitHub Actions to deploy CloudFormation stacks and frontend
│-- cloudformation/
│   ├── AWS Infrastructure as Code (IaC) written in YAML. (Configurations for Glue, IAM, SageMaker, S3, SSM, WAF, Frontend)
│   ├── main.yaml (Main CloudFormation template to create 5 AWS stacks)
│-- scripts/
│   ├── glue/ (ETL scripts for AWS Glue jobs)
│   ├── lambda/ (AWS Lambda function scripts)
```

## Features
- **Automated Infrastructure Deployment**: Uses AWS CloudFormation to provision necessary resources.
- **ETL Pipeline with AWS Glue**: Processes raw data from various sources and prepares it for analysis.
- **Machine Learning with SageMaker**: Deploys recommendation models for personalized user experience.
- **Serverless Functions with Lambda**: Handles event-driven automation and data transformations.
- **GitHub Actions CI/CD**: Automates deployment of CloudFormation stacks and frontend updates.

## Deployment
1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo/instamarket-recommendation-system.git
   ```
2. Configure AWS credentials for deployment.
3. Deploy CloudFormation stacks:
   ```sh
   cd cloudformation
   aws cloudformation deploy --template-file main.yaml --stack-name instamarket-main
   ```
4. Upload Glue and Lambda scripts to their respective AWS services.

