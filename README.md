# Personality Prediction Project with MLOps Implementation

This project combines Data Science with MLOps principles to create an automated, scalable framework for personality prediction. By leveraging modern operations practices, we ensure that the model development and deployment processes are efficient, robust, and repeatable.

## Overview

The Personality Prediction Project employs machine learning algorithms to classify personality types (Introvert vs. Extrovert). This initiative extends beyond traditional modeling; it implements a comprehensive end-to-end MLOps pipeline. The project highlights the importance of automating the ML lifecycle, focusing on streamlining development, facilitating smooth deployment, and ensuring long-term maintainability.


## 📁 Project Setup and Structure
#### Step 1: Project Template
    * Start by executing the template.py file to create the initial project template, which includes the required folder structure and placeholder files.
#### Step 2: Package Management
    * Write the setup for importing local packages in setup.py and pyproject.toml files.

#### Step 3: Virtual Environment and Dependencies
    * Create a virtual environment and install required dependencies from requirements.txt
      python -m venv (name of venv)
    * Using the Activate.ps1 activate the virtual environment
       pip install - requirements.txt

    * verfiy the local packages by running:
       pip list

## 📊 MongoDB Setup and Data Management

#### Step 4: MongoDB Atlas Configuration
    1. Sign up for MongoDB Atlas and create a new project.
    2. Set up a free M0 cluster, configure the username and password, and allow access from any IP address (0.0.0.0/0).
    3. Retrieve the MongoDB connection string for Python and save it (replace <password> with your password).
#### Step 5: Pushing Data to MongoDB
    1. Create a folder named notebook, add the dataset, and create a notebook file mongoDB_demo.ipynb.
    2. Use the notebook to push data to the MongoDB database.
    3. Verify the data in MongoDB Atlas under Database > Data Explore.

## 📥 Data Ingestion
#### Step 8: Data Ingestion Pipeline
    * Define MongoDB connection functions in configuration.mongo_db_connections.py.
    * Develop data ingestion components in the data_access and components.data_ingestion.py files to fetch and transform data.
    * Update entity/config_entity.py and entity/artifact_entity.py with relevant ingestion configurations.
    * Run demo.py after setting up MongoDB connection as an environment variable.

### Setting Envrionment Variables
* Set MongoDB URL:
    ##### For Bash
        export MONGODB_URL="mongodb+srv://<username>:<password>...."
    ##### For Powershell
        $env:MONGODB_URL = "mongodb+srv://<username>:<password>...."

* Note: On Windows, you can also set environment variables through the system settings.

## 🔍 Data Validation, Transformation & Model Training

#### Step 9: Data Validation
    * Define schema in config.schema.yaml and implement data validation functions in utils.main_utils.py.

#### Step 10: Data Transformation
    * Implement data transformation logic in components.data_transformation.py and create estimator.py in the entity folder.

#### Step 11: Model Training
    * Define and implement model training steps in components.model_trainer.py using code from estimator.py .

## 🌐 AWS Setup for Model Evaluation & Deployment
#### Step 12: AWS Setup
    1. Log in to the AWS console, create an IAM user, and grant AdministratorAccess.

    2. Set AWS credentials as environment variables.
    # For Bash
    export AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"
    export AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY"

    3. Configure S3 Bucket and add access keys in constants.__init__.py.

#### Step 13. Model Evaluation and Pushing to S3
    * Create an S3 bucket named "mlops-project-model-intro-pred" in the us-east-1 region.
    * Develop code to push/pull models to/from the S3 bucket in src.aws_storage and entity/s3_estimator.py.

## 🚀 Model Evaluation, Model Pusher, and Prediction Pipeline
#### Step 14: Model Evaluation & Model Pusher
    * Implement model evaluation and deployment components.
    * Create Prediction Pipeline and set up app.py for API integration.
#### Step 15: Static and Template Directory
    * Add static and template directories for web UI.

## 🔄 CI/CD Setup with Docker, GitHub Actions, and AWS
#### Step 16: Docker and GitHub Actions
    * Create Dockerfile and .dockerignore.
    * Set up GitHub Actions with AWS authentication by creating secrets in GitHub for:
        * AWS_ACCESS_KEY_ID
        * AWS_SECRET_ACCESS_KEY
        * AWS_DEFAULT_REGION
        * ECR_REPO
#### Step 17: AWS EC2 and ECR
    * Set up an EC2 instance for deployment.
    * Install Docker on the EC2 machine.
    * Connect EC2 as a self-hosted runner on GitHub.

#### Step 18: Final Steps
    * Open the 5080 port on the EC2 instance.
    * Access the deployed app by visiting http://<public_ip>:5000.

![alt text](<Screenshot 2026-01-21 164252.png>)