#!/bin/bash

# Script to create the medical-telegram-warehouse project structure
# Run from the parent directory where you want the project created

PROJECT_NAME="medical-telegram-warehouse"
ROOT="$PROJECT_NAME"

echo "📁 Creating project structure for '$PROJECT_NAME'..."

# Create root directory
mkdir -p "$ROOT"

# Root files
touch "$ROOT/.env"
touch "$ROOT/.gitignore"
touch "$ROOT/docker-compose.yml"
touch "$ROOT/Dockerfile"
touch "$ROOT/README.md"
touch "$ROOT/requirements.txt"

# .vscode directory
mkdir -p "$ROOT/.vscode"
touch "$ROOT/.vscode/settings.json"

# data/medical_warehouse (dbt-style project)
mkdir -p "$ROOT/data/medical_warehouse"
cd "$ROOT/data/medical_warehouse"

# Standard dbt directories
mkdir -p models/seeds tests macros docs
touch dbt_project.yml
touch models/example.sql  # optional placeholder
cd ../../..

# notebooks
mkdir -p "$ROOT/notebooks"
touch "$ROOT/notebooks/exploration.ipynb"
touch "$ROOT/notebooks/model_training.ipynb"

# src/api
mkdir -p "$ROOT/src/api"
touch "$ROOT/src/api/main.py"
touch "$ROOT/src/api/requirements.txt"

# src/pipeline
mkdir -p "$ROOT/src/pipeline"
touch "$ROOT/src/pipeline/__init__.py"
touch "$ROOT/src/pipeline/extract.py"
touch "$ROOT/src/pipeline/transform.py"
touch "$ROOT/src/pipeline/load.py"

# tests
mkdir -p "$ROOT/tests"
touch "$ROOT/tests/test_pipeline.py"
touch "$ROOT/tests/test_api.py"

# logs
mkdir -p "$ROOT/logs"

echo "✅ Project structure created successfully!"
echo "📁 Location: $(pwd)/$PROJECT_NAME"