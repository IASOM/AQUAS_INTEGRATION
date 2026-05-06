# PREDAP Data Pipeline Project

A comprehensive data pipeline system for processing demand and diagnosis data from SQL Server/Azure Synapse databases.

## Project Overview

This project contains two main data processing pipelines:

- **Demand Pipeline**: Processes visit data to generate demand metrics aggregated by region (CAT, RS, UP)
- **Diagnosis Pipeline**: Processes and filters visit data by diagnosis codes for detailed analysis

Both pipelines follow an incremental processing model, loading only new data since the last run and maintaining state for resumability.

## Project Structure

```
GIT/
├── config/                          # Centralized configuration
│   └── config.py                   # Pipeline configuration management
├── data/                           # Data directories (created at runtime)
│   ├── demand_pipeline/
│   │   ├── state/                 # Pipeline state files
│   │   ├── incremental/           # Incremental output files
│   │   └── finals/                # Final aggregated outputs
│   └── diagnosis_pipeline/
│       ├── state/
│       ├── selected_codes/
│       ├── incremental/
│       └── finals/
├── pipelines/                      # Pipeline implementations
│   ├── shared/                     # Shared utilities and database code
│   │   ├── __init__.py
│   │   ├── db.py                  # Database connection functions
│   │   ├── utils.py               # Data processing utilities
│   │   └── logging_config.py      # Logging setup
│   ├── demand/                     # Demand pipeline modules
│   │   ├── __init__.py
│   │   ├── config.py              # Demand-specific config
│   │   ├── main.py                # Pipeline entry point
│   │   ├── incremental.py         # Incremental processing logic
│   │   ├── aggregation.py         # Final aggregation logic
│   │   ├── transformations.py     # Data transformations
│   │   └── utils.py               # Demand-specific utilities
│   └── diagnosis/                  # Diagnosis pipeline modules
│       ├── __init__.py
│       ├── config.py
│       ├── diagnosis_main.py       # Pipeline entry point
│       ├── incremental.py
│       ├── aggregation.py
│       ├── transformations.py
│       └── utils.py
├── src/                            # [DEPRECATED] Old structure - can be removed
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment variables template
├── README.md                       # This file
└── run_pipeline.py                 # Main entry point for running pipelines
```

## Installation

### Prerequisites

- Python 3.9+
- ODBC Driver 18 for SQL Server
- Access to SQL Server/Azure Synapse database

### Setup Steps

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd GIT
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials and paths
   ```

5. **Set up data directories**:
   ```bash
   mkdir -p data/demand_pipeline/{state,incremental,finals}
   mkdir -p data/diagnosis_pipeline/{state,incremental,finals,selected_codes}
   ```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Database Configuration
DB_SERVER=your-server.sql.azuresynapse.net
DB_DATABASE=your_database
AUTH_MODE=ActiveDirectoryIntegrated

# Base Paths
BASE_DIR=/path/to/project
UP_RS_FILE=/path/to/UP_per_RS.xlsx

# Logging
LOG_LEVEL=INFO
```

### Python Configuration

See [config/config.py](config/config.py) for detailed configuration options. You can:

- Override environment variables with system variables
- Modify date ranges for processing
- Configure input/output file paths

## Running the Pipelines

### Run Both Pipelines

```bash
python run_pipeline.py --both
```

### Run Demand Pipeline Only

```bash
python run_pipeline.py --demand
```

### Run Diagnosis Pipeline Only

```bash
python run_pipeline.py --diagnosis
```

### View Help

```bash
python run_pipeline.py --help
```

## Pipeline Workflow

Both pipelines follow this workflow:

1. **Check State**: Load the last processed date from state file
2. **Query Database**: Fetch new data since last run
3. **Transform Data**: Apply business logic transformations
4. **Incremental Output**: Save new data to incremental files
5. **Aggregation**: Combine incremental and final files
6. **Update State**: Record the last processed date

### State Management

Pipeline state is stored in `data/{pipeline}_pipeline/state/state.json`:

```json
{
  "last_loaded_date": "2024-05-06T00:00:00",
  "updated_at": "2024-05-06T10:30:45.123456"
}
```

This allows pipelines to resume from where they left off without reprocessing data.

## Shared Utilities

Common functions are available in [pipelines/shared/](pipelines/shared/):

### Database Functions ([pipelines/shared/db.py](pipelines/shared/db.py))
- `get_connection()` - Establish database connection
- `get_min_max_date()` - Get data range from table
- `get_year_ranges()` - Generate year-based date ranges
- `get_data_for_year()` - Query data for specific year/date range

### Data Processing ([pipelines/shared/utils.py](pipelines/shared/utils.py))
- `load_state()` / `save_state()` - Manage pipeline state
- `load_output_matrix()` / `save_output_matrix()` - Handle CSV files with datetime index
- `ensure_daily_range()` - Ensure continuous daily time series
- `load_last_date_from_output()` - Get max date from output file

## Development Guidelines

### Adding New Features

1. Add shared utilities to `pipelines/shared/`
2. Pipeline-specific code goes in `pipelines/{demand|diagnosis}/`
3. Update configuration in `config/config.py` if needed
4. Update `requirements.txt` for new dependencies

### Code Style

- Follow PEP 8 conventions
- Use type hints for function parameters and returns
- Add docstrings to functions and modules
- Use descriptive variable names

### Testing

Create tests in a `tests/` directory (to be added):

```bash
pytest tests/
```

## Troubleshooting

### Database Connection Issues

- Verify `DB_SERVER` and `DB_DATABASE` are correct
- Check ODBC Driver 18 installation: `odbcconf /s`
- Ensure authentication credentials are valid
- Check network connectivity to database server

### Missing Data Files

- Verify `BASE_DIR` and `UP_RS_FILE` paths are correct
- Ensure data directories exist
- Check file permissions

### State File Issues

- Delete state file to force full reprocessing:
  ```bash
  rm data/demand_pipeline/state/state.json
  rm data/diagnosis_pipeline/state/state.json
  ```
- State will be recreated on next run

### Import Errors

- Verify virtual environment is activated
- Run `pip install -r requirements.txt` again
- Check that `PYTHONPATH` includes project root

## Data Flows

### Demand Pipeline

```
Database (P1038_visites)
    ↓
[Load new visits since last_loaded_date]
    ↓
Transform (region aggregation)
    ↓
Save to incremental CSV files
    ↓
Combine with previous finals
    ↓
Save to final CSV files (demanda_CAT.csv, demanda_RS.csv, demanda_UP.csv)
    ↓
Update state file with last_loaded_date
```

### Diagnosis Pipeline

```
Database (P1038_visites)
    ↓
[Load new visits since last_loaded_date]
    ↓
Filter by diagnosis codes (from selected_codes.csv)
    ↓
Transform (region aggregation)
    ↓
Save to incremental CSV files
    ↓
Combine with previous finals
    ↓
Save to final CSV files (selected_CAT.csv, selected_RS.csv, selected_UP.csv)
    ↓
Update state file with last_loaded_date
```

## Performance Considerations

- **Batch Processing by Year**: Data is processed in yearly batches to manage memory usage
- **Incremental Updates**: Only new data is processed on each run
- **State Tracking**: Prevents reprocessing of historical data

## Maintenance

### Updating Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt --upgrade
pip freeze > requirements.txt
```

### Archiving Old Data

```bash
# Archive incremental files older than 1 year
find data/demand_pipeline/incremental -type f -mtime +365 -move archive/
```

## Support and Issues

For issues or questions:
1. Check the troubleshooting section above
2. Review log files in `data/{pipeline}_pipeline/state/`
3. Verify configuration in `config/config.py`
4. Check database connectivity and permissions

## License

[Add your license here]

## Contributors

- [Your name/team]

## Changelog

### v2.0 (Current)
- Refactored project structure for better organization
- Centralized configuration management
- Separated pipeline modules
- Created shared utilities module
- Added comprehensive documentation

### v1.0 (Legacy - in src/ directory)
- Initial pipeline implementation
