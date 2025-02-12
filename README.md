# Solution File Processing

A comprehensive Python package for processing, analyzing, and visualizing PLEXOS solution files. This package provides tools to handle large-scale power system modeling outputs, with features for data extraction, transformation, caching, and visualization.

## Features

- Unarchive and process PLEXOS solution files (.zip to .h5 conversion)
- Efficient data handling using dask for large datasets
- Sophisticated caching system for processed objects and variables
- Extensive visualization capabilities including:
  - Generation stacks
  - Load duration curves
  - Regional analyses
  - CO2 emissions
  - VRE (Variable Renewable Energy) integration metrics
  - System flexibility metrics
- Multiple output formats:
  - Excel-based visualizations
  - CSV data exports
  - Summary statistics
  - Time series analysis

## Installation

### Prerequisites

1. Julia needs to be installed and accessible:
```bash
# Install Julia from software center or submit IT ticket
# Verify installation with:
where julia
```

2. Set up Julia proxies if required:
```julia
# Add to ~/.julia/config/startup.jl:
ENV["HTTP_PROXY"] = "http://proxy.example.org:8080"
ENV["HTTPS_PROXY"] = "http://proxy.example.org:8080"
```

3. Install required Julia packages:
```julia
]
registry add https://github.com/JuliaRegistries/General
add PyCall
registry add https://github.com/NREL/JuliaRegistry.git
add H5PLEXOS
add ZipFile
```

### Package Installation

1. Clone the repository:
```bash
git clone git@gitlab.iea.org:iea/ems/rise/solution-file-processing.git
```

2. Create and activate conda environment:
```bash
conda env create -f environment.yml
conda activate solution-file-processing
```

3. Install additional dependencies:
```bash
python -m pip install --proxy http://proxy.example.org:8080 julia
python -m pip install --proxy http://proxy.example.org:8080 https://github.com/NREL/h5plexos/archive/master.zip
pip install -e .
```

## Usage

Basic usage pattern:

```python
import solution_file_processing as sfp

# Initialize configuration
config = sfp.SolutionFilesConfig('config.toml')

# First time setup
config.install_dependencies()  # Only needed once
config.convert_solution_files_to_h5()  # Convert solution files

# Create outputs
sfp.plots.create_plot_2_summary(config)
sfp.summary.create_output_1(config)
sfp.timeseries.create_output_1(config)
```

### Available Output Functions

#### Plots
- `create_plot_1a`: Generation stacks for days of interest
- `create_plot_2_summary`: Annual summary plots
- `create_plot_3`: Annual generation by tech/region
- `create_plot_4_costs`: Cost savings analysis
- `create_plot_5_undispatched_tech`: Undispatched capacity analysis
- `create_plot_6_ldc_and_line_plots`: Load duration curves
- `create_plot_7_co2_savings`: CO2 emissions analysis
- `create_plot_8_services`: System services analysis
- `create_plot_9_av_cap`: Available capacity analysis
- `create_plot_10`: VRE generation and capacity factor analysis

#### Summary Outputs
- `create_output_1`: Load by region
- `create_output_2`: Unserved energy analysis
- `create_output_3`: Generation by technology and region
- `create_output_4`: RE/VRE shares
- `create_output_5`: Unit starts
- `create_output_6`: Generation maxima
- `create_output_7`: Transmission losses
- `create_output_8`: VRE capacity and availability
- `create_output_9`: VRE curtailment
- `create_output_10`: Line capacity and flows
- `create_output_11`: Generation capacity
- `create_output_12`: Capacity factors
- `create_output_13`: CO2 emissions

#### Time Series Outputs
- Generation profiles
- Load profiles
- VRE integration metrics
- System flexibility indicators
- Reserve provision
- Price analysis

## Project Structure

```
solution-file-processing/
├── solution_file_processing/     # Main package
│   ├── objects.py               # Data object handling
│   ├── variables.py             # Variable processing & caching
│   ├── plots.py                 # Visualization functions
│   ├── summary.py               # Summary statistics
│   ├── timeseries.py           # Time series analysis
│   ├── constants.py            # System constants and mappings
│   ├── plot_dataframes.py      # Plot-ready data structures
│   ├── solution_files.py       # Core solution file processing
│   └── utils/                  # Utility functions
│       ├── logger.py           # Custom logging
│       ├── utils.py            # General utilities
│       └── write_excel.py      # Excel output handling
├── config_files/               # Configuration templates
│   ├── china/                  # China-specific configs
│   ├── thailand/              # Thailand-specific configs
│   ├── ukraine/               # Ukraine-specific configs
│   └── vanilla_dev/           # Development configs
├── docs/                       # Documentation
├── templates/                  # Excel templates
├── logs/                      # Log files
├── legacy/                    # Legacy code
└── *_run_script.py           # Run scripts for different regions
```

## Configuration

The package uses TOML configuration files to specify:
- Model directory structure
- Solution file locations
- Regional aggregations
- Output preferences
- Caching behavior

The configuration file (TOML format) contains several key sections:

### [path]
```toml
[path]
model_dir = "path/to/model"                    # Main model directory
lt_output_path = "path/to/input/ExpUnits"      # Long-term output path
soln_idx_path = "path/to/generator_parameters.xlsx" # Solution index file
```

### [model]
```toml
[model]
soln_choice = "scenario_name"                  # Solution scenario to process
```

### [settings]
```toml
[settings]
geo_cols = ["Region", "Subregion"]            # Geographic aggregation levels
reg_ts = true                                 # Enable regional time series
```

### [plots]
```toml
[plots]
# Load-related plots
load_plots = [
    "load_by_reg",
    "pk_load_by_reg",
    "use_by_reg",
    "pk_netload_by_reg",
    "gen_cycling_pk",
    "gen_cycling_pc_pk"
]

# Generation-related plots
gen_plots = [
    "gen_cap_by_reg",
    "gen_cap_by_tech",
    "gen_by_tech",
    "gen_by_reg",
    "curtailment_rate",
    "vre_by_reg_av",
    "cf_tech_trans"
]

# Other analysis plots
other_plots = [
    "co2_by_tech",
    "co2_by_reg",
    "co2_intensity_reg",
    "op_costs_by_prop"
]

# Quick overview plots
quick_plots = [
    "gen_cap_by_reg",
    "gen_by_tech",
    "cf_tech",
    "curtailment_rate"
]
```

### [run]
```toml
[run]
working_dir = "path/to/working/dir"            # Working directory
log_file_path = "path/to/logs/file.log"        # Log file location
log_timestamp = true                           # Add timestamps to logs
variables_cache = true                         # Enable caching
catch_errors = true                           # Error handling mode
```

### [testing]
```toml
[testing]
# baseline_output_dir = "path/to/baseline"     # Baseline for comparison
# similar_output_dirs = false                  # Similar outputs check
```

## Caching System

The package implements a sophisticated caching system to handle large datasets efficiently:
- Drive cache: Stores processed data in parquet format
- Memory cache: Maintains frequently accessed data in memory
- Automatic cache invalidation when source data changes

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

When adding new features:
- Follow existing code structure
- Add appropriate documentation
- Include tests if applicable
- Update this README if necessary

## License

Proprietary - Internal use only

## Acknowledgments

This package uses several open-source libraries:
- dask for large data processing
- pandas for data analysis
- h5plexos for solution file handling
- matplotlib for visualization
- xlsxwriter for Excel output

## Support

For issues and questions, please contact the development team or submit an issue in the repository.
