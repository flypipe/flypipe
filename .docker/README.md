# Docker Environment Structure

This directory contains separate Docker environments for different Flypipe backends.

## Directory Structure

```
.docker/
├── core/               # Pandas-only environment (no Spark/Snowflake)
│   ├── Dockerfile
│   └── docker-compose.yaml
├── spark/              # PySpark environment (includes Spark cluster)
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── start.py        # IPython startup script (Spark-specific)
│   ├── wait-for-it.sh  # Utility script for waiting on services
│   └── logs/           # Spark logs directory
├── snowflake/          # Snowflake Snowpark environment
│   ├── Dockerfile
│   └── docker-compose.yaml
└── __init__.py
```

## Environments

### 1. Core (Pandas)
- **Container**: `flypipe-core`
- **Purpose**: Lightweight environment for core Pandas testing
- **Dependencies**: Python 3.11, Pandas, NumPy, pytest, etc.
- **Use Case**: Fast development and testing of core functionality
- **Size**: ~500MB

### 2. Spark (PySpark)
- **Container**: `flypipe-spark`
- **Purpose**: Full Apache Spark cluster with PySpark
- **Dependencies**: Spark 3.5.7, PySpark, Delta Lake, grpcio
- **Services**:
  - `spark-master`: Spark master node
  - `spark-worker`: Spark worker node
  - `spark-connect`: Spark Connect server
  - `flypipe-spark`: Jupyter/dev environment
- **Use Case**: PySpark development and Spark Connect testing
- **Size**: ~2.5GB

### 3. Snowflake (Snowpark)
- **Container**: `flypipe-snowflake`
- **Purpose**: Snowflake Snowpark development
- **Dependencies**: Python 3.11, snowflake-snowpark-python[localtest]
- **Use Case**: Snowflake Snowpark development and local testing
- **Size**: ~800MB

## Usage

The Makefile automatically selects the appropriate Docker environment based on `RUN_MODE`:

### Default (Pandas - fastest)
```bash
make test f=flypipe/cache/cache_test.py
# Uses: .docker/core/
```

### PySpark Local
```bash
make test RUN_MODE=SPARK f=flypipe/cache/cache_pyspark_test.py
# Uses: .docker/spark/
```

### PySpark Connect
```bash
make test RUN_MODE=SPARK_CONNECT f=flypipe/runner_pyspark_test.py
# Uses: .docker/spark/
```

### Snowflake
```bash
make test RUN_MODE=SNOWFLAKE f=flypipe/cache/cache_snowpark_test.py
# Uses: .docker/snowflake/
```

## Building Images

Each environment can be built independently:

```bash
# Build core environment
make build RUN_MODE=PANDAS

# Build Spark environment
make build RUN_MODE=SPARK

# Build Snowflake environment
make build RUN_MODE=SNOWFLAKE
```

## Running Services

### Start Jupyter Lab
```bash
# Core (Pandas only)
make up RUN_MODE=PANDAS
# Access: http://localhost:8888

# Spark (with full cluster)
make up RUN_MODE=SPARK
# Access: http://localhost:8888
# Spark UI: http://localhost:8080

# Snowflake
make up RUN_MODE=SNOWFLAKE
# Access: http://localhost:8888
```

### Interactive Shell
```bash
# Enter container shell
make bash RUN_MODE=PANDAS
make bash RUN_MODE=SPARK
make bash RUN_MODE=SNOWFLAKE
```

## Benefits

| Aspect | Before (monolithic) | After (split) |
|--------|-------------------|---------------|
| **Build time** | ~10 min (always) | 2-10 min (depends on mode) |
| **Image size** | 2.5GB (always) | 500MB - 2.5GB |
| **Startup time** | ~60s (Spark cluster) | <5s (core) to 60s (Spark) |
| **Resource usage** | High (always) | Low to High (depends on mode) |
| **Development speed** | Slow | Fast for core dev |
| **CI/CD efficiency** | Low | High (parallel builds) |

## CI/CD Strategy

```yaml
# Example GitHub Actions workflow
jobs:
  core-tests:
    # Fastest - runs first
    run: |
      make build RUN_MODE=PANDAS
      make test RUN_MODE=PANDAS
  
  spark-tests:
    needs: core-tests
    run: |
      make build RUN_MODE=SPARK
      make test RUN_MODE=SPARK
  
  snowflake-tests:
    needs: core-tests
    run: |
      make build RUN_MODE=SNOWFLAKE
      make test RUN_MODE=SNOWFLAKE
```

## Migration Notes

### Old Commands
```bash
# These still work but now use dynamic Docker directory
make build
make test
make up
```

### New Commands
```bash
# Explicit RUN_MODE for clarity
make build RUN_MODE=PANDAS
make test RUN_MODE=SPARK f=mytest.py
make up RUN_MODE=SNOWFLAKE
```

## Troubleshooting

### Port Conflicts
If ports are already in use:
- Core: 8888 (Jupyter)
- Spark: 7077, 8080, 8081, 4040, 4041, 15002
- Snowflake: 8888 (Jupyter)

Modify the respective `docker-compose.yaml` port mappings.

### Build Failures
```bash
# Clean everything and rebuild
make down RUN_MODE=SPARK
docker system prune -a
make build RUN_MODE=SPARK
```

### Container Name Conflicts
Each RUN_MODE uses a different container name:
- `CORE` → `flypipe-core`
- `SPARK`/`SPARK_CONNECT` → `flypipe-spark`
- `SNOWFLAKE` → `flypipe-snowflake`

