#!/bin/bash

# JSON Flattening Performance Test Script
# This script runs each flattening method individually and records timing

set -e

# Parse command line arguments
SKIP_WARMUP=false
if [[ "$1" == "--skip-warmup" ]]; then
    SKIP_WARMUP=true
    echo "Skipping warm-up session as requested"
fi

echo "=== JSON Flattening Performance Test ==="
echo "Starting at: $(date)"
echo ""

# Set environment variables
export DBT_ROLE_ARN="arn:aws:iam::$(aws sts get-caller-identity --query "Account" --output text):role/GlueInteractiveSessionRole"
export DBT_S3_LOCATION="s3://aws-dbt-glue-datalake-$(aws sts get-caller-identity --query "Account" --output text)-us-east-1"

echo "Environment:"
echo "DBT_ROLE_ARN: $DBT_ROLE_ARN"
echo "DBT_S3_LOCATION: $DBT_S3_LOCATION"
echo ""

# Activate virtual environment
source ../dbt-glue-env/bin/activate

# Function to cleanup S3 data and drop table
cleanup_model() {
    local model_name=$1
    echo "Cleaning up previous data for model: $model_name"
    
    # Remove S3 data
    aws s3 rm "${DBT_S3_LOCATION}/dbt_glue_json_comparison/${model_name}/" --recursive --quiet 2>/dev/null || true
    
    # Drop table if exists (ignore errors)
    aws glue delete-table --database-name dbt_glue_json_comparison --name "$model_name" --quiet 2>/dev/null || true
    
    echo "Cleanup completed for $model_name"
}

# Step 1: Warm up Glue Interactive Session (optional)
if [ "$SKIP_WARMUP" = false ]; then
    echo "=== STEP 1: Warming up Glue Interactive Session ==="
    echo "Starting warm-up at: $(date)"
    start_time=$(date +%s)
    
    # Cleanup previous warm-up data
    cleanup_model "session_warmup"
    
    dbt run --select warmup --profiles-dir profiles
    
    end_time=$(date +%s)
    warmup_duration=$((end_time - start_time))
    echo "Warm-up completed in: ${warmup_duration} seconds"
    echo ""
    
    # Wait a moment to ensure session is fully ready
    sleep 5
else
    echo "=== STEP 1: Skipping warm-up session ==="
    warmup_duration=0
    echo ""
fi

# Step 2: Test Legacy Approach (json_extract_scalar)
echo "=== STEP 2: Testing Legacy Approach (json_extract_scalar) ==="
echo "Starting legacy test at: $(date)"
start_time=$(date +%s)

# Cleanup previous legacy data
cleanup_model "flattened_transactions"

dbt run --select legacy --profiles-dir profiles

end_time=$(date +%s)
legacy_duration=$((end_time - start_time))
echo "Legacy approach completed in: ${legacy_duration} seconds"
echo ""

# Step 3: Test Optimized Approach (from_json with schema)
echo "=== STEP 3: Testing Optimized Approach (from_json) ==="
echo "Starting optimized test at: $(date)"
start_time=$(date +%s)

# Cleanup previous optimized data
cleanup_model "complete_optimized_transactions"

dbt run --select complete_optimized_transactions --profiles-dir profiles

end_time=$(date +%s)
optimized_duration=$((end_time - start_time))
echo "Optimized approach completed in: ${optimized_duration} seconds"
echo ""

# Step 4: Test Macro-based Approach
echo "=== STEP 4: Testing Macro-based Approach ==="
echo "Starting macro-based test at: $(date)"
start_time=$(date +%s)

# Cleanup previous macro-based data
cleanup_model "macro_based_flattened_transactions"

dbt run --select macro_based_flattened_transactions --profiles-dir profiles

end_time=$(date +%s)
macro_duration=$((end_time - start_time))
echo "Macro-based approach completed in: ${macro_duration} seconds"
echo ""

# Results Summary
echo "=== PERFORMANCE TEST RESULTS ==="
echo "Test completed at: $(date)"
echo ""
echo "Timing Results:"
if [ "$SKIP_WARMUP" = false ]; then
    echo "1. Session Warm-up:     ${warmup_duration} seconds"
fi
echo "2. Legacy (json_extract): ${legacy_duration} seconds"
echo "3. Optimized (from_json): ${optimized_duration} seconds"
echo "4. Macro-based:         ${macro_duration} seconds"
echo ""

# Calculate performance improvements
if [ $legacy_duration -gt 0 ]; then
    optimized_improvement=$(echo "scale=2; $legacy_duration / $optimized_duration" | bc -l)
    macro_improvement=$(echo "scale=2; $legacy_duration / $macro_duration" | bc -l)
    
    echo "Performance Improvements:"
    echo "- Optimized vs Legacy: ${optimized_improvement}x faster"
    echo "- Macro-based vs Legacy: ${macro_improvement}x faster"
fi

echo ""
echo "Tables created:"
if [ "$SKIP_WARMUP" = false ]; then
    echo "- dbt_glue_json_comparison.session_warmup"
fi
echo "- dbt_glue_json_comparison.flattened_transactions"
echo "- dbt_glue_json_comparison.complete_optimized_transactions"
echo "- dbt_glue_json_comparison.macro_based_flattened_transactions"

echo ""
echo "Usage: $0 [--skip-warmup]"
echo "  --skip-warmup: Skip the Glue session warm-up step"
