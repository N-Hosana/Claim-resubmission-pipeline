# Claim Resubmission Pipeline

A pipeline that processes healthcare insurance claims from different EMR systems and figures out which ones can be resubmitted. Handles CSV and JSON inputs, normalizes the data, and applies business rules to identify good candidates.

## What This Does

This pipeline solves the problem of dealing with claims data that comes in different formats from various Electronic Medical Records systems. Instead of manually processing each one, it:

- **Ingests data** from multiple sources (CSV, JSON)
- **Normalizes everything** into a common format
- **Applies business logic** to figure out what can be resubmitted
- **Spits out clean results** that can be used by other systems

## How It Works

The pipeline takes data from two sources, normalizes it, and processes it through business rules:

```
EMR Alpha (CSV)  →  Normalize  →  Business Rules  →  Output
EMR Beta (JSON)  →  Normalize  →  Business Rules  →  Output
```

## Features

### Core Stuff
- **Multi-format Support**: Works with CSV and JSON files
- **Schema Unification**: Converts everything to the same format
- **Business Rules Engine**: Implements the logic for resubmission eligibility
- **Error Handling**: Doesn't crash when things go wrong
- **Metrics**: Tracks what's happening

### Business Logic
The pipeline checks these rules to see if a claim can be resubmitted:

1. **Status must be "denied"** (obviously)
2. **Patient ID must exist** (can't resubmit without knowing who it's for)
3. **Claim must be old enough** (> 7 days from reference date)
4. **Denial reason must be retryable**

### Denial Reason Categories
- **Retryable**: "Missing modifier", "Incorrect NPI", "Prior auth required"
- **Non-Retryable**: "Authorization expired", "Incorrect provider type"  
- **Ambiguous**: Gets classified using some heuristics (like "incorrect procedure")

### Data Processing
- **Date handling**: Converts various date formats to ISO standard
- **Null values**: Explicitly handles missing data
- **Field mapping**: Maps source-specific fields to the standard format
- **Validation**: Makes sure the data makes sense

## Getting Started

### What You Need
- Python 3.8 or higher
- No external packages needed (uses built-in libraries)

### Setup
1. Download or clone the project
2. Put your data files in the `data/` folder:
   - `data/emr_alpha.csv`
   - `data/emr_beta.json`

### Running It
```bash
python claim_pipeline.py
```

### What You Get
The pipeline creates:
- `resubmission_candidates.json` - Claims that can be resubmitted
- `pipeline.log` - Detailed logs of what happened
- Console output with summary stats

## Data Format

### Input - EMR Alpha (CSV)
```csv
claim_id,patient_id,procedure_code,denial_reason,submitted_at,status
A123,P001,99213,Missing modifier,2025-07-01,denied
```

### Input - EMR Beta (JSON)
```json
{
  "id": "B987",
  "member": "P010",
  "code": "99213",
  "error_msg": "Incorrect provider type",
  "date": "2025-07-03T00:00:00",
  "status": "denied"
}
```

### Output Format
```json
{
  "claim_id": "A124",
  "resubmission_reason": "Incorrect NPI",
  "source_system": "alpha",
  "recommended_changes": "Review NPI number and resubmit",
  "patient_id": "P002",
  "procedure_code": "99214",
  "submitted_at": "2025-07-10"
}
```

## Configuration

### Reference Date
The pipeline uses a reference date (default: 2025-07-30) to calculate how old claims are. You can change this:

```python
pipeline = ClaimResubmissionPipeline(reference_date="2025-08-01")
```

### Business Rules
You can modify what counts as retryable:

```python
self.retryable_reasons = {
    "Missing modifier",
    "Incorrect NPI", 
    "Prior auth required",
    "Your custom reason here"
}
```

## Metrics & Monitoring

The pipeline tracks:
- Total claims processed
- How many from each source
- Claims flagged for resubmission
- Claims excluded and why
- Processing performance

## Testing

### Sample Data
Includes test data for both systems:
- **EMR Alpha**: 5 sample claims with various denial reasons
- **EMR Beta**: 6 sample claims with different structures

### Expected Results
Based on the sample data:
- **Total Claims**: 11
- **Eligible for Resubmission**: 6-7 claims
- **Excluded**: 4-5 claims (various reasons)

### Running Tests
```bash
python test_pipeline.py
```

## Future Improvements

### Already Done
- **Modular Design**: Clean class-based structure
- **Logging**: Comprehensive execution tracking
- **Error Handling**: Robust exception management
- **Metrics**: Performance monitoring

### Could Add Later
- **FastAPI Integration**: REST API for data upload
- **Database**: Store processed claims
- **Real-time Processing**: Stream processing
- **ML Classification**: Better denial reason classification
- **Workflow Tools**: Dagster/Prefect integration

## Logging

Logs go to `pipeline.log` with different levels:
- **INFO**: General processing info
- **WARNING**: Non-critical issues
- **ERROR**: Things that broke
- **DEBUG**: Detailed debugging (when enabled)

## Error Handling

The pipeline handles various problems gracefully:
- **Missing files**: Won't crash if data files aren't there
- **Bad data**: Handles malformed records
- **Date issues**: Flexible date parsing
- **Schema problems**: Field mapping validation

## Code Quality

### Standards
- Follows PEP 8 (mostly)
- Good docstrings (but not excessive)
- Type hints where helpful
- Proper error handling
- Clean, readable code

### Testing
- Test with different data formats
- Validate business logic
- Check error handling
- Performance testing

## Troubleshooting

If something goes wrong:
1. Check `pipeline.log` for error details
2. Look at console output for summary
3. Verify data file formats
4. Make sure Python version is compatible

## Contributing

Feel free to improve this! Some ideas:
- Add more data format support
- Improve the business logic
- Better error handling
- Performance optimizations
- Additional output formats

---

**Built for healthcare data teams who want to automate claim processing instead of doing it manually.**
