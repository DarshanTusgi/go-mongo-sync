# Environment Variable Substitution in Collections Configuration

## Overview

The `collectionsv2.json` configuration file now supports environment variable substitution, allowing you to dynamically configure database and collection names, filter values, and other configuration parameters based on environment variables.

## Syntax

### Basic Substitution
```json
{
  "name": "authn-${SOURCE_DATABASE}"
}
```

When `SOURCE_DATABASE=production` is set, this becomes:
```json
{
  "name": "authn-production"
}
```

### Substitution with Default Values
```json
{
  "name": "users_${TENANT_ID:-default}"
}
```

- If `TENANT_ID` is set (e.g., `TENANT_ID=kotak`), result: `"users_kotak"`
- If `TENANT_ID` is not set, result: `"users_default"`

## Supported Patterns

1. **Simple variable**: `${VAR_NAME}`
   - Replaces with environment variable value
   - If not set, becomes empty string

2. **Variable with default**: `${VAR_NAME:-default_value}`
   - Replaces with environment variable value if set
   - Otherwise uses the default value after `:-`

## Use Cases

### 1. Multi-Environment Deployments

```json
{
  "databases": [
    {
      "name": "authn-${ENVIRONMENT}",
      "collections": [
        {
          "name": "users_${TENANT_ID}",
          "document_filter": {
            "criteria": [
              {
                "field": "tenantId",
                "operator": "eq",
                "value": "${TENANT_FILTER_ID}"
              }
            ]
          }
        }
      ]
    }
  ]
}
```

**Development:**
```bash
export ENVIRONMENT=dev
export TENANT_ID=test
export TENANT_FILTER_ID=68418b2587942f1d3158a000
```

**Production:**
```bash
export ENVIRONMENT=prod
export TENANT_ID=kotak
export TENANT_FILTER_ID=68418b2587942f1d3158a798
```

### 2. Dynamic Database Prefixes

```json
{
  "databases": [
    {
      "name": "${DB_PREFIX:-users}-mgmt",
      "collections": [...]
    }
  ]
}
```

- With `DB_PREFIX=customer`: becomes `"customer-mgmt"`
- Without `DB_PREFIX`: becomes `"users-mgmt"` (default)

### 3. Tenant-Specific Collections

```json
{
  "collections": [
    {
      "name": "orders_${TENANT_ID}",
      "document_filter": {
        "criteria": [
          {
            "field": "status",
            "operator": "eq",
            "value": "${ORDER_STATUS_FILTER:-active}"
          }
        ]
      }
    }
  ]
}
```

### 4. Community-Specific Filtering

```json
{
  "document_filter": {
    "criteria": [
      {
        "field": "communityId",
        "operator": "eq",
        "value": "${COMMUNITY_FILTER_ID:-68418b2587942f1d3158a799}"
      }
    ]
  }
}
```

## Example Configuration

See `configs/collectionsv2-example.json` for a complete example demonstrating:
- Database name substitution
- Collection name substitution
- Filter value substitution
- Default values

## How It Works

1. **Load Time Processing**: Environment variables are expanded when the configuration file is loaded
2. **Pre-Unmarshalling**: Substitution happens before JSON parsing, ensuring all values are properly typed
3. **Regex-Based**: Uses regex pattern matching to find `${VAR}` and `${VAR:-default}` patterns
4. **Safe Defaults**: Missing variables without defaults become empty strings

## Logging

When environment variable expansion occurs, you'll see:
```
📝 CONFIG: Environment variable expansion completed for configs/collectionsv2.json
```

## Best Practices

1. **Use descriptive variable names**: `TENANT_ID` instead of `TID`
2. **Provide defaults for optional values**: `${DB_PREFIX:-default}`
3. **Document required variables**: List all required environment variables in deployment docs
4. **Validate in production**: Ensure all required variables are set before deployment

## Variable Naming Rules

- Must start with a letter or underscore: `[A-Za-z_]`
- Can contain letters, numbers, and underscores: `[A-Za-z0-9_]*`
- Case-sensitive: `TENANT_ID` ≠ `tenant_id`

## Limitations

- No nested variable expansion: `${VAR_${OTHER}}` is not supported
- No expression evaluation: `${VAR1+VAR2}` is not supported
- Simple string replacement only

## Testing

To test the feature:

```bash
# Set environment variables
export SOURCE_DATABASE=production
export TENANT_ID=kotak
export TENANT_FILTER_ID=68418b2587942f1d3158a798

# Start cloud-sync
./runnables/cloud-sync -config configs/cloud-config.yaml

# Check logs for expansion message
# Verify database names in MongoDB match expected values
```

## Migration from Static Configuration

**Before:**
```json
{
  "name": "authn",
  "collections": [
    {
      "name": "users",
      "document_filter": {
        "criteria": [
          { "field": "tenantId", "operator": "eq", "value": "68418b2587942f1d3158a798" }
        ]
      }
    }
  ]
}
```

**After:**
```json
{
  "name": "authn-${ENVIRONMENT}",
  "collections": [
    {
      "name": "users_${TENANT_ID}",
      "document_filter": {
        "criteria": [
          { "field": "tenantId", "operator": "eq", "value": "${TENANT_FILTER_ID}" }
        ]
      }
    }
  ]
}
```

With appropriate environment variables set, this provides the same configuration while enabling multi-environment deployments.
