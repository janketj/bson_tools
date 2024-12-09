# 🛠️ BSON Tools

[![Python 3.6+](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A powerful toolkit for analyzing, maintaining, and transforming BSON files with ease. Perfect for MongoDB database maintenance, data migration, and debugging.

## ✨ Features

### 📊 Analysis
- Generate comprehensive statistics about BSON contents
- Field name frequencies and data type distribution
- Document size analysis
- Array field detection
- Date range identification
- Structural validation

### 🔄 Transformation
- Convert BSON to JSON with proper type handling
- Remove duplicate documents
- Clean invalid UTF-8 and corrupt BSON data
- Trim files to specific document counts
- Remove specific documents

### 🔍 Validation
- Full structural validation
- UTF-8 encoding verification
- Size field validation
- Detailed error reporting
- Integrity checking

## 🚀 Quick Start

### Installation

```bash
pip install bson-tools
```

### Basic Usage

```bash
# Analyze a BSON file
bson-tools analyze input.bson

# Convert to JSON
bson-tools export input.bson -o output.json

# Remove duplicates
bson-tools deduplicate input.bson -o clean.bson

# Validate file
bson-tools validate suspect.bson
```

## 📖 Detailed Usage

### Analysis

Get detailed information about your BSON file:

```bash
bson-tools analyze large-collection.bson
```

Output example:
```json
{
  "total_documents": 1000,
  "total_size_bytes": 2048576,
  "avg_doc_size_bytes": 2048.58,
  "field_names": {
    "_id": 1000,
    "name": 985,
    "data.nested": 750
  },
  "data_types": {
    "ObjectId": 1000,
    "string": 1985,
    "int": 750
  },
  "array_fields": ["tags", "categories"],
  "date_range": {
    "min": "2023-01-01T00:00:00",
    "max": "2024-12-31T23:59:59"
  }
}
```

### Transformation

#### Export to JSON
```bash
bson-tools export input.bson -o output.json
```

#### Remove Duplicates
```bash
bson-tools deduplicate input.bson -o deduped.bson
```

#### Trim File
```bash
bson-tools trim input.bson -o trimmed.bson -n 1000
```

#### Clean Invalid Documents
```bash
bson-tools clean corrupt.bson -o clean.bson
```

### Validation

Run comprehensive validation:

```bash
bson-tools validate suspect.bson
```

Output example:
```json
{
  "valid_documents": 995,
  "invalid_documents": 5,
  "errors": [
    "Document 996: Invalid UTF-8 encoding",
    "Document 998: Truncated document"
  ],
  "warnings": [
    "File size mismatch detected"
  ],
  "integrity_check": false
}
```

## 🎯 Common Use Cases

### Database Maintenance
- Validate BSON dumps before restoration
- Clean corrupted backup files
- Remove duplicate documents
- Analyze collection structure

### Data Migration
- Convert BSON to JSON for processing
- Validate data integrity
- Transform document structure
- Clean invalid entries

### Debugging
- Analyze document structure
- Identify data type mismatches
- Locate corrupt documents
- Verify file integrity

## 🔧 Advanced Usage

### Compare Two Files
```bash
bson-tools compare original.bson --compare-with modified.bson
```

### Custom Output Format
```bash
bson-tools analyze input.bson --format yaml
```

### Quiet Mode
```bash
bson-tools transform input.bson -o output.bson --quiet
```

## 📝 Command Reference

```bash
bson-tools <command> [options]

Commands:
  analyze     Generate statistics about BSON contents
  export      Convert BSON to JSON
  deduplicate Remove duplicate documents
  validate    Check file integrity
  clean       Remove invalid documents
  trim        Keep first N documents
  transform   Apply custom transformations
  compare     Compare two BSON files

Options:
  --output, -o     Output file path
  --number, -n     Document number for trim
  --compare-with   Second file for comparison
  --quiet, -q      Suppress progress messages
  --format         Output format (json|yaml)
```

## ⚠️ Best Practices

1. **Always Backup**
   - Keep original files until verification
   - Test on sample data first
   - Verify output integrity

2. **Performance**
   - Use quiet mode for scripts
   - Monitor memory usage
   - Process large files in stages

3. **Validation**
   - Validate files after operations
   - Check output file sizes
   - Verify document counts

## 🤝 Contributing

Contributions are welcome! Areas for improvement:

- Additional validation checks
- Performance optimizations
- New transformation features
- Extended format support
- Documentation improvements

## 📄 License

MIT License - Feel free to use and modify as needed.

## 🛟 Support

- Report issues on GitHub
- Check documentation
- Contact maintainers

## 🔍 Debugging Tips

If you encounter issues:

1. Run validation first
2. Check file permissions
3. Verify input file integrity
4. Monitor system resources
5. Enable verbose logging

## 🏗️ Architecture

The toolkit is built with modularity in mind:

```
bson_tools/
├── processor/
│   ├── analyzer.py
│   ├── transformer.py
│   └── validator.py
├── utils/
│   ├── logging.py
│   └── progress.py
└── cli.py
```

Each component is independent and focused on specific tasks.

---

Made with ❤️
