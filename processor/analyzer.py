from typing import Dict, Any, Counter
from datetime import datetime
from collections import Counter
from bson import BSON
import struct

from .base import BSONProcessor

class BSONAnalyzer(BSONProcessor):
    def analyze(self) -> Dict[str, Any]:
        """Analyze BSON file and return statistics"""
        self._validate_paths()

        stats = {
            'total_documents': 0,
            'total_size_bytes': 0,
            'avg_doc_size_bytes': 0,
            'field_names': Counter(),
            'data_types': Counter(),
            'array_fields': [],
            'date_range': {'min': None, 'max': None},
            'sample_documents': []
        }

        with open(self.input_path, 'rb') as infile:
            while True:
                size_bytes = infile.read(4)
                if not size_bytes:
                    break

                doc_size = struct.unpack("<i", size_bytes)[0]
                infile.seek(-4, 1)
                document = infile.read(doc_size)

                try:
                    doc_dict = BSON(document).decode()
                    stats['total_documents'] += 1
                    stats['total_size_bytes'] += doc_size

                    self._analyze_document_structure(doc_dict, stats)

                    if stats['total_documents'] <= 5:
                        stats['sample_documents'].append(doc_dict)

                    self.progress.update(f"Analyzed document {stats['total_documents']}")

                except Exception as e:
                    self.progress.warn(f"Could not analyze document {stats['total_documents']}: {e}")

        if stats['total_documents'] > 0:
            stats['avg_doc_size_bytes'] = stats['total_size_bytes'] / stats['total_documents']

        return stats

    def _analyze_document_structure(self, doc: Dict, stats: Dict, prefix: str = '') -> None:
        """Recursively analyze document structure"""
        for key, value in doc.items():
            full_path = f"{prefix}.{key}" if prefix else key

            stats['field_names'][full_path] += 1
            stats['data_types'][type(value).__name__] += 1

            if isinstance(value, list):
                if full_path not in stats['array_fields']:
                    stats['array_fields'].append(full_path)

            if isinstance(value, datetime):
                if stats['date_range']['min'] is None or value < stats['date_range']['min']:
                    stats['date_range']['min'] = value
                if stats['date_range']['max'] is None or value > stats['date_range']['max']:
                    stats['date_range']['max'] = value

            if isinstance(value, dict):
                self._analyze_document_structure(value, stats, full_path)

    def compare(self, other_path: str) -> Dict[str, Any]:
        """Compare two BSON files"""
        other_analyzer = BSONAnalyzer(other_path, progress=self.progress)

        stats1 = self.analyze()
        stats2 = other_analyzer.analyze()

        return {
            'document_count_diff': stats2['total_documents'] - stats1['total_documents'],
            'size_diff_bytes': stats2['total_size_bytes'] - stats1['total_size_bytes'],
            'field_differences': {
                'added': list(set(stats2['field_names'].keys()) - set(stats1['field_names'].keys())),
                'removed': list(set(stats1['field_names'].keys()) - set(stats2['field_names'].keys())),
            },
            'type_differences': {
                'added': list(set(stats2['data_types'].keys()) - set(stats1['data_types'].keys())),
                'removed': list(set(stats1['data_types'].keys()) - set(stats2['data_types'].keys())),
            }
        }
