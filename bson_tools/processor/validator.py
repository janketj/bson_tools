import struct
from typing import Dict, Any
from bson import BSON

from .base import BSONProcessor

class BSONValidator(BSONProcessor):
    def validate(self) -> Dict[str, Any]:
        """Validate BSON file and return detailed report"""
        self._validate_paths()

        report = {
            'valid_documents': 0,
            'invalid_documents': 0,
            'errors': [],
            'warnings': [],
            'integrity_check': True
        }

        file_size = self.input_path.stat().st_size
        processed_bytes = 0
        doc_count = 0

        with open(self.input_path, 'rb') as infile:
            while True:
                size_bytes = infile.read(4)
                if not size_bytes:
                    break

                if len(size_bytes) < 4:
                    report['errors'].append(f"Truncated size field at position {processed_bytes}")
                    report['integrity_check'] = False
                    break

                doc_size = struct.unpack("<i", size_bytes)[0]
                if doc_size < 5:
                    report['errors'].append(f"Invalid document size ({doc_size}) at position {processed_bytes}")
                    report['integrity_check'] = False
                    break

                infile.seek(-4, 1)
                document = infile.read(doc_size)
                processed_bytes += len(document)

                try:
                    _ = BSON(document).decode()
                    report['valid_documents'] += 1
                except Exception as e:
                    report['invalid_documents'] += 1
                    report['errors'].append(f"Document {doc_count}: {str(e)}")

                doc_count += 1
                self.progress.update(f"Validated document {doc_count}")

        # Check if file size matches processed bytes
        if processed_bytes != file_size:
            report['warnings'].append(
                f"File size ({file_size}) doesn't match processed bytes ({processed_bytes})"
            )

        return report
