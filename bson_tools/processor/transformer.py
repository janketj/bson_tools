import json
import hashlib
import struct
from bson import BSON, json_util
from typing import Set

from .base import BSONProcessor

class BSONTransformer(BSONProcessor):
    def transform(self) -> None:
        """Base transformation method - override in subclasses"""
        raise NotImplementedError("Transformer must implement transform method")

    def export_json(self) -> None:
        """Export BSON to JSON format"""
        self._validate_paths()

        with open(self.input_path, 'rb') as infile, open(self.output_path, 'w') as outfile:
            outfile.write('[\n')
            is_first = True
            doc_count = 0

            while True:
                size_bytes = infile.read(4)
                if not size_bytes:
                    break

                doc_size = struct.unpack("<i", size_bytes)[0]
                infile.seek(-4, 1)
                document = infile.read(doc_size)

                try:
                    doc_dict = BSON(document).decode()
                    if not is_first:
                        outfile.write(',\n')
                    json_str = json_util.dumps(doc_dict, indent=2)
                    outfile.write(json_str)
                    is_first = False

                except Exception as e:
                    self.progress.warn(f"Could not convert document {doc_count}: {e}")

                doc_count += 1
                self.progress.update(f"Converted document {doc_count}")

            outfile.write('\n]')

    def deduplicate(self) -> None:
        """Remove duplicate documents"""
        self._validate_paths()

        seen_hashes: Set[str] = set()
        doc_count = 0

        with open(self.input_path, 'rb') as infile, open(self.output_path, 'wb') as outfile:
            while True:
                size_bytes = infile.read(4)
                if not size_bytes:
                    break

                doc_size = struct.unpack("<i", size_bytes)[0]
                infile.seek(-4, 1)
                document = infile.read(doc_size)

                try:
                    doc_dict = BSON(document).decode()
                    doc_hash = hashlib.sha256(
                        json.dumps(doc_dict, sort_keys=True, default=str).encode()
                    ).hexdigest()

                    if doc_hash not in seen_hashes:
                        outfile.write(document)
                        seen_hashes.add(doc_hash)

                except Exception as e:
                    self.progress.warn(f"Could not process document {doc_count}: {e}")

                doc_count += 1
                self.progress.update(f"Processed document {doc_count}")

    def trim(self, stop_after: int) -> None:
        """Keep only first N documents"""
        self._validate_paths()
        doc_count = 0

        with open(self.input_path, 'rb') as infile, open(self.output_path, 'wb') as outfile:
            while doc_count <= stop_after:
                size_bytes = infile.read(4)
                if not size_bytes:
                    break

                doc_size = struct.unpack("<i", size_bytes)[0]
                infile.seek(-4, 1)
                document = infile.read(doc_size)
                outfile.write(document)

                doc_count += 1
                self.progress.update(f"Processed document {doc_count}")

    def clean(self) -> None:
        """Remove invalid documents"""
        self._validate_paths()
        doc_count = 0

        with open(self.input_path, 'rb') as infile, open(self.output_path, 'wb') as outfile:
            while True:
                size_bytes = infile.read(4)
                if not size_bytes:
                    break

                doc_size = struct.unpack("<i", size_bytes)[0]
                infile.seek(-4, 1)
                document = infile.read(doc_size)

                try:
                    # Validate document
                    _ = BSON(document).decode()
                    outfile.write(document)
                except Exception as e:
                    self.progress.warn(f"Removing invalid document {doc_count}: {e}")

                doc_count += 1
                self.progress.update(f"Processed document {doc_count}")
