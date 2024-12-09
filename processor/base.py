from pathlib import Path
from typing import Optional
from ..utils.progress import ProgressTracker

class BSONProcessor:
    def __init__(self, input_path: str, output_path: Optional[str] = None,
                 progress: Optional[ProgressTracker] = None):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path) if output_path else None
        self.progress = progress or ProgressTracker()

    def _validate_paths(self) -> None:
        """Validate input and output paths"""
        if not self.input_path.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_path}")
        if self.output_path and self.output_path.exists():
            raise FileExistsError(f"Output file already exists: {self.output_path}")
        if self.output_path and self.input_path == self.output_path:
            raise ValueError("Input and output paths cannot be the same")
