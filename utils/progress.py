class ProgressTracker:
    def __init__(self, quiet: bool = False):
        self.quiet = quiet
        self.current = 0

    def update(self, message: str) -> None:
        """Update progress with a message"""
        self.current += 1
        if not self.quiet and self.current % 1000 == 0:
            print(message)

    def warn(self, message: str) -> None:
        """Print a warning message"""
        if not self.quiet:
            print(f"Warning: {message}")
