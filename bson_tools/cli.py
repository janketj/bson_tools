import argparse
import sys
import json
from pathlib import Path

from .processor.analyzer import BSONAnalyzer
from .processor.transformer import BSONTransformer
from .processor.validator import BSONValidator
from .utils.logging import setup_logging
from .utils.progress import ProgressTracker

def parse_args():
    parser = argparse.ArgumentParser(description="Enhanced BSON File Processor")
    parser.add_argument('command', choices=['analyze', 'transform', 'validate', 'compare',
                                          'export', 'deduplicate', 'trim', 'clean'],
                       help='Command to execute')
    parser.add_argument('input', help='Input BSON file path')
    parser.add_argument('--output', '-o', help='Output file path')
    parser.add_argument('--number', '-n', type=int, help='Document number for trim operations')
    parser.add_argument('--compare-with', help='Second BSON file for comparison')
    parser.add_argument('--quiet', '-q', action='store_true', help='Suppress progress messages')
    parser.add_argument('--format', choices=['json', 'yaml'], default='json',
                       help='Output format for analysis results')
    return parser.parse_args()

def main():
    args = parse_args()
    logger = setup_logging(quiet=args.quiet)
    progress = ProgressTracker(quiet=args.quiet)

    try:
        if args.command == 'analyze':
            analyzer = BSONAnalyzer(args.input, progress=progress)
            results = analyzer.analyze()
            print(json.dumps(results, indent=2, default=str))

        elif args.command in ('transform', 'export', 'deduplicate', 'trim', 'clean'):
            if not args.output:
                raise ValueError(f"{args.command} command requires --output argument")
            transformer = BSONTransformer(args.input, args.output, progress=progress)

            if args.command == 'transform':
                transformer.transform()
            elif args.command == 'export':
                transformer.export_json()
            elif args.command == 'deduplicate':
                transformer.deduplicate()
            elif args.command == 'trim':
                if args.number is None:
                    raise ValueError("trim command requires --number argument")
                transformer.trim(args.number)
            elif args.command == 'clean':
                transformer.clean()

        elif args.command == 'validate':
            validator = BSONValidator(args.input, progress=progress)
            report = validator.validate()
            print(json.dumps(report, indent=2))

        elif args.command == 'compare':
            if not args.compare_with:
                raise ValueError("compare command requires --compare-with argument")
            analyzer = BSONAnalyzer(args.input, progress=progress)
            diff = analyzer.compare(args.compare_with)
            print(json.dumps(diff, indent=2, default=str))

    except Exception as e:
        logger.error(str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
