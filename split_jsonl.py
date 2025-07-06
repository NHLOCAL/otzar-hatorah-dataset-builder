import argparse
import os
import sys

def count_lines(filepath):
    """Return the total number of lines in the file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)


def split_jsonl(input_path, output1, output2):
    """
    Split a JSONL file into two parts with approximately equal number of lines.
    """
    total = count_lines(input_path)
    if total == 0:
        print(f"Input file '{input_path}' is empty.")
        return

    half = total // 2

    with open(input_path, 'r', encoding='utf-8') as src, \
         open(output1, 'w', encoding='utf-8') as out1, \
         open(output2, 'w', encoding='utf-8') as out2:
        for idx, line in enumerate(src):
            if idx < half:
                out1.write(line)
            else:
                out2.write(line)

    print(f"Total lines: {total}")
    print(f"Written {min(half, total)} lines to '{output1}'.")
    print(f"Written {total - min(half, total)} lines to '{output2}'.")


def main():
    parser = argparse.ArgumentParser(
        description="Split a JSONL (newline-delimited JSON) file into two files of equal size."
    )
    parser.add_argument(
        '-i', '--input', required=True,
        help='Path to the input JSONL file.'
    )
    parser.add_argument(
        '-o1', '--output1', default=None,
        help='Path for the first output file. Defaults to input_basename_part1.jsonl'
    )
    parser.add_argument(
        '-o2', '--output2', default=None,
        help='Path for the second output file. Defaults to input_basename_part2.jsonl'
    )

    args = parser.parse_args()
    input_path = args.input
    if not os.path.isfile(input_path):
        print(f"Error: Input file '{input_path}' does not exist.")
        sys.exit(1)

    base, ext = os.path.splitext(input_path)
    output1 = args.output1 or f"{base}_part1{ext or '.jsonl'}"
    output2 = args.output2 or f"{base}_part2{ext or '.jsonl'}"

    split_jsonl(input_path, output1, output2)

if __name__ == '__main__':
    main()