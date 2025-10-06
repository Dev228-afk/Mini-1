"""Generate a deduplicated dataset for the 2020 fire data."""

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data" / "2020-fire"
INPUT_PATH = DATA_DIR / "merged.csv"
OUTPUT_PATH = DATA_DIR / "unique_2020_fire_data.csv"


def main() -> None:
	if not INPUT_PATH.exists():
		raise FileNotFoundError(
			f"Could not find input file at {INPUT_PATH}. "
			"Ensure the dataset has been downloaded and paths are correct."
		)

	df = pd.read_csv(INPUT_PATH)
	df_unique = df.drop_duplicates()
	# take mini sample of 100 rows
	df_unique = df_unique.sample(n=100, random_state=42)
	df_unique.to_csv(OUTPUT_PATH, index=False)

	print(
		"Saved", len(df_unique), "unique rows to", OUTPUT_PATH.relative_to(BASE_DIR)
	)


if __name__ == "__main__":
	main()

