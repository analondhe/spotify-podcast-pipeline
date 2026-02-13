"""Download the Top Spotify Podcasts dataset from Kaggle."""

import os
import subprocess
import sys


DATASET = "daniilmiheev/top-spotify-podcasts-daily-updated"
DATA_DIR = os.path.join(os.path.dirname(__file__), os.pardir, "data")


def main():
    data_dir = os.path.abspath(DATA_DIR)
    os.makedirs(data_dir, exist_ok=True)

    print(f"Downloading {DATASET} to {data_dir} ...")
    subprocess.run(
        [
            os.path.join(os.path.dirname(sys.executable), "kaggle"),
            "datasets",
            "download",
            "-d",
            DATASET,
            "-p",
            data_dir,
            "--unzip",
        ],
        check=True,
    )
    print("Download complete.")


if __name__ == "__main__":
    main()
