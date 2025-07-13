#!/usr/bin/env python3
import tarfile
from pathlib import Path

import requests
from bs4 import BeautifulSoup


def download_and_extract_tar_gz(url: str, download_folder: Path) -> None:
    """Download a .tar.gz file, extract it into a subfolder named after the file, and then delete the tar file."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error on a failed request

        filename = url.split("/")[-1]
        tar_gz_path = download_folder / filename
        extract_folder = download_folder / filename.removesuffix(
            ".tar.gz"
        )  # Create a specific folder for this file

        with open(tar_gz_path, "wb") as file:
            file.write(response.content)

        # Ensure the extract folder exists
        extract_folder.mkdir(exist_ok=True)

        # Extract the tar.gz file
        with tarfile.open(tar_gz_path, "r:gz") as tar:
            tar.extractall(path=extract_folder)
        print(f"Extracted: {filename} into {extract_folder}")

        # Delete the tar.gz file after extracting
        tar_gz_path.unlink()
        print(f"Deleted: {filename}")

    except Exception as e:
        print(f"Error downloading or extracting {url}. Error: {e}")


def main() -> None:
    page_url = "https://www.cs.ubc.ca/~hoos/SATLIB/benchm.html"
    download_folder = Path("inputs")
    download_folder.mkdir(exist_ok=True)

    try:
        response = requests.get(page_url)
        response.raise_for_status()  # Ensure we received a successful response

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a")

        for link in links:
            href = link.get("href")  # type: ignore
            if href and href.endswith(".tar.gz"):  # type: ignore
                full_url = f"{page_url.rsplit('/', 1)[0]}/{href}"
                print(f"Downloading and extracting {full_url}")
                download_and_extract_tar_gz(full_url, download_folder)

    except Exception as e:
        print(f"Failed to scrape {page_url}. Error: {e}")


if __name__ == "__main__":
    main()
