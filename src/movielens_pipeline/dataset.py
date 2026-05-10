from urllib.request import urlretrieve
import zipfile

def ensure_movielens_dataset(paths, dataset_url):
    ratings_csv = paths["ratings_csv"]
    movies_csv = paths["movies_csv"]
    zip_path = paths["zip_path"]
    source_dir = paths["source_dir"]

    if ratings_csv.exists() and movies_csv.exists():
        return ratings_csv, movies_csv

    source_dir.mkdir(parents=True, exist_ok=True)
    if not zip_path.exists():
        urlretrieve(dataset_url, str(zip_path))

    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(source_dir)

    if not ratings_csv.exists() or not movies_csv.exists():
        raise RuntimeError("MovieLens files were not found after extraction.")

    return ratings_csv, movies_csv
