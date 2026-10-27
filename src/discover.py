import os
import re
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
def discover_months():
    months = []
    #lấy raw
    files = os.listdir(PROJECT_ROOT / "raw")
    # 2. Duyệt từng filename
    for filename in files:
        match = re.fullmatch(
        r"yellow_tripdata_(\d{4}-\d{2})\.parquet",
        filename
        )
        # 3. Kiểm tra filename có đúng định dạng không
        if match:
            print(f"Valid file: {filename}")
        else:
            print(f"Invalid file: {filename}")
    # 4. Trích xuất YYYY-MM
        if match:
            month = match.group(1)
            months.append(month)
    months = sorted(set(months))
    return months
def check_silver(month):
    path = PROJECT_ROOT / "silver" / f"yellow_taxi_{month}"
    success_file = path / '_SUCCESS'
    return path.is_dir() and success_file.is_file()
def get_pending_months():
    pending_months = []
    months = discover_months()
    for month in months:
        if not check_silver(month):
            pending_months.append(month)
    return pending_months
if __name__ == "__main__":
    print("Raw months:", discover_months())
    print("Pending months:", get_pending_months())