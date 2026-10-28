from src import discover
def test_check_silver_success(tmp_path, monkeypatch):
    silver_dir = tmp_path / "silver" / "yellow_taxi_2025-06"
    silver_dir.mkdir(parents=True)
    file_path = silver_dir/"_SUCCESS"
    file_path.touch()
    monkeypatch.setattr(discover, "PROJECT_ROOT",tmp_path)
    assert discover.check_silver("2025-06") is True
def test_check_silver_missing_success(tmp_path, monkeypatch):
    silver_dir = tmp_path / "silver" / "yellow_taxi_2025-06"
    silver_dir.mkdir(parents = True)
    monkeypatch.setattr(discover, "PROJECT_ROOT", tmp_path)
    assert discover.check_silver("2025-06") is False
def test_discover_months(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    (raw_dir / "yellow_tripdata_2025-01.parquet").touch()
    (raw_dir / "yellow_tripdata_2025-02.parquet").touch()
    (raw_dir / "taxi_zone_lookup.csv").touch()

    monkeypatch.setattr(discover, "PROJECT_ROOT", tmp_path)

    result = discover.discover_months()

    assert result == ["2025-01", "2025-02"]
def test_get_pending_months(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    for month in ["2025-01", "2025-02", "2025-03"]:
        file_path = raw_dir / f"yellow_tripdata_{month}.parquet"
        file_path.touch()
    for month in ["2025-01", "2025-03"]:
        silver_dir = tmp_path / "silver" / f"yellow_taxi_{month}"
        silver_dir.mkdir(parents=True)

        success_file = silver_dir / "_SUCCESS"
        success_file.touch()

    # 3. Thay PROJECT_ROOT bằng tmp_path
    monkeypatch.setattr(discover, "PROJECT_ROOT", tmp_path)

    # 4. Kiểm tra kết quả
    result = discover.get_pending_months()

    assert result == ["2025-02"]
