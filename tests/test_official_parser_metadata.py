from pathlib import Path

from bortrace.official_parser import parse_beforeinfo, parse_racelist


DATA_ROOT = Path(__file__).resolve().parents[2] / "bortrace_data" / "official_pages"


def test_parses_race_and_weather_metadata() -> None:
    race = DATA_ROOT / "20260825" / "01" / "9"
    meta = parse_racelist(race / "racelist.html", rno=9)
    weather = parse_beforeinfo(race / "beforeinfo.html")
    assert meta["grade"] == "SG"
    assert meta["race_stage"] == "予選"
    assert meta["event_day"] == 1
    assert meta["deadline_minutes"] == 18 * 60 + 56
    assert weather["weather"] == "晴"
    assert weather["air_temperature"] == 31.0
    assert weather["water_temperature"] == 27.0
    assert weather["wind_speed"] == 2.0
    assert weather["wave_height"] == 1.0
    assert weather["wind_direction_code"] == 6


def test_parses_stabilizer_and_final_day() -> None:
    race = DATA_ROOT / "20210108" / "24" / "9"
    meta = parse_racelist(race / "racelist.html", rno=9)
    assert meta["grade"] == "ippan"
    assert meta["uses_stabilizer"] == 1
    assert meta["is_final_day"] == 1


def test_parses_fixed_entry() -> None:
    race = DATA_ROOT / "20210102" / "16" / "9"
    meta = parse_racelist(race / "racelist.html", rno=9)
    assert meta["is_fixed_entry"] == 1
