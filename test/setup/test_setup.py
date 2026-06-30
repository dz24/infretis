import copy
import os
from pathlib import Path, PosixPath

import pytest

from infretis.setup import (
    TOMLConfigError,
    check_config,
    setup_config,
    setup_temperatures,
    write_header,
)

HERE = Path(__file__).resolve().parent


def test_write_header(tmp_path: PosixPath, monkeypatch) -> None:
    """Test that we create new data file if datafile already present."""
    f1 = tmp_path / "temp"
    f1.mkdir()
    monkeypatch.chdir(f1)
    config: dict = {"current": {"size": 10}, "output": {"data_dir": "./"}}

    # write the first infretis_data.txt file
    write_header(config)
    assert os.path.isfile("./infretis_data.txt")
    for i in range(1, 6):
        # create new infretis_data.txt files
        write_header(config)
        isfile = f"./infretis_data_{i}.txt"
        assert os.path.isfile(isfile)
        assert config["output"]["data_file"] == isfile


def test_write_header_temperature_layers(
    tmp_path: PosixPath, monkeypatch
) -> None:
    """Test per-temperature data file names."""
    f1 = tmp_path / "temp"
    f1.mkdir()
    monkeypatch.chdir(f1)
    config = {
        "current": {"size": 10},
        "simulation": {"temperatures": [0.07, 0.07]},
        "output": {"data_dir": "./"},
    }

    write_header(config)

    assert os.path.isfile("./infretis_data_T000.txt")
    assert os.path.isfile("./infretis_data_T001.txt")
    assert config["output"]["data_file"] == "./infretis_data_T000.txt"
    assert config["output"]["data_files"] == [
        "./infretis_data_T000.txt",
        "./infretis_data_T001.txt",
    ]


def test_setup_temperatures_distinct_layers_disable_exchange() -> None:
    """Different temperatures use separate temperature layers."""
    config = {
        "current": {
            "active": [0, 1],
            "frac": {},
            "locked": [],
            "size": 2,
            "traj_num": 2,
        },
        "simulation": {
            "interfaces": [0.0, 1.0],
            "temperatures": [0.07, 0.08],
        },
    }

    setup_temperatures(config)

    assert config["simulation"]["temperature_count"] == 2
    assert config["simulation"]["temperature_exchange"] is False
    assert config["current"]["base_size"] == 2
    assert config["current"]["size"] == 4
    assert config["current"]["active"] == [0, 1, 2, 3]


def test_setup_temperatures_accepts_nve_exchange() -> None:
    """NVE temperature exchange is opt-in for distinct temperatures."""
    config = {
        "current": {
            "active": [0, 1],
            "frac": {},
            "locked": [],
            "size": 2,
            "traj_num": 2,
        },
        "simulation": {
            "interfaces": [0.0, 1.0],
            "temperature_exchange": "nve",
            "temperatures": [0.07, 0.08],
        },
    }

    setup_temperatures(config)

    assert config["simulation"]["temperature_exchange"] == "nve"


def set_nested_value(d, keys, value):
    """Set a value in a nested dictionary by following the list of keys,
    creating keys if they don't exist."""
    for key in keys[:-1]:
        if key not in d or not isinstance(d[key], dict):
            d[key] = {}  # Create a new dict if the key doesn't exist
        d = d[key]
    d[keys[-1]] = value


def test_check_config():
    toml_path = (
        Path(__file__).parent / "../../examples/gromacs/H2/infretis.toml"
    )
    original_config = setup_config(toml_path)
    test_cases = [
        (["runner", "workers"], 100),
        (["simulation", "tis_set", "interface_cap"], 100),
        (["simulation", "interfaces"], [0.0, 0.5, 0.2, 1.0]),
        (["simulation", "interfaces"], [0.0, 0.2, 0.2, 1.0]),
        (["simulation", "interfaces"], []),
    ]
    for keys, invalid_value in test_cases:
        config = copy.deepcopy(original_config)
        set_nested_value(config, keys, invalid_value)
        print("Testing:", keys, invalid_value)
        with pytest.raises(TOMLConfigError):
            check_config(config)


def test_multi_engine_config():
    toml_path = (
        Path(__file__).parent / "../../examples/gromacs/H2/infretis.toml"
    )
    original_config = setup_config(toml_path)
    original_config["simulation"]["tis_set"]["multi_engine"] = True
    original_config["engine0"] = original_config["engine"].copy()
    original_config["engine1"] = original_config["engine"].copy()
    original_config["engine2"] = original_config.pop("engine")
    test_cases = [
        (["engine2", "timestep"], 10.0),
    ]
    for keys, invalid_value in test_cases:
        config = copy.deepcopy(original_config)
        set_nested_value(config, keys, invalid_value)
        print("Testing:", keys, invalid_value)
        with pytest.raises(TOMLConfigError):
            check_config(config)
