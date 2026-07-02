import copy
from pathlib import PosixPath
from types import SimpleNamespace

import numpy as np
import pytest
import tomli

from infretis.classes.repex import REPEX_state, spawn_rng, write_to_pathens


def test_rgen_io(tmp_path: PosixPath, monkeypatch) -> None:
    """Test repex rgen and rgen spawn reproducability."""
    state = REPEX_state(repex_config())
    folder = tmp_path / "temp"
    folder.mkdir()
    monkeypatch.chdir(folder)

    # save initial state for restart
    state.write_toml()

    # generate numbers
    save_rng = []
    save_rng_child = []
    for i in range(5):
        save_rng.append(state.rgen.random())
        child = spawn_rng(state.rgen)
        save_rng_child.append(child.random())

    # restart with the "restarted_from" keyword
    with open("restart.toml", mode="rb") as f:
        config = tomli.load(f)
        config["current"]["restarted_from"] = {}
    state = REPEX_state(config)

    # test that the numbers are the same
    for rng, child_rng in zip(save_rng, save_rng_child):
        assert state.rgen.random() == rng
        child = spawn_rng(state.rgen)
        assert child.random() == child_rng


def test_repex_state_uses_instance_data() -> None:
    """REPEX states should not share mutable path state."""
    state1 = REPEX_state(copy.deepcopy(repex_config()))
    state2 = REPEX_state(copy.deepcopy(repex_config()))

    state1.traj_data[0] = {"length": 1}

    assert state2.traj_data == {}
    assert state1.pstore is not state2.pstore


def test_temperature_blocks_do_not_exchange() -> None:
    """Distinct temperature layers use independent swap blocks."""
    config = repex_config()
    config["current"]["size"] = 4
    config["simulation"]["temperature_count"] = 2
    config["simulation"]["temperature_exchange"] = False
    state = REPEX_state(config, minus=True)

    matrix = state.state.copy()
    matrix[0, 0] = 1
    matrix[1, 1] = 1
    matrix[2, 0] = 1
    matrix[2, 2] = 1
    matrix[3, 1] = 1
    matrix[3, 3] = 1
    locks = state._locks.copy()
    locks[:4] = 0

    prob = state.inf_retis(matrix, locks)

    assert prob[:4, :4].sum(axis=1).tolist() == [1, 1, 1, 1]
    assert prob[0, 1] == 0
    assert prob[1, 0] == 0
    assert prob[2, 3] == 0
    assert prob[3, 2] == 0


def test_nve_temperature_weight_uses_path_energy() -> None:
    """NVE exchange weights use exp(-beta H(path))."""
    config = repex_config()
    config["current"]["size"] = 2
    config["simulation"]["temperature_count"] = 2
    config["simulation"]["temperature_exchange"] = "nve"
    config["simulation"]["temperatures"] = [100.0, 200.0]
    config["simulation"]["temperature_kb"] = 0.5
    state = REPEX_state(config, minus=True)
    path = SimpleNamespace(
        phasepoints=[
            SimpleNamespace(etot=1.0),
            SimpleNamespace(etot=3.0),
        ]
    )

    assert state.path_energy(path) == 2.0
    assert state.temperature_weight(path, 0) == pytest.approx(
        0.9607894391523232
    )
    assert state.temperature_weight(path, 1) == pytest.approx(
        0.9801986733067553
    )


def test_nve_output_weights_omit_temperature_factor() -> None:
    """WHAM output weights remain base interface weights."""
    config = repex_config()
    config["current"]["size"] = 4
    config["simulation"]["temperature_count"] = 2
    config["simulation"]["temperature_exchange"] = "nve"
    config["simulation"]["temperatures"] = [100.0, 200.0]
    config["simulation"]["temperature_kb"] = 0.5
    config["simulation"]["interfaces"] = [0.0, 1.0]
    config["simulation"]["shooting_moves"] = ["sh", "sh"]
    config["simulation"]["tis_set"] = {"lambda_minus_one": False}
    state = REPEX_state(config, minus=True)
    path = SimpleNamespace(
        ordermax=(2.0, 0),
        phasepoints=[
            SimpleNamespace(etot=1.0),
            SimpleNamespace(etot=3.0),
        ],
    )

    swap_weights = state.path_weights(path, 0)
    output_weights = state.output_path_weights(path, 0)

    assert swap_weights[:2] == pytest.approx(
        [0.9607894391523232, 0.9801986733067553]
    )
    assert output_weights[:2] == (1.0, 1.0)


def test_temperature_output_writes_shared_paths_to_each_file(
    tmp_path: PosixPath,
) -> None:
    """A path assigned to both temperatures is written to both data files."""
    files = [tmp_path / "t0.txt", tmp_path / "t1.txt"]
    for file in files:
        file.write_text("", encoding="utf-8")
    state = SimpleNamespace(
        n=5,
        _offset=2,
        base_size=2,
        temperature_count=2,
        data_file=str(files[0]),
        config={
            "current": {"temperature_index": 0},
            "output": {"data_files": [str(file) for file in files]},
        },
        traj_data={
            7: {
                "ens_save_idx": 0,
                "frac": np.array([0.25, 0.0, 0.0, 0.75, 0.0]),
                "length": 12,
                "max_op": (1.5, 0),
                "output_weights": (1.0, 1.0, 1.0, 1.0, 0.0),
                "weights": (2.0, 2.0, 2.0, 2.0, 0.0),
            }
        },
    )

    write_to_pathens(state, [7])

    assert "\t  7\t" in files[0].read_text(encoding="utf-8")
    assert "\t  7\t" in files[1].read_text(encoding="utf-8")
    assert state.traj_data == {}


def test_ragged_temperature_interface_slots() -> None:
    """Per-temperature interface lists create a ragged 2D state."""
    config = repex_config()
    config["current"]["size"] = 7
    config["current"]["base_size"] = 4
    config["simulation"]["temperature_count"] = 2
    config["simulation"]["temperature_exchange"] = False
    config["simulation"]["interfaces"] = [0.0, 0.5, 1.0]
    config["simulation"]["interfaces_by_temperature"] = [
        [0.0, 0.5, 1.0],
        [0.0, 0.25, 0.5, 1.0],
    ]
    state = REPEX_state(config, minus=True)

    labels = [state.state_label(idx) for idx in range(state.n - 1)]

    assert labels == [
        "000_t0",
        "000_t1",
        "001_t0",
        "001_t1",
        "002_t0",
        "002_t1",
        "003_t1",
    ]
    assert state.external_slot(3, 1) == 4


def test_ragged_temperature_output_columns(tmp_path: PosixPath) -> None:
    """Per-temperature output files use their layer's own columns."""
    files = [tmp_path / "t0.txt", tmp_path / "t1.txt"]
    for file in files:
        file.write_text("", encoding="utf-8")
    state = SimpleNamespace(
        n=8,
        _offset=2,
        base_size=4,
        temperature_count=2,
        _slot_to_info=[
            (0, 0),
            (0, 1),
            (1, 0),
            (1, 1),
            (2, 0),
            (2, 1),
            (3, 1),
        ],
        data_file=str(files[0]),
        config={
            "current": {"temperature_index": 0},
            "output": {"data_files": [str(file) for file in files]},
        },
        traj_data={
            7: {
                "ens_save_idx": 0,
                "frac": np.array([0.1, 0.0, 0.2, 0.0, 0.3, 0.0, 0.0, 0.0]),
                "length": 12,
                "max_op": (1.5, 0),
                "output_weights": (1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
                "weights": (1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            },
            8: {
                "ens_save_idx": 1,
                "frac": np.array([0.0, 0.1, 0.0, 0.2, 0.0, 0.3, 0.4, 0.0]),
                "length": 12,
                "max_op": (1.5, 0),
                "output_weights": (1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
                "weights": (1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            },
        },
    )

    write_to_pathens(state, [7, 8])

    t0_text = files[0].read_text(encoding="utf-8")
    t1_text = files[1].read_text(encoding="utf-8")
    assert "\t  7\t" in t0_text
    assert "\t  8\t" not in t0_text
    assert "\t  7\t" not in t1_text
    assert "\t  8\t" in t1_text


def test_random_matching_prob_is_doubly_stochastic() -> None:
    """Sampled matching probabilities preserve row/column sums."""
    config = repex_config()
    config["current"]["size"] = 6
    state = REPEX_state(config)
    arr = np.array(
        [
            [1.0, 2.0, 0.0, 0.0, 0.0, 0.0],
            [3.0, 1.0, 2.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 3.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 2.0, 2.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 1.0, 2.0, 3.0],
            [0.0, 0.0, 0.0, 0.0, 1.0, 2.0],
        ]
    )

    prob = state.random_matching_prob(arr, n=200)

    assert prob.shape == arr.shape
    assert np.allclose(prob.sum(axis=0), 1)
    assert np.allclose(prob.sum(axis=1), 1)
    assert np.all(prob[arr == 0] == 0)


def test_random_prob_is_doubly_stochastic() -> None:
    """Adjacent sampled probabilities preserve row/column sums."""
    config = repex_config()
    config["current"]["size"] = 6
    state = REPEX_state(config)
    arr = np.tril(np.ones((6, 6)))

    prob = state.random_prob(arr, n=200)

    assert prob.shape == arr.shape
    assert np.allclose(prob.sum(axis=0), 1)
    assert np.allclose(prob.sum(axis=1), 1)


def repex_config():
    """Small REPEX config."""
    return {
        "current": {"size": 1, "cstep": 0},
        "runner": {"workers": 1},
        "simulation": {
            "seed": 0,
            "steps": 10,
            "zeroswap": 0.5,
            "pick_scheme": 0,
        },
    }
