import copy
from pathlib import PosixPath

import tomli

from infretis.classes.repex import REPEX_state, spawn_rng


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
