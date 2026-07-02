"""Defines the main REPEX class for path handling and permanent calc."""

import logging
import os
import time
from datetime import datetime

import numpy as np
import tomli_w
from numpy.random import default_rng

try:
    from numba import njit
except ImportError:  # pragma: no cover - optional acceleration
    njit = None

from infretis.classes.engines.factory import assign_engines
from infretis.classes.formatter import PathStorage
from infretis.core.core import make_dirs
from infretis.core.tis import calc_cv_vector

logger = logging.getLogger("main")  # pylint: disable=invalid-name
logger.addHandler(logging.NullHandler())
DATE_FORMAT = "%Y.%m.%d %H:%M:%S"


if njit is not None:

    @njit(cache=True)
    def _random_matching_counts(arr, assignment, row0s, row1s, rands):
        """Sample matching counts for a W matrix."""
        nsamples = len(row0s)
        size = len(assignment)
        out = np.zeros((size, size), dtype=np.float64)

        for sample in range(nsamples):
            row0 = row0s[sample]
            row1 = row1s[sample]
            col0 = assignment[row0]
            col1 = assignment[row1]
            old = arr[row0, col0] * arr[row1, col1]
            new = arr[row0, col1] * arr[row1, col0]
            if new > 0.0 and (new >= old or rands[sample] < new / old):
                assignment[row0] = col1
                assignment[row1] = col0

            for row in range(size):
                out[row, assignment[row]] += 1.0

        return out

    @njit(cache=True)
    def _random_adjacent_counts(
        prob_left, prob_right, directions, starts, rands
    ):
        """Sample adjacent swap counts for a block W matrix."""
        nsamples = len(directions)
        size = len(prob_left)
        choices = rands.shape[1]
        row_for_col = np.arange(size)
        out = np.eye(size, dtype=np.float64)

        for sample in range(nsamples):
            direction = directions[sample]
            start = starts[sample]

            for choice in range(choices):
                idx = choice * 2 + start
                if direction == -1:
                    if idx >= size - 1:
                        break
                    other = idx - 1
                    if other < 0:
                        other = size - 1
                    prob = (
                        prob_left[row_for_col[idx], idx]
                        * prob_right[row_for_col[other], other]
                    )
                else:
                    other = idx + 1
                    if other >= size:
                        break
                    prob = (
                        prob_right[row_for_col[idx], idx]
                        * prob_left[row_for_col[other], other]
                    )
                if rands[sample, choice] < prob:
                    row0 = row_for_col[idx]
                    row_for_col[idx] = row_for_col[other]
                    row_for_col[other] = row0

            for col in range(size):
                out[row_for_col[col], col] += 1.0

        return out

    @njit(cache=True)
    def _fast_glynn_perm64(M):
        """Float64 Glynn permanent for larger exact swap subblocks."""
        row_comb = np.zeros(len(M), dtype=np.float64)
        for row in range(len(M)):
            for col in range(len(M)):
                row_comb[col] += M[row, col]

        total = 0.0
        old_grey = 0
        sign = 1.0
        num_loops = 2 ** (len(M) - 1)

        for bin_index in range(1, num_loops + 1):
            prod = 1.0
            for value in row_comb:
                prod *= value
            total += sign * prod

            new_grey = bin_index ^ (bin_index // 2)
            grey_diff = old_grey ^ new_grey
            grey_diff_index = 0
            while 2**grey_diff_index != grey_diff:
                grey_diff_index += 1
            direction = 2.0 if old_grey > new_grey else -2.0
            for col in range(len(M)):
                row_comb[col] += M[grey_diff_index, col] * direction

            sign = -sign
            old_grey = new_grey

        return total / num_loops

else:
    _random_matching_counts = None
    _random_adjacent_counts = None
    _fast_glynn_perm64 = None


def spawn_rng(rgen):
    """
    Reimplementation of np.random.Generator.spawn() for numpy <= 1.24.4.

    Spawns a new random number generator (RNG) from an existing RNG.

    This function creates a new instance of the same type of RNG as the input
    RNG, using a seed generated from the input RNG's bit generator.

    Parameters:
    rgen (np.random.Generator): The input random number generator.

    Returns:
    np.random.Generator: A new random number generator instance.
    """
    return type(rgen)(
        type(rgen.bit_generator)(seed=rgen.bit_generator._seed_seq.spawn(1)[0])
    )


class REPEX_state:
    """Define the REPEX object."""

    # dicts to hold *toml, path data, ensembles and engine pointers.
    config: dict = {}
    traj_data: dict = {}
    ensembles: dict = {}
    engine_occ: dict = {}

    # holds counts current worker.
    cworker = None

    # defines storage object.
    pstore = PathStorage()

    def __init__(self, config, minus=False):
        """Initiate REPEX given confic dict from *toml file."""
        self.config = config
        self.traj_data = {}
        self.ensembles = {}
        self.engine_occ = {}
        self.pstore = PathStorage()
        # storage of additional trajectory files
        self.pstore.keep_traj_fnames = config.get("output", {}).get(
            "keep_traj_fnames", []
        )
        # set rng
        if "restarted_from" in config["current"]:
            self.set_rgen()
        else:
            self.rgen = default_rng(seed=config["simulation"]["seed"])

        n = config["current"]["size"]
        self.temperature_count = config["simulation"].get(
            "temperature_count", 1
        )
        self.temperature_exchange = config["simulation"].get(
            "temperature_exchange", True
        )
        self.temperature_interfaces = config["simulation"].get(
            "interfaces_by_temperature"
        )
        self.base_size = config["current"].get("base_size", n)
        self._slot_to_info = None
        self._info_to_slot = None
        if minus:
            self._offset = self.temperature_count
            n += 1 if self.temperature_count > 1 else int(minus)
        else:
            self._offset = 0

        self.n = n
        self.setup_temperature_slots()
        self.state = np.zeros(shape=(n, n))
        self._locks = np.ones(shape=n)
        self._last_prob = None
        self._random_count = 0
        self._trajs = [""] * n
        self.zeroswap = config["simulation"]["zeroswap"]
        self.pick_scheme = config["simulation"]["pick_scheme"]
        self.random_swap_samples = config["simulation"].get(
            "random_swap_samples", 10_000
        )

        # detect any locked ens-path pairs exist pre start
        self.locked0 = list(self.config["current"].get("locked", []))
        self.locked = []

        # determines the number of initiation loops to do.
        # either initiate all workers, or less if less steps left.
        stepsleft = self.tsteps - self.cstep
        self.toinitiate = min([self.workers, stepsleft])

        # keep track of olds in case of delete_old = True
        self.pn_olds = {}

    @property
    def prob(self):
        """Calculate the P matrix."""
        if self._last_prob is None:
            prob = self.inf_retis(abs(self.state), self._locks)
            self._last_prob = prob.copy()
        return self._last_prob

    @property
    def cstep(self):
        """Retrieve cstep from config dict."""
        return self.config["current"]["cstep"]

    @cstep.setter
    def cstep(self, val):
        """Iterate += cstep from val."""
        self.config["current"]["cstep"] = val

    @property
    def tsteps(self):
        """Retrieve total steps from config dict."""
        return self.config["simulation"]["steps"]

    @property
    def screen(self):
        """Retrieve screen print frequency from config dict."""
        return self.config["output"]["screen"]

    @property
    def mc_moves(self):
        """Retrieve mc moves list from config dict."""
        return self.config["simulation"]["shooting_moves"]

    @property
    def cap(self):
        """Retrieve mc moves list from config dict."""
        return self.config["simulation"]["tis_set"].get("interface_cap", None)

    @property
    def data_dir(self):
        """Retrieve data_dir from config dict."""
        return self.config["output"]["data_dir"]

    @property
    def data_file(self):
        """Retrieve data_file from config dict."""
        data_files = self.config["output"].get("data_files")
        if data_files is not None:
            idx = self.config["current"].get("temperature_index", 0)
            return data_files[idx]
        return self.config["output"]["data_file"]

    @property
    def interfaces(self):
        """Retrieve interfaces from config dict."""
        return self.config["simulation"]["interfaces"]

    @property
    def workers(self):
        """Retrieve workers from config dict."""
        return self.config["runner"]["workers"]

    @property
    def maxop(self):
        """Get the maximum orderparameter seen during the simulation."""
        maxop = self.config["current"].get("maxop", -float("inf"))
        return min(self.config["simulation"]["interfaces"][-1], maxop)

    @maxop.setter
    def maxop(self, val):
        """Update the maximum orderpameter seen during the sumulation."""
        if self.config["output"]["keep_maxop_trajs"]:
            self.config["current"]["maxop"] = min(
                val, self.config["simulation"]["interfaces"][-1]
            )

    def internal_ens(self, ens_num):
        """Convert an external ensemble number to a state index."""
        return int(ens_num + self._offset)

    def setup_temperature_slots(self):
        """Build state-index mappings for ragged temperature layers."""
        if self.temperature_count == 1 or self.temperature_interfaces is None:
            return
        slots = []
        for temperature in range(self.temperature_count):
            slots.append((0, temperature))
        self.base_size = max(
            len(layer) for layer in self.temperature_interfaces
        )
        for base_ensemble in range(1, self.base_size):
            for temperature, interfaces in enumerate(
                self.temperature_interfaces
            ):
                if base_ensemble < len(interfaces):
                    slots.append((base_ensemble, temperature))
        self._slot_to_info = slots
        self._info_to_slot = {
            (base_ensemble, temperature): idx
            for idx, (base_ensemble, temperature) in enumerate(slots)
        }

    def slot_info(self, internal):
        """Return ``(base_ensemble, temperature)`` for a state index."""
        internal = int(internal)
        if self.temperature_count == 1:
            return internal, 0
        if self._slot_to_info is not None:
            return self._slot_to_info[internal]
        if internal < self._offset:
            return 0, internal
        positive = internal - self._offset
        return positive // self.temperature_count + 1, (
            positive % self.temperature_count
        )

    def external_slot(self, base_ensemble, temperature):
        """Return the external ensemble number for a base/temp slot."""
        if self.temperature_count == 1:
            return base_ensemble - self._offset
        if self._info_to_slot is not None:
            return (
                self._info_to_slot[(base_ensemble, temperature)] - self._offset
            )
        if base_ensemble == 0:
            internal = temperature
        else:
            internal = (
                self._offset
                + (base_ensemble - 1) * self.temperature_count
                + temperature
            )
        return internal - self._offset

    def interfaces_for_temperature(self, temperature):
        """Return the interface list for a temperature layer."""
        if self.temperature_interfaces is None:
            return self.config["simulation"]["interfaces"]
        return self.temperature_interfaces[temperature]

    def zero_swap_partner(self, internal):
        """Return the paired 0-/0+ state index, if this is a zero slot."""
        base_ensemble, temperature = self.slot_info(internal)
        if base_ensemble == 0:
            return self.internal_ens(self.external_slot(1, temperature))
        if base_ensemble == 1:
            return self.internal_ens(self.external_slot(0, temperature))
        return None

    def temperature_beta(self, temperature):
        """Return beta for a configured temperature layer."""
        kb = self.config["simulation"].get("temperature_kb")
        if kb is None:
            engine_name = self.config.get("engine", {}).get("engine")
            class_name = self.config.get("engine", {}).get("class")
            if engine_name == "ase" or class_name == "ase":
                kb = 8.61733326e-5
            else:
                raise ValueError(
                    "Set simulation.temperature_kb when using "
                    "temperature_exchange = 'nve' for this engine."
                )
            self.config["simulation"]["temperature_kb"] = kb
        temp = self.config["simulation"]["temperatures"][temperature]
        return 1.0 / (float(kb) * temp)

    def path_energy(self, path):
        """Return the conserved energy used for NVE temperature exchange."""
        energies = [getattr(point, "etot", None) for point in path.phasepoints]
        energies = np.array(
            [energy for energy in energies if energy is not None],
            dtype=float,
        )
        energies = energies[np.isfinite(energies)]
        if energies.size == 0:
            raise ValueError(
                "temperature_exchange = 'nve' requires total energies "
                "on all exchangeable paths."
            )
        return float(np.mean(energies))

    def temperature_weight(self, path, temperature):
        """Return the temperature part of the path weight."""
        if self.temperature_exchange != "nve":
            return 1.0
        return float(
            np.exp(
                -self.temperature_beta(temperature) * self.path_energy(path)
            )
        )

    def base_path_weights(self, path, ens_num):
        """Calculate path/interface weights before temperature factors."""
        internal = self.internal_ens(ens_num)
        base_ensemble, temperature = self.slot_info(internal)
        minus = base_ensemble == 0
        return calc_cv_vector(
            path,
            self.interfaces_for_temperature(temperature),
            self.mc_moves,
            lambda_minus_one=self.config["simulation"]["tis_set"][
                "lambda_minus_one"
            ],
            cap=self.cap,
            minus=minus,
        )

    def expand_weights(self, path, weights, temperature, output=False):
        """Expand base TIS weights over temperature slots."""
        if self.temperature_count == 1:
            return weights
        expanded = [
            0.0 for _ in range((len(weights) - 1) * self.temperature_count)
        ]
        for base_idx, weight in enumerate(weights[:-1]):
            if output and self.temperature_exchange:
                for temp_idx in range(self.temperature_count):
                    expanded[
                        base_idx * self.temperature_count + temp_idx
                    ] = weight
            elif self.temperature_exchange == "nve":
                for temp_idx in range(self.temperature_count):
                    expanded[
                        base_idx * self.temperature_count + temp_idx
                    ] = weight * self.temperature_weight(path, temp_idx)
            elif self.temperature_exchange:
                for temp_idx in range(self.temperature_count):
                    expanded[
                        base_idx * self.temperature_count + temp_idx
                    ] = weight
            else:
                expanded[
                    base_idx * self.temperature_count + temperature
                ] = weight
        expanded.append(weights[-1])
        return tuple(expanded)

    def expand_temperature_interface_weights(
        self, path, source_temperature, output=False
    ):
        """Calculate weights for ragged per-temperature interface layers."""
        expanded = [0.0 for _ in range(self.n - self._offset)]
        if self.temperature_exchange:
            temperatures = range(self.temperature_count)
        else:
            temperatures = [source_temperature]

        terminal = 0.0
        for temperature in temperatures:
            weights = calc_cv_vector(
                path,
                self.interfaces_for_temperature(temperature),
                self.mc_moves,
                lambda_minus_one=self.config["simulation"]["tis_set"][
                    "lambda_minus_one"
                ],
                cap=self.cap,
                minus=False,
            )
            factor = (
                self.temperature_weight(path, temperature)
                if self.temperature_exchange == "nve" and not output
                else 1.0
            )
            for base_idx, weight in enumerate(weights[:-1]):
                base_ensemble = base_idx + 1
                internal = self._info_to_slot.get((base_ensemble, temperature))
                if internal is not None:
                    expanded[internal - self._offset] = weight * factor
            terminal = max(terminal, weights[-1])
        expanded[-1] = terminal
        return tuple(expanded)

    def path_weights(self, path, ens_num):
        """Calculate path weights for the combined state."""
        internal = self.internal_ens(ens_num)
        minus = self.slot_info(internal)[0] == 0
        weights = self.base_path_weights(path, ens_num)
        if not minus:
            temperature = self.slot_info(internal)[1]
            if self.temperature_interfaces is not None:
                return self.expand_temperature_interface_weights(
                    path, temperature
                )
            return self.expand_weights(path, weights, temperature)
        if self.temperature_count == 1:
            return weights
        if self.temperature_exchange:
            return tuple(
                [
                    weights[0] * self.temperature_weight(path, temp_idx)
                    for temp_idx in range(self.temperature_count)
                ]
            )
        temperature = self.slot_info(internal)[1]
        expanded = [0.0 for _ in range(self.temperature_count)]
        expanded[temperature] = weights[0]
        return tuple(expanded)

    def output_path_weights(self, path, ens_num):
        """Calculate base TIS weights for per-temperature output files."""
        internal = self.internal_ens(ens_num)
        temperature = self.slot_info(internal)[1]
        weights = self.base_path_weights(path, ens_num)
        minus = self.slot_info(internal)[0] == 0
        if self.temperature_count == 1:
            return weights
        if not minus:
            if self.temperature_interfaces is not None:
                return self.expand_temperature_interface_weights(
                    path, temperature, output=True
                )
            return self.expand_weights(path, weights, temperature, output=True)
        if self.temperature_exchange:
            return tuple([weights[0]] * self.temperature_count)
        expanded = [0.0 for _ in range(self.temperature_count)]
        expanded[temperature] = weights[0]
        return tuple(expanded)

    def pick(self):
        """Pick path and ens."""
        prob = self.prob.astype("float64")
        if self.pick_scheme > 0:
            # Pick ensemble based on weight, primarily only necessary
            # for inf-init simulations.
            valid_idx = np.where(1.0 - self._locks)[0]
            ens_weights = np.zeros(self.n)
            ens_weights[valid_idx] = np.arange(1, len(valid_idx) + 1)
            prob *= ens_weights**self.pick_scheme

        prob = prob.flatten()
        p = self.rgen.choice(self.n**2, p=np.nan_to_num(prob / np.sum(prob)))
        traj, ens = np.divmod(p, self.n)

        self.swap(traj, ens)
        self.lock(ens)
        traj = self._trajs[ens]
        # If available do 0+- swap with 50% probability

        ens_nums = (ens - self._offset,)
        inp_trajs = (traj,)

        other = self.zero_swap_partner(ens)
        if (
            other is not None
            and not self._locks[other]
            and self.rgen.random() < self.zeroswap
        ):
            other_traj = self.pick_traj_ens(other)
            minus = ens if ens < self._offset else other
            plus = other if ens < self._offset else ens
            ens_nums = (minus - self._offset, plus - self._offset)
            inp_trajs = (
                traj if ens == minus else other_traj,
                other_traj if ens == minus else traj,
            )

        # lock and print the picked traj and ens
        pat_nums = [str(i.path_number) for i in inp_trajs]
        self.locked.append((list(ens_nums), pat_nums))
        if self.printing():
            self.print_pick(ens_nums, pat_nums, self.cworker)
        picked = {}

        child_rng = spawn_rng(self.rgen)
        for ens_num, inp_traj in zip(ens_nums, inp_trajs):
            ens_pick = self.ensembles[self.internal_ens(ens_num)]
            ens_pick["rgen"] = spawn_rng(child_rng)
            picked[ens_num] = {
                "ens": ens_pick,
                "traj": inp_traj,
                "pn_old": inp_traj.path_number,
            }
        return picked

    def pick_traj_ens(self, ens):
        """Pick traj ens."""
        prob = self.prob.astype("float64")[:, ens].flatten()
        traj = self.rgen.choice(self.n, p=np.nan_to_num(prob / np.sum(prob)))
        self.swap(traj, ens)
        self.lock(ens)
        return self._trajs[ens]

    def pick_lock(self):
        """Pick path and ens.

        In case a crash, we pick lock locked from previous simulation.
        """
        if not self.locked0:
            if "restarted_from" in self.config["current"]:
                # get the same pick() as pre-restart. Need to set it again
                # because current self.rgen was used for calculating self.prob.
                self.set_rgen()
            return self.pick()

        enss = []
        trajs = []
        enss0, trajs0 = self.locked0.pop(0)
        logger.info("pick locked!")
        for ens, traj in zip(enss0, trajs0):
            enss.append(ens - self._offset)
            traj_idx = self.live_paths().index(int(traj))
            self.swap(traj_idx, ens)
            self.lock(ens)
            trajs.append(self._trajs[ens])
        if self.printing():
            self.print_pick(tuple(enss), tuple(trajs0), self.cworker)
        picked = {}

        child_rng = spawn_rng(self.rgen)
        for ens_num, inp_traj in zip(enss, trajs):
            ens_pick = self.ensembles[self.internal_ens(ens_num)]
            ens_pick["rgen"] = spawn_rng(child_rng)
            picked[ens_num] = {
                "ens": ens_pick,
                "traj": inp_traj,
                "pn_old": inp_traj.path_number,
            }
        return picked

    def prep_md_items(self, md_items):
        """Fill md_items with picked path and ens."""
        # Remove previous picked
        md_items.pop("picked", None)

        # pick/lock ens & path
        if self.toinitiate >= 0:
            # assign pin
            md_items.update({"pin": self.cworker})

            # pick lock
            md_items["picked"] = self.pick_lock()

            ens0 = next(iter(md_items["picked"]))
            temp = self.slot_info(self.internal_ens(ens0))[1]
            md_items["temperature_index"] = temp
            if self.temperature_count > 1:
                self.config["current"]["temperature_index"] = temp

        else:
            md_items["picked"] = self.pick()
            ens0 = next(iter(md_items["picked"]))
            temp = self.slot_info(self.internal_ens(ens0))[1]
            md_items["temperature_index"] = temp
            if self.temperature_count > 1:
                self.config["current"]["temperature_index"] = temp

        w_folder = os.path.join(os.getcwd(), f"worker{md_items['pin']}")
        make_dirs(w_folder)
        md_items["w_folder"] = w_folder
        temperatures = self.config["simulation"].get("temperatures")
        if temperatures is not None:
            md_items["temperature"] = temperatures[temp]

        # Record ens_nums
        md_items["ens_nums"] = list(md_items["picked"].keys())

        # allocate worker pin:
        ens_engs = self.config["simulation"]["ensemble_engines"]
        eng_names = []
        for ens_num in md_items["ens_nums"]:
            md_items["picked"][ens_num]["exe_dir"] = md_items["w_folder"]
            if "temperature" in md_items:
                md_items["picked"][ens_num]["temperature"] = md_items[
                    "temperature"
                ]
            if self.config["runner"].get("wmdrun", False):
                md_items["picked"][ens_num]["wmdrun"] = self.config["runner"][
                    "wmdrun"
                ][md_items["pin"]]
            # spawn rgen for all engines
            ens_rgen = md_items["picked"][ens_num]["ens"]["rgen"]
            md_items["picked"][ens_num]["rgen-eng"] = spawn_rng(ens_rgen)
            md_items["picked"][ens_num]["pin"] = md_items["pin"]
            eng_names += ens_engs[self.internal_ens(ens_num)]

        # engine assignment
        unique_eng_names = list(set(eng_names))
        eng_idx = assign_engines(
            self.engine_occ, unique_eng_names, md_items["pin"]
        )
        for ens_num in md_items["ens_nums"]:
            md_items["picked"][ens_num]["eng_idx"] = {
                eng: eng_idx[eng]
                for eng in ens_engs[self.internal_ens(ens_num)]
            }

        # check time:
        md_items["md_start"] = time.time()

        # record pnum_old
        md_items["pnum_old"] = []
        for key in md_items["picked"].keys():
            pnum_old = md_items["picked"][key]["traj"].path_number
            md_items["pnum_old"].append(pnum_old)

        # empty / update md_items:
        for key in ["moves", "trial_len", "trial_op", "generated"]:
            md_items[key] = []

        return md_items

    def add_traj(self, ens, traj, valid, count=True, n=0):
        """Add traj to state and calculate P matrix."""
        if ens >= 0 and self._offset != 0:
            valid = tuple([0 for _ in range(self._offset)] + list(valid))
        elif ens < 0:
            if self._offset > 1:
                valid_full = list(valid) + [
                    0 for _ in range(self.n - self._offset)
                ]
                valid = tuple(valid_full)
            else:
                valid = tuple(
                    list(valid) + [0 for _ in range(self.n - self._offset)]
                )
        ens = self.internal_ens(ens)

        if valid[ens] == 0:
            # The path is not valid in ensemble.
            # This situation should only occur in the initial path loading.
            raise_msg = (
                f"Path {traj.path_number} lying in {traj.adress} "
                f"is not valid in ensemble {ens:03.0f}!\n"
            )
            cap = self.cap if self.cap is not None else self.interfaces[-1]
            if ens > 0:
                ens_interfaces = self.ensembles[ens]["interfaces"]
                raise_msg += (
                    f"Path {traj.path_number} has max_op {traj.ordermax[0]}"
                    f" and does not have any phase points "
                    f"between {ens_interfaces[1]} and {cap}.\n"
                )

            raise ValueError(raise_msg)

        # invalidate last prob
        self._last_prob = None
        self._trajs[ens] = traj
        self.state[ens, :] = valid
        self.unlock(ens)

        # Calculate P matrix
        if count:
            self.prob

    def sort_trajstate(self):
        """Sort trajs and calculate P matrix."""
        if self.temperature_count > 1:
            self._last_prob = None
            self.prob
            return

        if np.any(self._locks[:-1] == 1):
            self._last_prob = None
            self.prob
            return

        def needs_move():
            return [
                self._locks[idx] == 0 and self.state[idx][:-1][idx] == 0
                for idx in range(self.n - 1)
            ]

        needstomove = needs_move()
        while True in needstomove and self.toinitiate == -1:
            ens_idx = list(needstomove).index(True)
            locks = self.locked_paths()
            zero_idx = list(self.state[ens_idx][1:-1]).index(0) + 1
            avail = [1 if i != 0 else 0 for i in self.state[:, zero_idx]]
            avail = [
                j if self._trajs[i].path_number not in locks else 0
                for i, j in enumerate(avail[:-1])
            ]
            trj_idx = avail.index(1)
            self.swap(ens_idx, trj_idx)
            needstomove = needs_move()
        self._last_prob = None
        self.prob

    def lock(self, ens):
        """Lock ensemble."""
        # invalidate last prob
        self._last_prob = None
        assert self._locks[ens] == 0
        self._locks[ens] = 1

    def unlock(self, ens):
        """Unlock ensemble."""
        # invalidate last prob
        self._last_prob = None
        assert self._locks[ens] == 1
        self._locks[ens] = 0

    def swap(self, traj, ens):
        """Swap to keep the locks symmetric."""
        # mainly to keep the locks symmetric
        self.state[[ens, traj]] = self.state[[traj, ens]].copy()
        temp1 = self._trajs[ens]
        self._trajs[ens] = self._trajs[traj]
        self._trajs[traj] = temp1

    def live_paths(self):
        """Return list of live paths."""
        return [traj.path_number for traj in self._trajs[:-1]]

    def locked_paths(self):
        """Return list of locked paths."""
        locks = [
            t0.path_number
            for t0, l0 in zip(self._trajs[:-1], self._locks[:-1])
            if l0
        ]
        return locks

    def set_rgen(self):
        """Set numpy random generator state from restart."""
        seed_sequence = np.random.SeedSequence(
            entropy=0, n_children_spawned=self.cstep
        )
        self.rgen = default_rng(seed_sequence)
        self.rgen.bit_generator.state = self.config["current"]["rng_state"]

    def loop(self):
        """Check and iterate loop."""
        if self.printing():
            if self.cstep not in (
                0,
                self.config["current"].get("restarted_from", 0),
            ):
                logger.info("date: " + datetime.now().strftime(DATE_FORMAT))
                logger.info(
                    f"------- infinity {self.cstep:5.0f} END -------" + "\n"
                )

        if self.cstep >= self.tsteps:
            # should probably add a check for stopping when all workers
            # are free to close the while loop, but for now when
            # cstep >= tsteps we return false.
            self.print_end()
            self.write_toml()
            logger.info("date: " + datetime.now().strftime(DATE_FORMAT))
            return False

        self.cstep += 1

        if self.printing() and self.cstep <= self.tsteps:
            logger.info(f"------- infinity {self.cstep:5.0f} START -------")
            logger.info("date: " + datetime.now().strftime(DATE_FORMAT))

        return self.cstep <= self.tsteps

    def initiate(self):
        """Initiate loop."""
        if not self.cstep < self.tsteps:
            return False

        self.cworker = self.workers - self.toinitiate

        if self.toinitiate == self.workers:
            if self.screen > 0:
                self.print_start()
        if self.toinitiate < self.workers:
            if self.screen > 0:
                logger.info(
                    f"------- submit worker {self.cworker-1} END -------"
                    + datetime.now().strftime(DATE_FORMAT)
                    + "\n"
                )
        if self.toinitiate > 0:
            if self.screen > 0:
                logger.info(
                    f"------- submit worker {self.cworker} START -------"
                    + datetime.now().strftime(DATE_FORMAT)
                )
        self.toinitiate -= 1
        return self.toinitiate >= 0

    def inf_retis(self, input_mat, locks):
        """Permanent calculator."""
        if (
            self.temperature_interfaces is not None
            and self.temperature_exchange
        ):
            return self.inf_retis_general(input_mat, locks)
        if self.temperature_count > 1 and not self.temperature_exchange:
            return self.inf_retis_temperature_blocks(input_mat, locks)
        return self.inf_retis_block(input_mat, locks, self._offset)

    def inf_retis_general(self, input_mat, locks):
        """Permanent calculator for non-contiguous W matrices."""
        bool_locks = locks == 1
        insert_list = []
        i = 0
        for lock in bool_locks:
            if lock:
                insert_list.append(i)
            else:
                i += 1

        non_locked = input_mat[~bool_locks, :][:, ~bool_locks]
        if len(non_locked) == 0:
            out = non_locked
        elif len(non_locked) <= 8:
            out = self.permanent_prob(non_locked)
        else:
            self._random_count += 1
            out = self.random_matching_prob(
                non_locked, n=self.random_swap_samples
            )

        out[np.where(np.abs(out) < 1e-15)] = 0
        if np.sum(out < 0) > 0:
            out[out < 0] = 0

        final_out_rows = np.insert(out, insert_list, 0, axis=0)
        return np.insert(final_out_rows, insert_list, 0, axis=1)

    def inf_retis_temperature_blocks(self, input_mat, locks):
        """Calculate independent swap matrices for each temperature layer."""
        out = np.zeros_like(input_mat, dtype="longdouble")
        terminal = self.n - 1
        for temperature in range(self.temperature_count):
            indices = [
                idx
                for idx in range(terminal)
                if self.slot_info(idx)[1] == temperature
            ]
            if locks[terminal] == 0:
                indices.append(terminal)
            submat = input_mat[np.ix_(indices, indices)]
            sublocks = locks[indices]
            subprob = self.inf_retis_block(submat, sublocks, 1)
            out[np.ix_(indices, indices)] = subprob
        return out

    def inf_retis_block(self, input_mat, locks, offset):
        """Permanent calculator."""
        # Drop locked rows and columns
        bool_locks = locks == 1
        # get non_locked minus interfaces
        offset = offset - sum(bool_locks[:offset])
        # make insert list
        i = 0
        insert_list = []
        for lock in bool_locks:
            if lock:
                insert_list.append(i)
            else:
                i += 1

        # Drop locked rows and columns
        non_locked = input_mat[~bool_locks, :][:, ~bool_locks]

        # Sort based on the index of the last non-zero values in the rows
        # argmax(a>0) gives back the first column index that is nonzero
        # so looping over the columns backwards and multiplying by -1
        # gives the right ordering
        minus_idx = np.argsort(np.argmax(non_locked[:offset] > 0, axis=1))
        pos_idx = (
            np.argsort(-1 * np.argmax(non_locked[offset:, ::-1] > 0, axis=1))
            + offset
        )

        sort_idx = np.append(minus_idx, pos_idx)
        sorted_non_locked = non_locked[sort_idx]

        # check if all trajectories have equal weights
        sorted_non_locked_T = sorted_non_locked.T
        # Check the minus interfaces
        equal_minus = np.all(
            sorted_non_locked_T[
                np.where(
                    sorted_non_locked_T[:, :offset]
                    != sorted_non_locked_T[offset - 1, :offset]
                )
            ]
            == 0
        )
        # check the positive interfaces
        if len(sorted_non_locked_T) <= offset:
            equal_pos = True
        else:
            equal_pos = np.all(
                sorted_non_locked_T[:, offset:][
                    np.where(
                        sorted_non_locked_T[:, offset:]
                        != sorted_non_locked_T[offset, offset:]
                    )
                ]
                == 0
            )

        equal = equal_minus and equal_pos

        out = np.zeros(shape=sorted_non_locked.shape, dtype="longdouble")
        if equal:
            # All trajectories have equal weights, run fast algorithm
            # run_fast
            # minus move should be run backwards
            out[:offset, ::-1] = self.quick_prob(
                sorted_non_locked[:offset, ::-1]
            )
            if offset < len(out):
                # Catch only minus ens available
                out[offset:] = self.quick_prob(sorted_non_locked[offset:])
        else:
            # TODO DEBUG print
            # print("DEBUG this should not happen outside of wirefencing")
            blocks = self.find_blocks(sorted_non_locked, offset=offset)
            for start, stop, direction in blocks:
                if direction == -1:
                    cstart, cstop = stop - 1, start - 1
                    if cstop < 0:
                        cstop = None
                else:
                    cstart, cstop = start, stop
                subarr = sorted_non_locked[start:stop, cstart:cstop:direction]
                subarr_T = subarr.T
                if len(subarr) == 1:
                    out[start:stop, start:stop] = 1
                elif np.all(subarr_T[np.where(subarr_T != subarr_T[0])] == 0):
                    # Either the same weight as the last one or zero
                    temp = self.quick_prob(subarr)
                    out[start:stop, cstart:cstop:direction] = temp
                elif len(subarr) <= 12:
                    # We can run this subsecond
                    temp = self.permanent_prob(subarr)
                    out[start:stop, cstart:cstop:direction] = temp
                else:
                    self._random_count += 1
                    logger.debug(
                        f"random #{self._random_count}, "
                        f"dims = {len(subarr)}"
                    )
                    # do n random parallel samples
                    temp = self.random_prob(subarr, n=self.random_swap_samples)
                    out[start:stop, cstart:cstop:direction] = temp

        out[sort_idx] = out.copy()  # COPY REQUIRED TO NOT BRAKE STATE!!!

        # Make sure we have a valid probability square
        assert np.allclose(np.sum(out, axis=1), 1)
        assert np.allclose(np.sum(out, axis=0), 1)

        # edge case that negative probs exist: set to zero
        if np.sum(out < 0) > 0:
            out[out < 0] = 0
            logger.info(
                f"Found {int(np.sum(out<0))} precision \
                errors in the P-matrix, setting negative \
                elements to 0. min: {np.min(out):.3e}"
            )

        # reinsert zeroes for the locked ensembles
        final_out_rows = np.insert(out, insert_list, 0, axis=0)

        # reinsert zeroes for the locked trajectories
        final_out = np.insert(final_out_rows, insert_list, 0, axis=1)

        return final_out

    def find_blocks(self, arr, offset):
        """Find blocks in a W matrix."""
        if len(arr) == 1:
            return (0, 1, 1)
        # Assume no zeroes on the diagonal or lower triangle
        temp_arr = arr.copy()
        # for counting minus blocks
        temp_arr[:offset, :offset] = arr[:offset, :offset].T
        temp_arr[offset:, :offset] = 1  # add ones to the lower triangle
        non_zero = np.count_nonzero(temp_arr, axis=1)
        blocks = []
        start = 0
        for i, e in enumerate(non_zero):
            if e == i + 1:
                direction = -1 if start < offset else 1
                blocks.append((start, e, direction))
                start = e
        return blocks

    def quick_prob(self, arr):
        """Quick P matrix calculation for specific W matrix."""
        total_traj_prob = np.ones(shape=arr.shape[0], dtype="longdouble")
        out_mat = np.zeros(shape=arr.shape, dtype="longdouble")
        working_mat = np.where(arr != 0, 1, 0)  # convert non-zero numbers to 1

        for i, column in enumerate(working_mat.T[::-1]):
            ens = column * total_traj_prob
            s = ens.sum()
            if s != 0:
                ens /= s
            out_mat[:, -(i + 1)] = ens
            total_traj_prob -= ens
            # force negative values to 0
            total_traj_prob[np.where(total_traj_prob < 0)] = 0
        return out_mat

    def permanent_prob(self, arr):
        """P matrix calculation for specific W matrix."""
        out = np.zeros(shape=arr.shape, dtype="longdouble")
        # Don't overwrite input arr
        scaled_arr = arr.copy()
        n = len(scaled_arr)
        # Rescaling the W-matrix avoids numerical instabilites when the
        # matrix is large and contains large weights from
        # high-acceptance moves
        for i in range(n):
            scaled_arr[i, :] /= np.max(scaled_arr[i, :])
        for i in range(n):
            rows = [r for r in range(n) if r != i]
            sub_arr = scaled_arr[rows, :]
            for j in range(n):
                if scaled_arr[i][j] == 0:
                    continue
                columns = [r for r in range(n) if r != j]
                M = sub_arr[:, columns]
                if _fast_glynn_perm64 is not None and n > 8:
                    f = _fast_glynn_perm64(M.astype(np.float64))
                else:
                    f = self.fast_glynn_perm(M)
                out[i][j] = f * scaled_arr[i][j]
        return out / max(np.sum(out, axis=1))

    def initial_matching(self, arr):
        """Find one valid row-column matching for a sparse W matrix."""
        n = len(arr)
        row_for_col = [-1 for _ in range(n)]

        def assign(row, seen):
            for col in np.flatnonzero(arr[row]):
                if seen[col]:
                    continue
                seen[col] = True
                if row_for_col[col] == -1 or assign(row_for_col[col], seen):
                    row_for_col[col] = row
                    return True
            return False

        for row in range(n):
            if not assign(row, [False for _ in range(n)]):
                raise ValueError(
                    "No valid matching in replica-exchange matrix"
                )

        matching = np.zeros(n, dtype=int)
        for col, row in enumerate(row_for_col):
            matching[row] = col
        return matching

    def random_matching_prob(self, arr, n=10_000):
        """Sample matching marginals for a general W matrix."""
        size = len(arr)
        assignment = self.initial_matching(arr)

        if _random_matching_counts is not None and size > 1:
            row0s = self.rgen.integers(0, size, size=n, dtype=np.int64)
            row1s = self.rgen.integers(0, size - 1, size=n, dtype=np.int64)
            row1s += row1s >= row0s
            rands = self.rgen.random(n)
            out = _random_matching_counts(
                arr.astype(np.float64),
                assignment.astype(np.int64),
                row0s,
                row1s,
                rands,
            )
            return out.astype("longdouble") / n

        rows = np.arange(size)
        out = np.zeros(shape=arr.shape, dtype="longdouble")

        for _ in range(n):
            row0, row1 = self.rgen.choice(size, size=2, replace=False)
            col0, col1 = assignment[row0], assignment[row1]
            old = arr[row0, col0] * arr[row1, col1]
            new = arr[row0, col1] * arr[row1, col0]
            if new > 0 and (new >= old or self.rgen.random() < new / old):
                assignment[row0], assignment[row1] = col1, col0
            out[rows, assignment] += 1

        return out / n

    def random_prob(self, arr, n=10_000):
        """P matrix calculation for specific W matrix."""
        with np.errstate(divide="ignore", invalid="ignore"):
            prob_right = np.nan_to_num(np.roll(arr, -1, axis=1) / arr)
            prob_left = np.nan_to_num(np.roll(arr, 1, axis=1) / arr)

        choices = len(arr) // 2
        if _random_adjacent_counts is not None and choices > 0:
            directions = self.rgen.choice(
                np.array([1, -1], dtype=np.int64), size=n
            )
            even = choices * 2 == len(arr)
            if even:
                starts = np.zeros(n, dtype=np.int64)
            else:
                starts = self.rgen.integers(0, 2, size=n, dtype=np.int64)
            rands = self.rgen.random((n, choices))
            out = _random_adjacent_counts(
                prob_left.astype(np.float64),
                prob_right.astype(np.float64),
                directions,
                starts,
                rands,
            )
            return out.astype("longdouble") / (n + 1)

        out = np.eye(len(arr), dtype="longdouble")
        current_state = np.eye(len(arr))
        even = choices * 2 == len(arr)

        start = 0
        zero_one = np.array([0, 1])
        p_m = np.array([1, -1])
        temp = np.where(current_state == 1)

        for i in range(n):
            direction = self.rgen.choice(p_m)
            if not even:
                start = self.rgen.choice(zero_one)

            temp_left = prob_left[temp]
            temp_right = prob_right[temp]

            if not even:
                start = self.rgen.choice(zero_one)

            if direction == -1:
                probs = (
                    temp_left[start:-1:2]
                    * np.roll(temp_right, 1, axis=0)[start:-1:2]
                )
            else:
                probs = temp_right[start:-1:2] * temp_left[start + 1 :: 2]

            r_nums = self.rgen.random(choices)
            success = r_nums < probs

            for j in np.where(success)[0]:
                idx = j * 2 + start
                temp_state = current_state[:, [idx + direction, idx]]
                current_state[:, [idx, idx + direction]] = temp_state
                temp_state_2 = temp[0][[idx + direction, idx]]
                temp[0][[idx, idx + direction]] = temp_state_2

            out += current_state

        return out / (n + 1)

    def fast_glynn_perm(self, M):
        """Glynn permanent."""

        def cmp(a, b):
            if a == b:
                return 0
            elif a > b:
                return 1
            else:
                return -1

        row_comb = np.sum(M, axis=0, dtype="longdouble")
        n = len(M)

        total = 0
        old_grey = 0
        sign = +1

        binary_power_dict = {2**i: i for i in range(n)}
        num_loops = 2 ** (n - 1)

        for bin_index in range(1, num_loops + 1):
            total += sign * np.multiply.reduce(row_comb)

            new_grey = bin_index ^ (bin_index // 2)
            grey_diff = old_grey ^ new_grey
            grey_diff_index = binary_power_dict[grey_diff]
            direction = 2 * cmp(old_grey, new_grey)
            if direction:
                new_vector = M[grey_diff_index]
                row_comb += new_vector * direction

            sign = -sign
            old_grey = new_grey

        return total / num_loops

    def update_current(self):
        """Update restart state in the config."""
        self.config["current"]["active"] = [
            int(path_num) for path_num in self.live_paths()
        ]
        locked_ep = []
        for tup in self.locked:
            locked_ep.append(
                ([int(tup0 + self._offset) for tup0 in tup[0]], tup[1])
            )
        self.config["current"]["locked"] = locked_ep
        self.config["current"]["rng_state"] = self.rgen.bit_generator.state

        # save accumulative fracs
        self.config["current"]["frac"] = {}
        for key in sorted(self.traj_data.keys()):
            fracs = [str(i) for i in self.traj_data[key]["frac"]]
            self.config["current"]["frac"][str(key)] = fracs

    def write_toml(self):
        """Toml writer."""
        self.update_current()
        if self.config["current"].get("_skip_restart_write", False):
            return

        with open("./restart.toml", "wb") as f:
            tomli_w.dump(self.config, f)

    def printing(self):
        """Check if print."""
        return self.screen > 0 and np.mod(self.cstep, self.screen) == 0

    def print_pick(self, ens_nums, pat_nums, pin):
        """Print pick."""
        base_ensemble = self.slot_info(self.internal_ens(ens_nums[0]))[0]
        if len(ens_nums) > 1 or base_ensemble == 0:
            move = "sh"
        else:
            move = self.ensembles[self.internal_ens(ens_nums[0])]["mc_move"]
        ens_p = " ".join(
            [self.ensemble_label(ens_num) for ens_num in ens_nums]
        )
        pat_p = " ".join(pat_nums)
        logger.info(
            f"shooting {move} in ensembles: {ens_p} with paths:"
            f" {pat_p} and worker: {pin}"
        )

    def print_shooted(self, md_items, pn_news):
        """Print shooted."""
        moves = md_items["moves"]
        ens_nums = " ".join(
            [self.ensemble_label(i) for i in md_items["ens_nums"]]
        )
        pnum_old = " ".join([str(i) for i in md_items["pnum_old"]])
        pnum_new = " ".join([str(i) for i in pn_news])
        trial_lens = " ".join([str(i) for i in md_items["trial_len"]])
        trial_ops = " ".join(
            [f"[{i[0]:4.4f} {i[1]:4.4f}]" for i in md_items["trial_op"]]
        )
        status = md_items["status"]
        simtime = md_items["md_end"] - md_items["md_start"]
        subcycles = md_items["subcycles"]
        arrow = "=)" if status == "ACC" else "=("
        logger.info(
            f"shooted {' '.join(moves)} in ensembles: {ens_nums}"
            f" with paths: {pnum_old} {arrow} {pnum_new}"
        )
        logger.info(
            "with status:" f" {status} len: {trial_lens} op: {trial_ops} and"
        )
        logger.info(
            f"worker: {self.cworker} total time:"
            f"{simtime:.2f}s and subcycles: {subcycles}"
        )
        self.print_state()

    def print_start(self):
        """Print start."""
        if self.pick_scheme > 0:
            logger.info(
                f"ensemble selection scheme: {self.pick_scheme}"
                + " should only be used with Inf-init"
            )
        logger.info("stored ensemble paths:")
        ens_num = self.live_paths()
        logger.info(
            " ".join(
                [f"{self.state_label(i)}: {j}," for i, j in enumerate(ens_num)]
            )
            + "\n"
        )
        self.print_state()

    def ensemble_label(self, ens_num):
        """Format an external ensemble number."""
        return self.state_label(self.internal_ens(ens_num))

    def state_label(self, idx):
        """Format a state matrix ensemble number."""
        if self.temperature_count == 1:
            return f"{idx:03.0f}"
        base_ensemble, temperature = self.slot_info(idx)
        return f"{base_ensemble:03d}_t{temperature}"

    def print_state(self):
        """Print state."""
        last_prob = True
        if isinstance(self._last_prob, type(None)):
            self.prob
            last_prob = False

        logger.info("===")
        logger.info(" xx |\tv Ensemble numbers v")
        labels = [self.state_label(i) for i in range(self.n - 1)]
        width = max(len(label) for label in labels)
        for row in range(width):
            row_chars = [label.ljust(width)[row] for label in labels]
            suffix = "\t\tmax_op\tmin_op\tlen" if row == width - 1 else ""
            logger.info(" xx |\t" + " ".join(row_chars) + suffix)

        logger.info(" -- |\t" + "".join("--" for _ in range(self.n + 14)))

        locks = self.locked_paths()
        oil = False
        for idx, live in enumerate(self.live_paths()):
            if live not in locks:
                to_print = f"p{live:02.0f} |\t"
                if (
                    self.state[idx][:-1][idx] == 0
                    or self._last_prob[idx][:-1][idx] < 0.001
                ):
                    oil = True
                for prob in self._last_prob[idx][:-1]:
                    if prob == 1:
                        marker = "x "
                    elif prob == 0:
                        marker = "- "
                    else:
                        marker = f"{int(round(prob*10,1))} "
                        # change if marker == 10
                        if len(marker) == 3:
                            marker = "9 "
                    to_print += marker
                to_print += f"|\t{self.traj_data[live]['max_op'][0]:5.3f} \t"
                to_print += f"{self.traj_data[live]['min_op'][0]:5.3f} \t"
                to_print += f"{self.traj_data[live]['length']:5.0f}"
                logger.info(to_print)
            else:
                to_print = f"p{live:02.0f} |\t"
                logger.info(
                    to_print + "".join(["- " for j in range(self.n - 1)]) + "|"
                )
        if oil:
            logger.info("olive oil")
            oil = False

        logger.info("===")
        if not last_prob:
            self._last_prob = None

    def print_end(self):
        """Print end."""
        live_trajs = self.live_paths()
        stopping = self.cstep
        logger.info("--------------------------------------------------")
        logger.info(f"live trajs: {live_trajs} after {stopping} cycles")
        logger.info("==================================================")
        labels = "\t".join([self.state_label(i) for i in range(self.n - 1)])
        logger.info(f"xxx | {labels} |")
        logger.info("--------------------------------------------------")
        for key, item in self.traj_data.items():
            values = "\t".join(
                [
                    f"{item0:02.2f}" if item0 != 0.0 else "----"
                    for item0 in item["frac"][:-1]
                ]
            )
            logger.info(f"{key:03.0f} * {values} *")

    def treat_output(self, md_items):
        """Treat output."""
        pn_news = []
        md_items["md_end"] = time.time()
        picked = md_items["picked"]
        traj_num = self.config["current"]["traj_num"]

        for ens_num in picked.keys():
            pn_old = picked[ens_num]["pn_old"]
            out_traj = picked[ens_num]["traj"]
            self.ensembles[self.internal_ens(ens_num)] = picked[ens_num]["ens"]
            path_status = md_items["status"]

            for idx, lock in enumerate(self.locked):
                if str(pn_old) in lock[1]:
                    self.locked.pop(idx)
            # if path is new: number and save the path:
            if out_traj.path_number is None or path_status == "ACC":
                if path_status == "ACC":
                    out_traj.weights = self.path_weights(out_traj, ens_num)
                # keep track of the highest order value seen during the sim
                base_ensemble = self.slot_info(self.internal_ens(ens_num))[0]
                if base_ensemble != 0 and out_traj.ordermax[0] > self.maxop:
                    self.maxop = out_traj.ordermax[0]
                # move to accept:
                ens_save_idx = self.traj_data[pn_old]["ens_save_idx"]
                out_traj.path_number = traj_num
                data = {
                    "path": out_traj,
                    "dir": os.path.join(
                        os.getcwd(), self.config["simulation"]["load_dir"]
                    ),
                    "status": path_status,
                }
                out_traj = self.pstore.output(self.cstep, data)
                self.traj_data[traj_num] = {
                    "frac": np.zeros(self.n, dtype="longdouble"),
                    "max_op": out_traj.ordermax,
                    "min_op": out_traj.ordermin,
                    "length": out_traj.length,
                    "weights": out_traj.weights,
                    "output_weights": self.output_path_weights(
                        out_traj, ens_num
                    ),
                    "adress": out_traj.adress,
                    "ens_save_idx": ens_save_idx,
                }
                traj_num += 1
                if self.config["output"]["delete_old"] and pn_old > self.n - 2:
                    if len(self.pn_olds) > self.n - 2:
                        pn_old_del, del_dic = next(iter(self.pn_olds.items()))
                        load_dir = self.config["simulation"]["load_dir"]
                        if self.config["output"]["keep_maxop_trajs"]:
                            path_dir = os.path.join(load_dir, pn_old_del)
                            # delete trajectory files if low orderp (infinit)
                            # and directory is not a symlink
                            if del_dic["max_op"][
                                0
                            ] < self.maxop and not os.path.islink(path_dir):
                                # update maxop and then delete|
                                for adress in del_dic["adress"]:
                                    os.remove(adress)
                        else:
                            # delete trajectory files
                            for adress in del_dic["adress"]:
                                os.remove(adress)
                        # delete txt files
                        if self.config["output"]["delete_old_all"]:
                            for txt in ("order.txt", "traj.txt", "energy.txt"):
                                txt_adress = os.path.join(
                                    load_dir, pn_old_del, txt
                                )
                                if os.path.isfile(txt_adress):
                                    os.remove(txt_adress)
                            try:
                                os.rmdir(
                                    os.path.join(
                                        load_dir, pn_old_del, "accepted"
                                    )
                                )
                            except OSError:
                                continue
                            os.rmdir(os.path.join(load_dir, pn_old_del))
                        # pop the deleted path.
                        self.pn_olds.pop(pn_old_del)
                    # keep delete list:
                    if len(self.pn_olds) <= self.n - 2:
                        self.pn_olds[str(pn_old)] = {
                            "adress": self.traj_data[pn_old]["adress"],
                            "max_op": self.traj_data[pn_old]["max_op"],
                        }
            # store rejected paths if status match the ones we want to keep
            elif path_status in self.config["output"]["keep_status"]:
                rej_traj = picked[ens_num]["rej_traj"]
                rej_traj.path_number = pn_old
                data_rej = {
                    "path": rej_traj,
                    "dir": os.path.join(
                        os.getcwd(), self.config["simulation"]["load_dir"]
                    ),
                    "status": path_status,
                }
                rej_traj = self.pstore.output(self.cstep, data_rej)
                # remove rejected trajectory files if delete_old = True
                if self.config["output"]["delete_old"]:
                    for adress in rej_traj.adress:
                        os.remove(adress)

            pn_news.append(out_traj.path_number)
            self.add_traj(ens_num, out_traj, valid=out_traj.weights)

        # record weights
        locked_trajs = self.locked_paths()
        if self._last_prob is None:
            self.prob
        for idx, live in enumerate(self.live_paths()):
            if live not in locked_trajs:
                self.traj_data[live]["frac"] += self._last_prob[:-1][idx, :]

        # write succ data to infretis_data.txt
        if md_items["status"] == "ACC":
            if self.temperature_count > 1:
                self.config["current"]["temperature_index"] = md_items[
                    "temperature_index"
                ]
            write_to_pathens(self, md_items["pnum_old"])

        self.sort_trajstate()
        cdict = self.config["current"]
        cdict["traj_num"] = traj_num
        cdict["wsubcycles"][md_items["pin"]] += md_items["subcycles"]
        cdict["tsubcycles"] = int(sum(self.config["current"]["wsubcycles"]))
        self.cworker = md_items["pin"]
        if self.printing():
            self.print_shooted(md_items, pn_news)
        # save for possible restart
        self.write_toml()

        return md_items

    def load_paths(self, paths):
        """Load paths."""
        for internal, path in enumerate(paths):
            ens_num = internal - self._offset
            path.weights = self.path_weights(path, ens_num)
            self.add_traj(
                ens=ens_num,
                traj=path,
                valid=path.weights,
                count=False,
            )
            pnum = path.path_number
            frac = self.config["current"]["frac"].get(
                str(pnum), np.zeros(self.n)
            )
            self.traj_data[pnum] = {
                "ens_save_idx": internal,
                "max_op": path.ordermax,
                "min_op": path.ordermin,
                "length": path.length,
                "adress": path.adress,
                "weights": path.weights,
                "output_weights": self.output_path_weights(path, ens_num),
                "frac": np.array(frac, dtype="longdouble"),
            }
            if (
                self.slot_info(internal)[0] != 0
                and path.ordermax[0] > self.maxop
            ):
                self.maxop = path.ordermax[0]
        self.prob

    def initiate_ensembles(self):
        """Create all the ensemble dicts from the *toml config dict."""
        lambda_minus_one = self.config["simulation"]["tis_set"][
            "lambda_minus_one"
        ]

        def make_pensembles(intfs):
            ens_intfs = []
            if lambda_minus_one is not False:
                ens_intfs.append(
                    [
                        lambda_minus_one,
                        (lambda_minus_one + intfs[0]) / 2,
                        intfs[0],
                    ]
                )
            else:
                ens_intfs.append([float("-inf"), intfs[0], intfs[0]])
            ens_intfs.append([intfs[0], intfs[0], intfs[-1]])

            reactant, product = intfs[0], intfs[-1]
            for i in range(len(intfs) - 2):
                middle = intfs[i + 1]
                ens_intfs.append([reactant, middle, product])

            pensembles = {}
            for i, ens_intf in enumerate(ens_intfs):
                pensembles[i] = {
                    "interfaces": tuple(ens_intf),
                    "tis_set": self.config["simulation"]["tis_set"],
                    "mc_move": self.config["simulation"]["shooting_moves"][i],
                    "ens_name": f"{i:03d}",
                    "start_cond": (
                        ["L", "R"]
                        if lambda_minus_one is not False and i == 0
                        else ("R" if i == 0 else "L")
                    ),
                }
            return pensembles

        intfs = self.config["simulation"]["interfaces"]
        pensembles = make_pensembles(intfs)

        if self.temperature_count == 1:
            self.ensembles = pensembles
            return

        if self.temperature_interfaces is not None:
            by_temperature = [
                make_pensembles(intfs) for intfs in self.temperature_interfaces
            ]
            self.ensembles = {}
            for internal, (base_ensemble, temperature) in enumerate(
                self._slot_to_info
            ):
                ens = by_temperature[temperature][base_ensemble].copy()
                ens["ens_name"] = self.state_label(internal)
                self.ensembles[internal] = ens
            return

        expanded = {}
        for temperature in range(self.temperature_count):
            ens = pensembles[0].copy()
            ens["ens_name"] = self.state_label(temperature)
            expanded[temperature] = ens

        for base_ensemble in range(1, len(pensembles)):
            for temperature in range(self.temperature_count):
                internal = self.internal_ens(
                    self.external_slot(base_ensemble, temperature)
                )
                ens = pensembles[base_ensemble].copy()
                ens["ens_name"] = self.state_label(internal)
                expanded[internal] = ens

        self.ensembles = expanded


def write_to_pathens(state, pn_archive):
    """Write data to infretis_data.txt."""
    traj_data = state.traj_data
    size = state.n

    def path_columns(pn, temp_idx=None):
        weights = traj_data[pn].get("output_weights", traj_data[pn]["weights"])
        if len(weights) == size:
            full_weights = weights
        elif len(weights) == state._offset:
            full_weights = list(weights) + [
                0.0 for _ in range(size - state._offset)
            ]
        elif len(weights) == 1:
            full_weights = [0.0 for _ in range(size)]
            full_weights[traj_data[pn]["ens_save_idx"]] = weights[0]
        else:
            full_weights = [0.0 for _ in range(state._offset)]
            full_weights += list(weights)

        frac_values = traj_data[pn]["frac"][:-1]
        weight_values = full_weights[:-1]
        if temp_idx is not None:
            if getattr(state, "_slot_to_info", None) is not None:
                temp_columns = [
                    idx
                    for idx, slot_info in enumerate(state._slot_to_info)
                    if slot_info[1] == temp_idx
                ]
            else:
                temp_columns = [temp_idx]
                temp_columns += [
                    state._offset
                    + (base_ensemble - 1) * state.temperature_count
                    + temp_idx
                    for base_ensemble in range(1, state.base_size)
                ]
            frac_values = [frac_values[idx] for idx in temp_columns]
            weight_values = [weight_values[idx] for idx in temp_columns]
        return frac_values, weight_values

    def path_line(pn, temp_idx=None):
        frac_values, weight_values = path_columns(pn, temp_idx=temp_idx)
        string = ""
        string += f"\t{pn:3.0f}\t"
        string += f"{traj_data[pn]['length']:5.0f}" + "\t"
        string += f"{traj_data[pn]['max_op'][0]:8.5f}" + "\t"
        frac = []
        weight = []
        for w0, f0 in zip(weight_values, frac_values):
            frac.append("----" if f0 == 0.0 else str(f0))
            weight.append("----" if f0 == 0.0 else str(w0))
        return string + "\t".join(frac) + "\t" + "\t".join(weight) + "\t\n"

    data_files = state.config["output"].get("data_files")
    if state.temperature_count > 1 and data_files is not None:
        for pn in pn_archive:
            wrote = False
            for temp_idx, data_file in enumerate(data_files):
                frac_values, _ = path_columns(pn, temp_idx=temp_idx)
                if np.any(np.array(frac_values, dtype="longdouble") != 0.0):
                    with open(data_file, "a") as fp:
                        fp.write(path_line(pn, temp_idx=temp_idx))
                    wrote = True
            if not wrote:
                temp_idx = state.config["current"].get("temperature_index", 0)
                with open(data_files[temp_idx], "a") as fp:
                    fp.write(path_line(pn, temp_idx=temp_idx))
            traj_data.pop(pn)
        return

    with open(state.data_file, "a") as fp:
        for pn in pn_archive:
            fp.write(path_line(pn))
            traj_data.pop(pn)
