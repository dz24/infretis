import tomli
from help_func import run_md, treat_output
from help_func import setup_internal, setup_dask, prep_pyretis

if __name__ == "__main__":

    with open("./infretis.toml", mode="rb") as f:
        config = tomli.load(f)

    # setup pyretis, repex, dask client and futures
    md_items, state = setup_internal(config)
    client, futures = setup_dask(state.workers)

    # submit the first number of workers
    while state.initiate():
        # chose ens and path for the next job
        ens_nums, input_traj = state.pick_lock()
        prep_pyretis(state, md_items, input_traj, ens_nums)

        # submit job
        fut = client.submit(run_md, md_items, pure=False)
        futures.add(fut)

    # main loop
    while state.loop():
        # get output from finished worker
        md_items = next(futures)[1]

        # analyze & store output
        treat_output(state, md_items)

        # submit new job:
        if state.cstep < state.tsteps:
            # chose ens and path for the next job
            ens_nums, inp_traj = state.pick()
            prep_pyretis(state, md_items, inp_traj, ens_nums)

            # submit job
            fut = client.submit(run_md, md_items, pure=False)
            futures.add(fut)