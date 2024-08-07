"""The main infretis loop."""
import asyncio

from infretis.core.tis import run_md


async def scheduler(loop, md_items, state):
    """Run infretis loop."""
    # setup repex, dask and futures
    # client, futures = setup_dask(state)
    incomp = set()

    # submit the first number of workers
    i = 0
    while state.initiate():
        # pick and prep ens and path for the next job
        md_items = state.prep_md_items(md_items)

        # submit job to scheduler
        print("send it 0", md_items["picked"])
        print("send it a", list(md_items.keys()))
        list(md_items.keys())
        # for z in [0, 1, 2, 3, 4, 6, 7, 8,]:
        #     md_items0.pop(keys[z])
        # print('zzz', len(keys))
        print("send it b", list(md_items.keys()), md_items)
        print(" ")
        # md_items.pop(
        # incomp.add(loop.create_task(run_md0(md_items)))
        # new = {'picked':{'whadaf': pick[i]}}
        # new['picked']['ens'] = md_items["picked"][pick[i]]['ens']
        # new['picked']['traj'] = md_items["picked"][pick[i]]['traj']
        # print('send it 1', new)

        # incomp.add(loop.create_task(run_md0(md_items["picked"][pick[i]])))
        incomp.add(loop.create_task(run_md(md_items, md_items["picked"])))
        i += 1

    while len(incomp):
        print("tres a", len(incomp))
        # get and treat worker output
        # md_items = state.treat_output(next(futures)[1])
        _done, incomp = await asyncio.wait(
            incomp, return_when=asyncio.FIRST_COMPLETED
        )
        print("tres b", len(incomp))
        for _done0 in _done:
            # submit new job:
            state.cstep += 1
            # print('ducki', _done0.result()["picked"])
            print("ducki a", _done0.result()[0])
            print("ducki a", _done0.result()[1])
            # exit('howe')
            md_items = state.treat_output(*_done0.result())
            if state.cstep + state.workers <= state.tsteps:
                # chose ens and path for the next job
                md_items = state.prep_md_items(md_items)

                # submit job to scheduler
                incomp.add(
                    loop.create_task(run_md(md_items, md_items["picked"]))
                )
            #     incomp.add(loop.create_task(run_md(md_items)))
            # fut = client.submit(
            #     run_md, md_items, workers=md_items["pin"], pure=False
            # )
            # futures.add(fut)

    # end client
    # client.close()
