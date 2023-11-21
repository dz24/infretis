import os
import numpy as np
import time
import tomli
import logging
from datetime import datetime
from pyretis.inout.formats.formatter import get_log_formatter
from pyretis.core.tis import select_shoot
from pyretis.core.retis import retis_swap_zero, ppretis_swap
from pyretis.setup import create_simulation
from pyretis.inout.settings import parse_settings_file
from pyretis.inout.restart import write_ensemble_restart
import subprocess
from pyretis.inout.archive import PathStorage
from pyretis.inout.common import make_dirs
from infretis.inf_core import REPEX_state
from dask.distributed import dask, Client, as_completed, get_worker
from pyretis.core.common import compute_weight
dask.config.set({'distributed.scheduler.work-stealing': False})
logger = logging.getLogger('')
# logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)
# Define a console logger. This will log to sys.stderr:
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
console.setFormatter(get_log_formatter(logging.WARNING))
logger.addHandler(console)
DATE_FORMAT = "%Y.%m.%d %H:%M:%S"

def run_bm(md_items):
    md_items['wmd_start'] = time.time()
    ens_nums = md_items['ens_nums']
    ensembles = md_items['ensembles']
    settings = md_items['settings']
    interfaces = md_items['interfaces']

    # set shooting_move = 'md'
    if 'shooting_move' in settings:
        settings['shooting_move'] = 'md'

    start_cond = ensembles[ens_nums[0]+1]['path_ensemble'].start_condition
    accept, trials, status = select_shoot(ensembles[ens_nums[0]+1],
                                          md_items['settings'],
                                          start_cond)

    md_items['pnum_old'].append(ensembles[ens_nums[0]+1]['path_ensemble'].last_path.path_number)
    md_items['trial_len'].append(trials.length)
    md_items['trial_op'].append((trials.ordermin[0], trials.ordermax[0]))
    md_items.update({'status': 'BMA',
                     'interfaces': interfaces,
                     'wmd_end': time.time()})
    return md_items

def run_md(md_items):
    logger.info(f"Worker {md_items['pin']} is running.")
    md_items['wmd_start'] = time.time()
    ens_nums = md_items['ens_nums']
    ensembles = md_items['ensembles']
    interfaces = md_items['interfaces']
    start_time = datetime.now()
    logger.info(start_time.strftime(DATE_FORMAT))

    if len(ens_nums) == 1:
        pnum = ensembles[ens_nums[0]+1]['path_ensemble'].last_path.path_number
        enum = f"{ens_nums[0]+1:03.0f}"
        move = md_items['mc_moves'][ens_nums[0]+1]
        logger.info(f"Shooting {move} in ensemble: {enum}"\
                    f" with path: {pnum} and worker: {md_items['pin']}")
        start_cond = ensembles[ens_nums[0]+1]['path_ensemble'].start_condition

        pathold = ensembles[ens_nums[0]+1]['path_ensemble'].last_path
        #print(f"monkey the file {pathold.phasepoints[0].particles.config[0]}")
        #print(f"monkey the file {pathold.phasepoints[-1].particles.config[0]}")
        #print(f"monkey does the file exist: {os.path.exists(pathold.phasepoints[0].particles.config[0])}")
        #print(f"monkey does the file exist: {os.path.exists(pathold.phasepoints[-1].particles.config[0])}")

        if not md_items['internal']:
            ensembles[ens_nums[0]+1]['engine'].clean_up()
        accept, trials, status = select_shoot(ensembles[ens_nums[0]+1],
                                              md_items['settings'],
                                              start_cond)
        trials = [trials]

    elif len(ens_nums) == 2 and -1 in ens_nums:  # swap_zero
        logger.info(f"len(ens_nums):  {len(ens_nums)}")
        logger.info(f"ens_nums:  {ens_nums}")
        ensembles_l = [ensembles[i+1] for i in ens_nums]
        pnums = [ensembles_l[0]['path_ensemble'].last_path.path_number,
                 ensembles_l[1]['path_ensemble'].last_path.path_number]
        logger.info(f"Shooting sh sh in ensembles: {ens_nums[0]} <-> {ens_nums[1]}"\
                    f" with paths: {pnums} and worker: {md_items['pin']}")
        if not md_items['internal']:
            ensembles_l[0]['engine'].clean_up()
            ensembles_l[1]['engine'].clean_up()
        accept, trials, status = retis_swap_zero(ensembles_l, md_items['settings'], 0)

    else:
        logger.info(f"len(ens_nums):  {len(ens_nums)}")
        logger.info(f"ens_nums:  {ens_nums}")
        ensembles_l = [ensembles[i+1] for i in ens_nums]
        pnums = [ensembles_l[0]['path_ensemble'].last_path.path_number,
                 ensembles_l[1]['path_ensemble'].last_path.path_number]
        logger.info(f"Shooting sh sh in ensembles: {ens_nums[0]} <-> {ens_nums[1]}"\
                    f" with paths: {pnums} and worker: {md_items['pin']}")
        if not md_items['internal']:
            ensembles_l[0]['engine'].clean_up()
            ensembles_l[1]['engine'].clean_up()
        accept, trials, status = ppretis_swap(ensembles_l,0, md_items['settings'], 
                                              pnums[0])

    for trial, ens_num, ifaces in zip(trials, ens_nums, interfaces):
        md_items['moves'].append(md_items['mc_moves'][ens_num+1])
        md_items['pnum_old'].append(ensembles[ens_num+1]['path_ensemble'].last_path.path_number)
        md_items['trial_len'].append(trial.length)
        md_items['trial_op'].append((trial.ordermin[0], trial.ordermax[0]))
        md_items['generated'] = trial.generated
        logger.info(f'Move finished with trial path lenght of {trial.length}')
        log_mdlogs(f'{ens_num+1:03}/generate/')
        if status == 'ACC':
            valid = np.zeros(md_items['n'])
            valid[ens_num] = 1.
            trial.traj_v = valid
            if ens_num == -1: 
                trial.traj_v = (1.,)
            ensembles[ens_num+1]['path_ensemble'].last_path = trial
            logger.info('The move was accepted!')
        else:
            logger.info('The move was rejected!')
        msg = md_items.keys()
        logger.info(f"md_items: {msg}")
        logger.info(f"md_items['pn_old']: {md_items['pnum_old']}")
    end_time = datetime.now()
    delta_time = end_time - start_time
    logger.info(end_time.strftime(DATE_FORMAT) +
                f', {delta_time.days} days {delta_time.seconds} seconds' + '\n')
    md_items.update({'status': status,
                     'interfaces': interfaces,
                     'wmd_end': time.time()})
    logger.info(f"Worker {md_items['pin']} is done.")
    #logger.info(f"the file {trials[0].phasepoints[0].particles.config[0]}")
    #logger.info(f"the file {trials[0].phasepoints[-1].particles.config[0]}")
    #filename = trials[0].phasepoints[0].particles.config[0]
    #logger.info(f"does the file exist: {os.path.exists(trials[0].phasepoints[0].particles.config[0])}")
    #logger.info(f"does the file exist: {os.path.exists(trials[0].phasepoints[-1].particles.config[0])}")
    #p = subprocess.Popen(["ls", filename], stdout=subprocess.PIPE)
    #out, err = p.communicate()
    #logger.info(f"out: {out}")
    #logger.info(f"err: {err}")
    return md_items

def log_mdlogs(inp):
    logs = [log for log in os.listdir(inp) if 'log' in log]
    speed = []
    for log in logs:
        with open(os.path.join(inp, log), 'r') as read:
            for line in read:
                if 'Performance' in line:
                    logger.info(log + ' '+ line.rstrip().split()[1] + ' ns/day')

def treat_output(state, md_items):
    traj_num_dic = state.traj_num_dic
    traj_num = state.config['current']['traj_num']
    ensembles = md_items['ensembles']
    pn_news = []
    md_items['md_end'] = time.time()

    # analyse and record worker data
    for ens_num, pn_old in zip(md_items['ens_nums'],
                               md_items['pnum_old']):
        out_traj = ensembles[ens_num+1]['path_ensemble'].last_path

        for idx, lock in enumerate(state.locked):
            if str(pn_old) in lock[1]:
                state.locked.pop(idx)

        state.ensembles[ens_num+1] = ensembles[ens_num+1]
        # if path is new: number and save the path:
        #print("traj_out: ", dir(out_traj))
        if out_traj.path_number == None or md_items['status'] == 'ACC':
            # move to accept:
            #logger.info(f"pn_old: {pn_old}")
            #logger.info(f"traj_num_dic: {traj_num_dic}")
            ens_save_idx = traj_num_dic[pn_old]['ens_save_idx']
            #logger.info(f"Shark")
            #logger.info(f"ens_save_idx: {ens_save_idx}")
            #for i in out_traj.phasepoints:
            #    logger.info(f"before i: {i}")
            state.ensembles[ens_save_idx]['path_ensemble'].store_path(out_traj)
            #for i in out_traj.phasepoints:
            #    logger.info(f"after i: {i}")
            out_traj.path_number = traj_num
            #print("zerba: ", out_traj.path_number)

            traj_num_dic[traj_num] = \
                {'frac': np.zeros(state.n, dtype="float128"),
                 'ptype': get_ptype(out_traj, 
                                     *ensembles[ens_num+1]['interfaces']),
                 'max_op': out_traj.ordermax,
                 'min_op': out_traj.ordermin,
                 'length': out_traj.length,
                 'traj_v': out_traj.traj_v,
                 'ens_save_idx': ens_save_idx}
            if not md_items['internal']:
                traj_num_dic[traj_num]['adress'] = set(os.path.basename(kk.particles.config[0]) for kk in out_traj.phasepoints)
            traj_num += 1

            # NB! Saving can take some time..
            # add setting where we save .trr file or not (we always save restart)
            if md_items['internal'] and state.config['output']['store_paths']:
                make_dirs(f'./trajs/{out_traj.path_number}')
            if state.config['output']['store_paths'] and not md_items['internal']:
                state.pstore.output(state.cstep, state.ensembles[ens_num+1]['path_ensemble'])
                if state.config['output'].get('delete_old', False) and pn_old > state.n - 2:
                    # if pn is larger than ensemble number ...
                    for adress in traj_num_dic[pn_old]['adress']:
                        os.remove(f'./trajs/{pn_old}/accepted/{adress}')

        if state.config['output']['store_paths']:
            # save ens-path_ens-rgen (not used) and ens-path
            write_ensemble_restart(state.ensembles[ens_num+1], state.pyretis_settings, save='path')
            # save ens-rgen, ens-engine-rgen
            write_ensemble_restart(state.ensembles[ens_num+1], state.pyretis_settings, save=f'e{ens_num+1}')

        pn_news.append(out_traj.path_number)
        state.add_traj(ens_num, out_traj, out_traj.traj_v)
        ensembles.pop(ens_num+1)
        
    # record weights 
    locked_trajs = state.locked_paths()
    if state._last_prob is None:
        state.prob
    for idx, live in enumerate(state.live_paths()):
        if live not in locked_trajs:
            traj_num_dic[live]['frac'] += state._last_prob[:-1][idx, :]

    # write succ data to infretis_data.txt
    if md_items['status'] == 'ACC':
        write_to_pathens(state, md_items['pnum_old'])

    state.sort_trajstate()
    state.config['current']['traj_num'] = traj_num
    state.cworker = md_items['pin']
    #print("md_items: ", md_items)
    state.print_shooted(md_items, pn_news)
    # save for possible restart
    state.save_rng()
    state.write_toml()

def setup_internal(input_file):

    # setup logger
    fileh = logging.FileHandler('sim.log', mode='a')
    log_levl = getattr(logging, 'info'.upper(),
                       logging.INFO)
    # log_levl = getattr(logging, 'debug'.upper(),
    #                    logging.DEBUG)
    fileh.setLevel(log_levl)
    fileh.setFormatter(get_log_formatter(log_levl))
    logger.addHandler(fileh)

    # read input_file.toml
    with open(input_file, mode="rb") as f:
        config = tomli.load(f)
    # if input_file.toml != restart.toml and restart.toml exist:
    if os.path.isfile('restart.toml') and \
       'restart.toml' not in input_file and \
       config['output']['store_paths']:
        with open('./restart.toml', mode="rb") as f:
            restart = tomli.load(f)
        # check if they are similar to use restart over input_file
        equal = True
        for key in ['dask', 'simulation', 'output']:
            if config[key] != restart[key]:
                equal = False
                break
        if equal:
            for act in restart['current']['active']:
                store_p = os.path.join('trajs', str(act), 'ensemble.restart')
                if not os.path.isfile(store_p):
                    equal = False
                    break
        if equal:
            restart['current']['restarted_from'] = restart['current']['cstep']
            config = restart
            logger.info('We use restart.toml instead.')

    # parse retis.rst
    inp = config['simulation']['pyretis_inp']
    sim_settings = parse_settings_file(inp)
    interfaces = sim_settings['simulation']['interfaces']
    size = len(interfaces)

    # setup config
    endsim = setup_config(config, size)
    if endsim:
        return None, None, None

    # setup pyretis and infretis
    sim = setup_pyretis(config, sim_settings)
    state = setup_repex(config, sim)

    # initiate by adding paths from retis sim to repex
    traj_num_dic = state.traj_num_dic
    for i in range(size-1):
        # we add all the i+ paths.
        path = sim.ensembles[i+1]['path_ensemble'].last_path
        valid = np.zeros(state.n)
        valid[i] = 1.
        path.traj_v = valid
        state.add_traj(ens=i, traj=path, valid=path.traj_v, count=False)
        pnum = path.path_number
        frac = config['current']['frac'].get(str(pnum), np.zeros(size+1))
        traj_num_dic[pnum] = {'ens_save_idx': i + 1,
                              'max_op': path.ordermax,
                              'min_op': path.ordermin,
                              'ptype': get_ptype(path, 
                                                 *sim.ensembles[i+1]['interfaces']),
                              'length': path.length,
                              'traj_v': path.traj_v,
                              'frac': np.array(frac, dtype='float128')}
        if not config['simulation']['internal']:
            traj_num_dic[pnum]['adress'] = set(os.path.basename(kk.particles.config[0]) for kk in path.phasepoints)
    
    # add minus path:
    path = sim.ensembles[0]['path_ensemble'].last_path
    pnum = path.path_number
    path.traj_v = (1.,)
    state.add_traj(ens=-1, traj=path, valid=path.traj_v, count=False)
    frac = config['current']['frac'].get(str(pnum), np.zeros(size+1))
    traj_num_dic[pnum]= {'ens_save_idx': 0,
                         'max_op': path.ordermax,
                         'min_op': path.ordermin,
                         'ptype': get_ptype(path,
                                            *sim.ensembles[0]['interfaces']),
                         'length': path.length,
                         'traj_v': path.traj_v,
                         'frac': np.array(frac, dtype='float128')}
    if not config['simulation']['internal']:
        traj_num_dic[pnum]['adress'] = set(os.path.basename(kk.particles.config[0]) for kk in path.phasepoints)

    # Ensemble definitions can be changed here to PPTIS QQQQQ 
    state.ensembles = {i: sim.ensembles[i] for i in range(len(sim.ensembles))}
    sim.settings['initial-path']['load_folder'] = 'trajs'
    state.pyretis_settings = sim.settings
    md_items = {'mc_moves': state.mc_moves,
                'ensembles': {}, 
                'internal': config['simulation']['internal']}

    if state.pattern_file:
        writemode = 'a' if 'restarted_from' in state.config['current'] else 'w'
        with open(state.pattern_file, writemode) as fp:
            fp.write(f"# Worker\tMD_start [s]\t\twMD_start [s]\twMD_end"
                     + f"[s]\tMD_end [s]\t Dask_end [s]\tEnsembles\t{state.start_time}\n")

    return md_items, state, config

def setup_dask(config, workers):
    client = Client(n_workers=workers,)
    for module in config['dask'].get('files', []):
        client.upload_file(module)
    futures = as_completed(None, with_results=True)
    # create worker logs
    client.run(set_logger)
    return client, futures

def pwd_checker(state):
    all_good = True
    ens_str = [f'{i:03.0f}' for i in range(state.n-1)]

    tot = []
    for path_temp in state._trajs[:-1]:
        tot += list(set([pp.particles.config[0] for pp in path_temp.phasepoints]))
    for ppath in tot:
        if not os.path.isfile(ppath):
            print('warning! this path does not exist', ppath)
            all_good = False

    return all_good

def prep_pyretis(state, md_items, inp_traj, ens_nums):

    # pwd_checker
    if not md_items['internal']:
        if not pwd_checker(state):
            exit('sumtin fishy goin on here')

    # prep path and ensemble
    for ens_num, traj_inp in zip(ens_nums, inp_traj):
        state.ensembles[ens_num+1]['path_ensemble'].last_path = traj_inp
        md_items['ensembles'][ens_num+1] = state.ensembles[ens_num+1]

        # in retis.rst, gmx = gmx, mdrun = gmx mdrun
        # config['dask']['wmdrun'] a list of commands with len equal no works.
        if not md_items['internal'] and state.config['dask'].get('wmdrun', False):
            mdrun0 = state.config['dask']['wmdrun'][md_items['pin']]
            mdrun = mdrun0 + ' -s {} -deffnm {} -c {}'
            mdrun_c = mdrun0 + ' -s {} -cpi {} -append -deffnm {} -c {}'
            md_items['ensembles'][ens_num+1]['engine'].mdrun = mdrun
            md_items['ensembles'][ens_num+1]['engine'].mdrun_c = mdrun_c

    interfaces = state.pyretis_settings['simulation']['interfaces']
    if len(ens_nums) == 1:
        interfaces = [interfaces] if ens_nums[0] >= 0 else [interfaces[0:1]]
        md_items['settings'] = state.pyretis_settings['ensemble'][ens_nums[0]+1]['tis']
        md_items['interfaces'] = interfaces
    else:
        md_items['settings'] = state.pyretis_settings
        md_items['interfaces'] = [interfaces[0:1], interfaces]

    # write pattern:
    if state.pattern_file and state.toinitiate == -1:
        state.write_pattern(md_items)
    else:
        md_items['md_start'] = time.time()

    # empty / update md_items:
    for key in ['moves', 'pnum_old', 'trial_len', 'trial_op', 'generated']:
        md_items[key] = []
    md_items.update({'ens_nums': ens_nums})

# def calc_cv_vector(path, interfaces, moves):
#     path_max, _ = path.ordermax
    

#     cv = []
#     if len(interfaces) == 1:
#         return (1. if interfaces[0] <= path_max else 0.,)

#     for idx, intf_i in enumerate(interfaces[:-1]):
#         if moves[idx+1] == 'wf':
#             intfs = [interfaces[0], intf_i, interfaces[-1]]
#             cv.append(compute_weight(path, intfs, moves[idx+1]))
#         else:
#             cv.append(1. if intf_i <= path_max else 0.)
#     cv.append(0.)
#     return(tuple(cv))

def setup_config(config, size):

    data_dir = config['output']['data_dir']
    data_file = os.path.join(data_dir, 'infretis_data.txt')
    config['output']['data_file'] = data_file

    # check if we restart or not
    if 'current' not in config:
        config['current'] = {'traj_num': size, 'cstep': 0,
                             'active': list(range(size)),
                             'locked': [], 'size': size, 'frac': {}}
        # write/overwrite infretis_data.txt
        with open(data_file, 'w') as fp:
            fp.write('# ' + '='*(34+8*size)+ '\n')
            ens_str = '\t'.join([f'{i:03.0f}' for i in range(size)])
            fp.write('# ' + f'\txxx\tlen\tmax OP\t\t{ens_str}\n')
            fp.write('# ' + '='*(34+8*size)+ '\n')
    else:
        config['current']['restarted_from'] = config['current']['cstep']
        if config['current']['cstep'] == config['simulation']['steps']:
            print('current step and total steps are equal so we exit ',
                  'without doing anything.')
            return True
    return False

def setup_pyretis(config, sim_settings):
    # give path to the active paths
    sim_settings['current'] = {'active': config['current']['active']}

    sim = create_simulation(sim_settings)
    for idx, pn in enumerate(config['current']['active']):
        sim.ensembles[idx]['path_ensemble'].path_number = pn

    sim.set_up_output(sim_settings)
    sim.initiate(sim_settings)
    return sim

def setup_repex(config, sim):
    state = REPEX_state(n=config['current']['size'],
                        workers=config['dask']['workers'],
                        minus=True)
    state.tsteps = config['simulation']['steps']
    state.cstep = config['current']['cstep']
    state.screen = config['output']['screen']
    state.output_tasks = sim.output_tasks
    state.mc_moves = sim.settings['tis']['shooting_moves']
    state.config = config
    if config['output'].get('pattern', False):
        state.pattern_file = os.path.join('pattern.txt')
    state.data_file = config['output']['data_file']
    if 'restarted_from' in config['current']:
        state.set_rng()
    state.locked0 = list(config['current'].get('locked', []))
    state.locked = list(config['current'].get('locked', []))

    pstore = PathStorage()
    state.pstore = pstore

    return state

def set_logger():
    pin = get_worker().name
    log = logging.getLogger()
    fileh = logging.FileHandler(f"worker{pin}.log", mode='a')
    log_levl = getattr(logging, 'info'.upper(),
                       logging.INFO)
    # log_levl = getattr(logging, 'debug'.upper(),
    #                    logging.DEBUG)
    fileh.setLevel(log_levl)
    fileh.setFormatter(get_log_formatter(log_levl))
    logger.addHandler(fileh)
    logger.info(f'=============================')
    logger.info(f'Logging file for worker {pin}')
    logger.info(f'=============================\n')

def write_to_pathens(state, pn_archive):
    traj_num_dic = state.traj_num_dic
    size = state.n

    with open(state.data_file, 'a') as fp:
        for pn in pn_archive:
            string = ''
            string += f'\t{pn:3.0f}\t'
            string += f"{traj_num_dic[pn]['length']:5.0f}" + '\t'
            string += f"{traj_num_dic[pn]['max_op'][0]:8.5f}" + '\t'
            string += f"{traj_num_dic[pn]['min_op'][0]:8.5f}" + '\t'
            string += f"{traj_num_dic[pn]['ptype']}" + '\t'
            frac = []
            weight = []
            logger.info(f'traj_num_dic: {traj_num_dic[pn]}')
            if len(traj_num_dic[pn]['traj_v']) == 1:
                f0 = traj_num_dic[pn]['frac'][0]
                w0 = traj_num_dic[pn]['traj_v'][0]
                frac.append('----' if f0 == 0.0 else str(f0))
                if weight == 0:
                    print('tortoise', frac, weight)
                    exit('fish')
                weight.append('----' if f0 == 0.0 else str(w0))
                frac += ['----']*(size-2)
                weight += ['----']*(size-2)
            else:
                frac.append('----')
                weight.append(f'----')
                for w0, f0 in zip(traj_num_dic[pn]['traj_v'][:-1],
                                  traj_num_dic[pn]['frac'][1:-1]):
                    frac.append('----' if f0 == 0.0 else str(f0))
                    weight.append('----' if w0 == 0.0 else str(w0))
            fp.write(string + '\t'.join(frac) + '\t' + '\t'.join(weight) + '\t\n')
            traj_num_dic.pop(pn)

def get_ptype(path, L, M, R):
    end_cond = 'L' if path.phasepoints[-1].order[0] < L else 'R'
    start_cond = 'R' if path.phasepoints[0].order[0] > R else 'L'
    return start_cond + 'M' + end_cond