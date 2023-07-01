import os
import MDAnalysis as mda
import numpy as np
from pyretis.core.pathensemble import generate_ensemble_name
from pyretis.inout.common import make_dirs
import sys

predir = 'load'
traj = './merged.trr' # sys.argv[1] # trajectory  file
order = './order.txt' # sys.argv[2] # order file

u = mda.Universe(traj)
order = np.loadtxt(order)

interfaces = [ -0.55,        -0.549729973, -0.549459946, -0.549159916,
               -0.548844884, -0.54849985,  -0.548124812, -0.54770477,
               -0.547239724, -0.546729673, -0.546129613, -0.545424542,
               -0.544584458, -0.543519352, -0.542154215, -0.540354035,
               -0.538163816, -0.535853585, -0.533648365, -0.531593159,
               -0.529627963, -0.527692769, -0.525772577, -0.523807381,
               -0.521752175, -0.519561956, -0.517191719, -0.51459646,
               -0.511761176, -0.508715872, -0.505520552, -0.502280228,
               -0.499054905, -0.495844584, -0.492619262, -0.489273927,
               -0.485688569, -0.481578158, -0.47640264, -0.468076808,
               -0.40]

sorted_idx = np.argsort(order[:,1])
no_shpt = 10

for i in range(len(interfaces)):
    dirname = os.path.join(predir, generate_ensemble_name(i))
    accepted = os.path.join(dirname, 'accepted')
    trajfile = os.path.join(accepted, 'traj.trr')
    orderfile = os.path.join(dirname, 'order.txt')
    print('Making folder: {}'.format(dirname))
    make_dirs(dirname)
    print('Making folder: {}'.format(accepted))
    make_dirs(accepted)
    print('Writing trajectory {} and order {}'.format(trajfile, orderfile))

    # [0^-] ensemble
    if i == 0:
        start = 0
        where = np.where(order[:,1]>interfaces[i])[0]
        where_sort = np.argsort(order[:, 1][where])
        stop = np.where(order[sorted_idx,1]<interfaces[0])[0][-1]+1
        # # iterator = [sorted_idx[stop]]+ [i for i in sorted_idx[start:stop-1]] + [sorted_idx[stop]]
        iterator = [where[where_sort][0]] + [sorted_idx[0]]*250 + [where[where_sort][0]]
    # [(N-1)^+] ensembles
    else:
        where = np.where(order[:,1]>interfaces[i-1])[0]
        where_sort = np.argsort(order[:, 1][where])
        iterator = [sorted_idx[0]] + list(np.random.choice(where[where_sort[:no_shpt]], 250))

    # write trajectory frames
    with mda.Writer(trajfile,u.atoms.n_atoms) as W:
        for idxi in iterator:
            u.trajectory[idxi]
            W.write(u.atoms)

    # write order file
    np.savetxt(orderfile, np.c_[order[:len(iterator),0],order[iterator,1]], header=f"{'time':>10} {'orderparam':>15}",fmt=["%10.d","%15.8f"])
