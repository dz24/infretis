from dask.distributed import Client, as_completed
import subprocess
import os
import time

def print_output(output, cnt, detail=True, start_time=0):
    if detail:
        print(' --- ', 'cycle: ', cnt, ' | START', ' | --- ')
        print('worker:\t', output['worker'])
        print('return_code:\t', output['return_code'])
        print('start_time:\t', output['start_time'])
        print('end_time:\t', output['end_time'])
        print('sim_time:\t', output['sim_time'])
        print(' --- ', 'cycle: ', cnt, ' | END  ', ' | --- ')
        print(' ')
    else: 
        print('------------', f"{output['cnt']:02}", f'{time.time() - start_time:05.02f}',
                output['folder'], output['worker'],
                ' | return code: ', output['return_code'], '------------')


def func0(inp_dic):
    worker = inp_dic['worker']
    folder = inp_dic['folder']
    cnt = inp_dic['cnt']
    inp_dic['start_time'] = time.time()
    
    # run nwchem in worker .. 
    stdout = folder + f'stdout_{cnt}.txt'
    stderr = folder + 'stderr.txt'
    cmd = ["srun", "--exclusive", "--ntasks", "2", "--mem-per-cpu", "500", "nwchem", f"worker{worker}.nw"]
    with open(stdout, 'wb') as out, open(stderr, 'wb') as err:
        exe = subprocess.Popen(cmd, cwd=folder, stdout=out, stderr=err)
        exe.communicate(input=None)
        inp_dic['return_code'] = exe.returncode

    inp_dic['end_time'] = time.time()
    inp_dic['sim_time'] = inp_dic['end_time'] - inp_dic['start_time']
    inp_dic['cnt'] = cnt
    return inp_dic

if __name__ == "__main__":

   n_workers = 2
   for worker in range(n_workers):
       worker_n = f'worker{worker}'
       if not os.path.exists(worker_n):
           exe = subprocess.Popen(['mkdir', worker_n])
       exe = subprocess.Popen(['cp', 'in.nw', worker_n + '/' + worker_n + '.nw'])

   start_time = time.time()
   cnt = 0
   cnt1 = 0
   maxi = 10
   detail = True
   client = Client(n_workers=n_workers)

   futures = as_completed(None, with_results=True)
   for worker in range(n_workers):
       inp_dic = {'worker': worker,
                  'folder': f'./worker{worker}/',
                  'cnt':cnt1}
       print('workerz', worker, f'./worker{worker}/')
       j = client.submit(func0, inp_dic)
       futures.add(j)
       cnt1+=1
    
   while cnt < maxi:
       output = next(futures)[1]

       print_output(output, cnt, detail, start_time)

       inp_dic = {'worker': output['worker'],
                  'folder': output['folder'],
                  'cnt':cnt1}
       fut = client.submit(func0, inp_dic)
       futures.add(fut)
       cnt+=1
       cnt1+=1

   for i in futures:
       output = i[1]
       print_output(output, cnt, detail, start_time)
       cnt+=1