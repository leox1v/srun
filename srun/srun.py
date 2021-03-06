#!/usr/bin/env python3

import os
import sys
import sh
import fabric
import uuid
from invoke import run as local
import invoke

'''
Argument should look like this:
    srun ladolphs@youagain opt1=1 opt2=2 ... python main.py
'''
def main():
    addr = get_remote_address()
    
    # should be executed as background process ?
    in_background = '-bg' in sys.argv
    if in_background:
        del sys.argv[sys.argv.index('-bg')]

    # execute on server
    # establish connection to the server
    connection = fabric.Connection(addr, inline_ssh_env=True) # connect to server
    connection.client.load_system_host_keys()

    srun_options = load_srun_options(connection)
    env = get_environment_variables(srun_options)
    if 'CUDA_VISIBLE_DEVICES' in env:
        srun_options['CUDA_VISIBLE_DEVICES'] = env['CUDA_VISIBLE_DEVICES']
    path = '/tmp/ladolphs/{}'.format(uuid.uuid4()) # new tmp folder on server

    # upload the files to the server
    upload_files_to_server(addr, path)
    
    # construct the command
    cmd = get_commands(path, srun_options, in_background)

    construct_venv(srun_options['VIRTUALENV'], connection)

    # execute the command
    connection.run(cmd, env=env) 
    
    if in_background:
        print('[i] Started background tmux session {} on {}'.format(path.split('/')[-1], addr.split('@')[-1]))

def exists(name, connection, is_directory=True):
    return bool(int(connection.run('test {} {} && echo 1 || echo 0'.format('-d' if is_directory else '-f', name)).stdout))

def construct_venv(path, connection):
    prefix = ''
    if path[0] == '/':
        prefix = '/'
        path = path[1:]

    dirs = [d for d in path.split("/") if not '.' in d]
    dirs = ['{}{}'.format(prefix, "/".join(dirs[:i+1])) for i in range(len(dirs))]
    for _dir in dirs[:-1]:
        if not exists(_dir, connection):
            connection.run('mkdir {}'.format(_dir))
    
    if not exists(dirs[-1], connection):
        # virtualenv doesnt exist yet
        connection.run('python3 -m venv {}'.format(dirs[-1]))

def execute_in_background(session_id, cmd):
    wrapped_cmd = 'tmux new-session -s {} -d && tmux send-keys -t {} "{}" Enter'.format(session_id, session_id, cmd)
    return wrapped_cmd

def get_commands(path, srun_options, in_background):
    cd_cmd = 'cd {}'.format(path)
    execution_cmd = '{}'.format(' '.join(sys.argv[1:])) # command 
    if 'CUDA_VISIBLE_DEVICES' in srun_options:
        execution_cmd = 'DATADIR={} CUDA_VISIBLE_DEVICES={} {}'.format(srun_options['DATADIR'], srun_options['CUDA_VISIBLE_DEVICES'], execution_cmd)
    cmds = [cd_cmd, execution_cmd]  

    if 'requirements.txt' in os.listdir():
        activate_virtualenv = 'source {}/bin/activate'.format(srun_options['VIRTUALENV'])
        install_requirements = 'pip install --quiet -r requirements.txt'
        cmds = [activate_virtualenv, cd_cmd, install_requirements, execution_cmd]

    cmd = ' && '.join(cmds)
    if in_background:
        cmd = execute_in_background(path.split('/')[-1], cmd)

    return cmd

def upload_files_to_server(addr, path):
    # exclude some files
    files_to_exclude = ['__pycache__', '*.swp', '.git', '.DS_Store', '.gitignore']
    files_to_exclude = ' --exclude '.join(files_to_exclude).split()
    print(sh.rsync('-a', '-v', '-z', '--exclude', *files_to_exclude, '{}/'.format(os.getcwd()), '{}:{}'.format(addr, path)))

def get_environment_variables(srun_options):
    env = []
    while '=' in sys.argv[1]: # save all the options (having '=') in dictionary env
        env.append(sys.argv[1].split('='))
        del sys.argv[1]
    env = dict(env)
    if not 'DATADIR' in env:
        env['DATADIR'] = srun_options['DATADIR']
    return env

def get_remote_address():
    addr = sys.argv[1]
    del sys.argv[1]
    return addr

def load_srun_options(connection, local=False):
    # loads the srun options from the server
    try:
        if local:
            file_path = os.path.join(os.path.expanduser('~'), '.srun.conf')
        else:
            connection.get('.srun.conf', '/tmp/.srun.conf')
            file_path = '/tmp/.srun.conf'
        with open(file_path, 'r') as f:
            options = f.read().split('\n')
        options = {opt.split('=')[0]: opt.split('=')[1] for opt in options if '=' in opt}
        assert all(key in options for key in ['DATADIR', 'VIRTUALENV']), 'Not all required keys are in the srun.conf file.'
       
        if local:
            for k, v in options.items():
                if '~' in v:
                    options[k] = v.replace('~', os.path.expanduser('~'))
    except FileNotFoundError:
        print('Options couldnt be found.')
        exit(1)
    return options

if __name__ == '__main__':
    main()
