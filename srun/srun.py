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
    srun.py ladolphs@youagain opt1=1 opt2=2 ... python main.py
'''
def main():
    addr = get_remote_address()
    
    if addr == 'local':
        srun_options = load_local_srun_options()
        env = get_environment_variables(srun_options)
        cmd = get_local_commands(srun_options)
        connection = invoke
    else:
        # execute on server
        # establish connection to the server
        connection = fabric.Connection(addr, inline_ssh_env=True) # connect to server
        connection.client.load_system_host_keys()

        srun_options = load_srun_options(connection)
        env = get_environment_variables(srun_options)
        path = '/tmp/{}'.format(uuid.uuid4()) # new tmp folder on server

        # upload the files to the server
        upload_files_to_server(addr, path)
        
        # construct the command
        cmd = get_commands(path, srun_options)

    # execute the command
    connection.run(cmd, env=env) 

def get_local_commands(srun_options):
    execution_cmd = '{}'.format(' '.join(sys.argv[1:])) # command 
    cmds = [execution_cmd]  

    if 'requirements.txt' in os.listdir():
        activate_virtualenv = 'source {}/bin/activate'.format(srun_options['VIRTUALENV'])
        install_requirements = 'pip install --quiet -r requirements.txt'
        cmds = [activate_virtualenv, install_requirements, execution_cmd]

    return ' && '.join(cmds)

def get_commands(path, srun_options):
    cd_cmd = 'cd {}'.format(path)
    execution_cmd = '{}'.format(' '.join(sys.argv[1:])) # command 
    cmds = [cd_cmd, execution_cmd]  

    if 'requirements.txt' in os.listdir():
        activate_virtualenv = 'source {}/bin/activate'.format(srun_options['VIRTUALENV'])
        install_requirements = 'pip install --quiet -r requirements.txt'
        cmds = [activate_virtualenv, cd_cmd, install_requirements, execution_cmd]

    return ' && '.join(cmds)

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

def load_local_srun_options():
    return load_srun_options(connection=None, local=True)

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
        import IPython ; IPython.embed() ; exit(1) 
        print('Options couldnt be found.')
        exit(1)
    return options

if __name__ == '__main__':
    main()
