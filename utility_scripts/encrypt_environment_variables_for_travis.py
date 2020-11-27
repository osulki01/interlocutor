"""Encrypt secrets so that Travis CI can decrypt and use them when running tests."""

import os
import pathlib
import subprocess


if __name__ == '__main__':

    # Get full path of staging environment variables
    directory_of_this_script = os.path.dirname(os.path.abspath(__file__))
    project_root = pathlib.Path(directory_of_this_script).parent
    secrets_file = f'{project_root}/Docker/environment_variables/.env.stg'

    # Store the environment variable and encrypted version of its value
    encrypted_variables = {}

    with open(file=secrets_file, mode='rt') as env_file:

        for line in env_file:
            if not line.startswith('#'):  # Ignore commented lines
                variable = line.split('=')[0]
                secret = line.split('=')[1]

                encrypt_command = ['travis', 'encrypt', '--pro', f'{variable}={secret}']

                try:
                    result = subprocess.run(
                        encrypt_command,
                        stderr=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        text=True,
                        check=True,
                        universal_newlines=True
                    )

                    encrypted_variables[variable] = result.stdout

                except subprocess.CalledProcessError as called_process_exception:
                    print(called_process_exception.stderr)
                    raise called_process_exception

    print(f'Secrets file has been encrypted: {secrets_file} \n')
    print('Add each of the encrypted secrets as a new row in the environment section of .travis.yml as per the '
          'example below:')
    print('''
    env:
      global:
        secure: <encoded secret 1>
        secure: <encoded secret 2>
    ''')

    for variable in encrypted_variables:
        print(f'For variable {variable} the encrypted secret is...')
        print(encrypted_variables[variable])
