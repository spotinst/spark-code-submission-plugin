import pip
import sys

#parent = '/opt/spark/work-dir'
parent = '/Users/sigmar/Documents/GitHub/SparkCodeSubmissionPlugin/src/main/resources'
#pip.main(['uninstall', 'pycrypto', 'Crypto'])
if "--public-key" in sys.argv:
    print("Using new launch_ipykernel.py")
    #pip.main(['install', '--target', '/opt/spark/work-dir', 'xyzservices', 'geopandas', 'bokeh', 'pycryptodomex', 'future', 'IPython', 'ipython_genutils', 'ipykernel', 'jedi', 'jupyter_client', 'jupyter_core', 'parso', 'pexpect', 'pickleshare', 'prompt-toolkit', 'ptyprocess', 'pygments', 'python-dateutil', 'pyzmq', 'setuptools', 'six', 'traitlets', 'wcwidth'])
    path = 'launch_ipykernel_eg322.py'
else:
    print("Using old launch_ipykernel.py")
    #pip.main(['install', '--target', '/opt/spark/work-dir', 'xyzservices', 'geopandas', 'bokeh', 'pycryptodome', 'future', 'IPython', 'ipython_genutils', 'ipykernel', 'jedi', 'jupyter_client', 'jupyter_core', 'parso', 'pexpect', 'pickleshare', 'prompt-toolkit', 'ptyprocess', 'pygments', 'python-dateutil', 'pyzmq', 'setuptools', 'six', 'traitlets', 'wcwidth'])
    path = 'launch_ipykernel_old.py'

print(f"hey! {path}")
with open(f'{parent}/{path}', 'r') as f:
    script = f.read()

exec(script)