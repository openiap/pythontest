## Python test project
This is a test project showing how to call most of the functions in the OpenIAP core library.
cli.py is a command line interface that can be used to call a few functions
test.py is a test script that calls all the functions in the core library
queuetest.py is a basic example of how consume a message queue, and print any message you get

# Build and run
Setup environment with micromamba and run using the following commands
```bash
micromamba create -y -n pythontest -f conda.yaml
# or
micromamba install -y -n pythontest -f conda.yaml
python cli.py 
```
To run using default python installation, use the following commands
```bash
pip uninstall openiap-edge
python -m pip cache purge
pip install openiap-edge==0.0.26
python cli.py 
```
github repository:

```
https://github.com/skadefro/pythontest.git 
```