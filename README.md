# test python with 
```bash
micromamba create -y -n pythontest -f conda.yaml
# or
micromamba install -y -n pythontest -f conda.yaml
python test.py
```


# test python with pip install
```bash
pip uninstall openiap-edge
python -m pip cache purge
pip install openiap-edge==0.0.15
python test.py
```