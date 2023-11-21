python3 -m venv iretisvenv
source iretisvenv/bin/activate
git clone https://gitlab.com/danielzh/pyretis.git
cd pyretis ; git checkout dz24/dask
pip install pip --upgrade
python -m pip install -r requirements-dev.txt
python -m pip install -e .
cd ..
git clone  https://github.com/dz24/infretis.git
cd infretis ; git checkout dz24/external
pip install dask distributed tomli_w
# If you want to run with mdanalysis, you'll need dask=2023.3.0 and distributed=2023.3.0
python -m pip install -e .
cd ..
