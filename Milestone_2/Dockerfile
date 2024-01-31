FROM python:3.11

RUN pip install pandas numpy matplotlib seaborn scipy scikit-learn geopy tqdm   

RUN pip install sqlalchemy psycopg2 pyarrow
        


ENTRYPOINT ["python","code/main.py"]