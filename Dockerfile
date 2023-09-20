FROM python:3.9

WORKDIR /code

COPY requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# COPY ./app /code/app
# COPY ./data /code/data
# COPY ./src /code/src
COPY ./streamlit /code/streamlit
COPY .env /code/

# EXPOSE 80
EXPOSE 8501

# CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "80"]
CMD ["streamlit", "run", "streamlit/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
