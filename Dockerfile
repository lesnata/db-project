FROM python:latest
ADD . /code
ADD users.csv /code

WORKDIR /code

COPY . ./code
COPY users.csv ./code

RUN pip install -r requirements.txt

CMD [ "python", "main.py"]
