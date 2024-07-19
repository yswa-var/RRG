FROM python:3.12

EXPOSE 8501

WORKDIR /app

COPY requirements.txt ./requirements.txt

RUN pip install -r requirements.txt
RUN pip install setuptools wheel
RUN pip install streamlit

COPY . .

CMD ["streamlit", "run", "app/app.py"]

# docker run --name postgres -e POSTGRES_PASSWORD=123456 -d -p 5454:5454 -v postgres_data:/var/lib/postgresql/data postgres