FROM python:3.9-buster

RUN curl -fsSL https://research.cs.wisc.edu/htcondor/repo/keys/HTCondor-current-Key | apt-key add -

RUN echo "deb [arch=amd64] https://research.cs.wisc.edu/htcondor/repo/debian/current buster main\ndeb-src https://research.cs.wisc.edu/htcondor/repo/debian/current buster main" > /etc/apt/sources.list.d/htcondor.list

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y htcondor && apt-get autoremove && apt-get clean

RUN useradd -m -U app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /home/app
USER app

RUN mkdir -p /home/app/.condor/tokens.d

COPY . .

ENV PYTHONPATH=/home/app

CMD ["python", "-m", "pyglidein_server"]
