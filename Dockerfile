FROM python:3.7

RUN pip install --upgrade pip

RUN useradd -m worker
WORKDIR /project
# It is a real shame that WORKDIR doesn't honor the current user (or even take a chown option), so.....
RUN chown worker:worker /project
USER worker

RUN pip install --upgrade --user pipenv
ENV PATH=/home/worker/.local/bin:$PATH

COPY --chown=worker:worker . ./

# First we get the dependencies for the stack itself
RUN pipenv lock -r > requirements.txt