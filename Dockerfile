FROM macavaney/idabox:py310-v66

WORKDIR /opt/ows_preprocessing
ENV PYTHONPATH="${PYTHONPATH}:/opt/ows_preprocessing"
ENV RESILIPIPE_DIR="/opt/ows_preprocessing/resilipipe/resilipipe"

EXPOSE 9000

RUN mkdir -p resilipipe/resilipipe/conf

COPY resilipipe/pyproject.toml resilipipe/
COPY resilipipe/resilipipe/__init__.py /opt/ows_preprocessing/resilipipe/resilipipe/
COPY resilipipe/resilipipe/conf/config.py resilipipe/resilipipe/conf/modules.yaml /opt/ows_preprocessing/resilipipe/resilipipe/conf/
COPY resilipipe/resilipipe/parse/ /opt/ows_preprocessing/resilipipe/resilipipe/parse/
COPY resources/ /opt/ows_preprocessing/resources/

RUN apt-get update && apt-get install -y git
RUN python -m pip install git+https://github.com/terrierteam/pyterrier-quality

RUN rm /root/.ir_datasets
RUN rm /root/.pyterrier
RUN rm /root/.cache/huggingface

RUN python -m pip install ./resilipipe
RUN python3 resilipipe/resilipipe/parse/prepare_modules.py

ENTRYPOINT ["python3", "resilipipe/resilipipe/parse/parse_single_warc.py"]