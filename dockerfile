FROM public.ecr.aws/lambda/python:3.11

# Install system dependencies
RUN yum install -y gcc gcc-c++ make && \
    yum clean all

# Copy requirements
COPY src/forecaster/requirements.txt ${LAMBDA_TASK_ROOT}/

# Install Python dependencies first
RUN pip install --no-cache-dir --timeout=300 -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy forecaster source
COPY src/forecaster/forecaster.py ${LAMBDA_TASK_ROOT}/

# Lambda handler entrypoint
CMD ["forecaster.handler"]