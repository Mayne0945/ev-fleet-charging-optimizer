FROM public.ecr.aws/lambda/python:3.11

# 1. Install system dependencies
RUN yum install -y gcc gcc-c++ make tar gzip && \
    yum clean all

# 2. Pin NumPy below 2.0 to bypass the GCC compiler trap, THEN install cmdstanpy
RUN pip install --no-cache-dir "numpy<2.0.0" cmdstanpy==1.2.0

# 3. Forge the C++ Stan Engine
ENV CMDSTAN_DIR=/opt/cmdstan
RUN mkdir -p $CMDSTAN_DIR && \
    python -c "import cmdstanpy; cmdstanpy.install_cmdstan(dir='$CMDSTAN_DIR')"

# 4. Copy requirements
COPY src/forecaster/requirements.txt ${LAMBDA_TASK_ROOT}/

# 5. Force Prophet to compile the C++ model RIGHT NOW
ENV PROPHET_REBUILD_MODELS=1
RUN pip install --no-cache-dir --timeout=300 -r ${LAMBDA_TASK_ROOT}/requirements.txt

# 6. Ensure runtime operations use the writable /tmp directory
ENV STAN_BACKEND=CMDSTANPY
ENV TMPDIR=/tmp

# 7. THE SLEDGEHAMMER: Grant execute permissions to the Lambda ghost user
RUN chmod -R 777 /opt/cmdstan && \
    chmod -R 777 /var/lang/lib/python3.11/site-packages/prophet

# 8. Copy forecaster source
COPY src/forecaster/forecaster.py ${LAMBDA_TASK_ROOT}/

# 9. Lambda handler entrypoint
CMD ["forecaster.handler"]