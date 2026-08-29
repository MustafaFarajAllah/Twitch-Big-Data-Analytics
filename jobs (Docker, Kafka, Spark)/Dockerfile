# This Dockerfile creates a Docker image for running a Spark job on a cluster.
# It installs system dependencies, Python dependencies, and prepares a Spark user.

# Use a lightweight Debian 12 base image
FROM debian:12-slim

# Prevent Debian from asking interactive installation questions
ENV DEBIAN_FRONTEND=noninteractive

# Install required Linux packages:
# procps: provides process tools for Spark runtime
# tini: small init process that handles whats inside containers
# libjemalloc2: memory allocator that can improve performance
# Then clean apt cache to reduce image size
RUN apt-get update && apt-get install -y procps tini libjemalloc2 && \
    rm -rf /var/lib/apt/lists/*

# Preload jemalloc as the memory allocator
# This can improve memory behavior/performance for Spark workloads
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

# Set Miniforge installation location (V for LDU the pa)
ENV CONDA_HOME=/opt/miniforge3

# Tell PySpark which Python executable to use
ENV PYSPARK_PYTHON=${CONDA_HOME}/bin/python

# Add Conda/Miniforge binaries to the system PATH
ENV PATH=${CONDA_HOME}/bin:${PATH}

# Download the latest Miniforge installer for Linux x86_64
ADD https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh .

# Install Miniforge silently into /opt/miniforge3
# Configure Conda:
# always_yes True: automatically answer yes to installs
# auto_update_conda False: prevent automatic Conda updates
# channel_priority strict: prefer packages from the selected channel consistently
RUN bash Miniforge3-Linux-x86_64.sh -b -p /opt/miniforge3 \
  && ${CONDA_HOME}/bin/conda config --system --set always_yes True \
  && ${CONDA_HOME}/bin/conda config --system --set auto_update_conda False \
  && ${CONDA_HOME}/bin/conda config --system --set channel_priority strict

# Install Python libraries needed by the Spark preprocessing code
# emoji: remove emojis from text
# langcodes: convert language codes to language names
# scipy, pandas, numpy: common data processing/scientific libraries
RUN ${CONDA_HOME}/bin/mamba install -n base \
    emoji \ langcodes \ scipy \ pandas \numpy

# Install unicodedata2 using pip
# Used for Unicode character handling (emojies)
RUN ${PYSPARK_PYTHON} -m pip install unicodedata2

# Create a Linux group named spark with group ID 1099
# Create a Linux user named spark with user ID 1099
# Set /home/spark as the user's home directory
RUN groupadd -g 1099 spark && \
    useradd -u 1099 -g 1099 -d /home/spark -m spark

# Run the container as the non-root spark user for better security
USER spark
