FROM debian:bullseye-slim
ARG PYTHON_VERSION=3.8.14
ENV PYTHON_VERSION $PYTHON_VERSION

RUN apt update && apt upgrade -y \
 && apt install sudo locales -y \
 && echo "tzdata tzdata/Areas select Europe" | debconf-set-selections \
 && echo "tzdata tzdata/Zones/Europe select Warsaw" | debconf-set-selections \
 && rm -f /etc/localtime /etc/timezone \
 && dpkg-reconfigure -f noninteractive tzdata \
 && rm -rf /var/lib/apt/lists/* && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
 && export LANG=en_US.utf8 \
 && adduser airflow \
 && echo "airflow ALL=NOPASSWD: ALL" > /etc/sudoers.d/airflow

USER airflow

RUN sudo apt update -y \
 && sudo apt install --no-install-recommends freetds-bin libffi-dev libsasl2-2 libsasl2-modules libssl-dev locales lsb-release sasl2-bin sqlite3 unixodbc -y \
 && sudo apt install python3 python3-pip -y

RUN sudo apt install curl -y \
 && export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s` \
 && echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list \
 && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add - \
 && sudo apt update -y \
 && sudo apt install gcsfuse -y

RUN sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
xz-utils tk-dev libffi-dev liblzma-dev git -y

RUN curl -L https://raw.githubusercontent.com/pyenv/pyenv-installer/master/bin/pyenv-installer | bash

RUN echo 'export PYENV_ROOT="$HOME/.pyenv"' >> $HOME/.bash_profile
RUN echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> $HOME/.bash_profile
RUN echo 'eval "$(pyenv init -)"' >> $HOME/.bash_profile
#RUN echo 'eval "$(pyenv virtualenv-init -)"' >> $HOME/.bash_profile

# to source $HOME/.bash_profile
SHELL ["/bin/bash", "-c", "-l"]

#RUN source $HOME/.bash_profile \
# && pyenv install $PYTHON_VERSION
# && pyenv virtualenv $PYTHON_VERSION airflow-1.10.15-python-$PYTHON_VERSION \
# && pyenv activate airflow-1.10.15-python-$PYTHON_VERSION \
# && python3 -V

#RUN exit 1


RUN source $HOME/.bash_profile \
 && pyenv install $PYTHON_VERSION \
 && pyenv global $PYTHON_VERSION \
 && python3 -V \
 && pip3 -V \
 && AIRFLOW_VERSION=1.10.15 \
 && PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)" \
 && CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
 && pip3 install --use-deprecated=legacy-resolver "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-backport-providers-apache-beam==2021.3.13 apache-airflow-backport-providers-google==2021.3.3 --constraint "${CONSTRAINT_URL}"

RUN source /home/airflow/.bash_profile \
 && pyenv global $PYTHON_VERSION \
 && PATH=$PATH:/home/airflow/.local/bin \
 && airflow db init

COPY entrypoint.sh /entrypoint.sh

RUN sudo chown airflow:airflow /entrypoint.sh && sudo chmod u+x /entrypoint.sh

ENV PATH="${PATH}:/home/airflow/.local/bin"

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]

#RUN sudo apt install openjdk-17-jdk -y && java -version
