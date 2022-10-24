FROM debian:bullseye-slim

ARG MYARG=abc
ENV MYENV def
ENV MYENV_FROM_ARG $MYARG

# build an image layer
RUN echo MYARG=$MYARG
RUN echo MYENV=$MYENV

# execute on container start
#CMD ["echo", "MYARG=$MYARG"] #MYARG=$MYARG
#CMD ["echo", "MYENV=$MYENV"] #MYENV=$MYENV
#CMD ["sh", "-c", "echo MYARG=$MYARG"] #MYARG=
#CMD ["sh", "-c", "echo MYENV=$MYENV"] #MYENV=def
#CMD ["sh", "-c", "echo MYENV_FROM_ARG=$MYENV_FROM_ARG"] #MYENV_FROM_ARG=abc

#ENTRYPOINT echo "MYARG=$MYARG" #MYARG=
#ENTRYPOINT echo "MYENV=$MYENV" #MYENV=def
#ENTRYPOINT echo "MYENV_FROM_ARG=$MYENV_FROM_ARG" #MYENV_FROM_ARG=abc

RUN apt-get update -y && apt-get install sudo -y

#RUN adduser airflow && echo "airflow ALL=NOPASSWD: ALL" > /etc/sudoers.d/airflow
RUN adduser airflow -ge "" --disabled-password && echo "airflow ALL=NOPASSWD: ALL" > /etc/sudoers.d/airflow

USER airflow

RUN echo USER=$USER
RUN echo HOME=$HOME

ENV MYBASHVAR=ghi

#RUN echo "MYBASHVAR=ghi" >> $HOME/.bash_profile 
# entrypoint.sh requires: source ~/.bash_profile

#/entrypoint.sh: HOME=/home/airflow
#/entrypoint.sh: USER=
#/entrypoint.sh: MYARG=
#/entrypoint.sh: MYENV=def
#/entrypoint.sh: MYBASHVAR=ghi


COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
