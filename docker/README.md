# Development Environment `dderl` Image

This image supports the use of a Docker container for the further development of `dderl` in a Debian environment. 

## 1 Introduction

## 2. Creating a new `dderl_dev` Docker container

## 2.1 Getting started

    > REM Assuming the path prefix for the local repository mapping is //C/dderl
    > docker run -it -p 443:8443 \
            --name dderl_dev \
            -v //C/dderl:dderl \
            konnexionsgmbh/dderl_dev
            
    > REM Stopping the container
    > docker stop dderl_dev
    
    > REM Restarting the container
    > docker start dderl_dev

    > REM Entering a running container
    > docker exec -it dderl_dev

## 2.2 Detailed Syntax

A new container can be created with the `docker run` command.

##### Syntax:

    docker run -it 
               [-p <port>:8443] \
               [--name <container_name>] \
               [-v <directory_repository>:dderl] \
               konnexionsgmbh/dderl_dev 
               <cmd>
 
##### Parameters:

- **port** - an optional listener port             
- **container_name** - an optional container identification 
- **directory_repository** - an optional local repository directory - the default value is expecting the repository inside the container 
- **cmd** - the command to be executed in the container, e.g. `bash` for running the `bash` shell

Detailed documentation for the command `docker run` can be found [here](https://docs.docker.com/engine/reference/run/).

##### Examples:

1. Creating a new Docker container named `dderl_dev` using a repository inside the Docker container:  

    `docker run -it --name dderl_dev konnexionsgmbh/dderl_dev`

2. Creating a new Docker container named `dderl_dev` using the local repository in the local Windows directory `D:\SoftDevelopment\Projects\Konnexions\dderl_idea\dderl`:  

    `docker run -it --name dderl_dev -v //D/SoftDevelopment/Projects/Konnexions/dderl_idea/dderl:/dderl konnexionsgmbh/dderl_dev`

2. Creating a new Docker container named `dderl_dev` using the local repository in the local Linux directory `/dderl` and mapping port `8443` to port `8000`:  

    `docker run -it --name dderl_dev -p 8000:8443 -v /dderl:/dderl konnexionsgmbh/dderl_dev`

## 3 Working with an existing `dderl_dev` Docker container

### 3.1 Starting a stopped container

A previously stopped container can be started with the `docker start` command.

##### Syntax:

    docker start <container_name>

##### Parameter:

- **container_name** - the mandatory container identification, that is an UUID long identifier, an UUID short identifier or a previously given name 

Detailed documentation for the command `docker start` can be found [here](https://docs.docker.com/engine/reference/commandline/start/).

### 3.2 Entering a running container

A running container can be entered with the `docker exec` command.

##### Syntax:

    docker exec -it <container_name> <cmd>

##### Parameter:

- **container_name** - the mandatory container identification, that is an UUID long identifier, an UUID short identifier or a previously given name 
- **cmd** - the command to be executed in the container, e.g. `bash` for running the `bash` shell

Detailed documentation for the command `docker exec` can be found [here](https://docs.docker.com/engine/reference/commandline/exec/).

## 4 Working inside a running Docker container

### 4.1 `dderl` development

Inside the Docker container you can either clone a `dderl` repository or switch to an existing `dderl` repository. 
If a Docker container with an Oracle database is located on the host computer it can be accessed by using the IP address of the host computer.
Any `dderl` script can be executed inside the Docker container, for example:

    rebar3 compile
    rebar3 as prod release
    ./start.sh 
    
The following port numbers are exposed and can be mapped if necessary:

    1236
    7000-7020
    8125
    8443
    9443    

### 4.2 Available software

The Docker Image is based on the latest official Erlang Image on Docker Hub, which is currently `22.3-slim`.
With the following command you can check which other software versions are included in the Docker image:

    apt list --installed
