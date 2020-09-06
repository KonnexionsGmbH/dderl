Buidling DDErl.
=====
``
## Table of Contents

**[1. Prerequisites](#prerequisites)**<br>
**[2. Building DDErl](#buildinf_dderl)**<br>
**[2.1 Building On Operating System Level](#building_on_operating_system_level)**<br>
**[2.2 Building Using Docker Containers](#building_using_docker_containers)**<br>

----

## <a name="prerequisites"></a> 1. Prerequisites

Building DDErl is only supported for Unix and similar systems.
All instructions have been tested with Ubuntu 20.04 (Focal Fossa). 

The following software components are required in addition to a Unix operating system:

| Component | From Version  | Source                                                   |
| --------- | ------------- | -------------------------------------------------------- |
| Erlang    | OTP 23.0      | https://www.erlang-solutions.com/resources/download.html |
| gcc       | 9.3.0         | https://gcc.gnu.org/                                     |
| git       | 2.28.0        | https://git-scm.com/                                     | 
| GNU make  | 4.2.1         | https://www.gnu.org/software/make/                       |
| rebar3    | V3.13.2       | https://www.rebar3.org/                                  |
| Yarn      | 1.22.5        | https://yarnpkg.com                                      |

## <a name="buildinf_dderl"></a> 2. Building DDErl

The build process can either be done directly on the operating system level or based on the Konnexions development image.
For the former, all the software components mentioned under section 1 must be installed, for the latter they are already pre-installed in the image.
In addition, a Docker compose script is available that combines the Konnexions development image with an empty Oracle database. 
This can be used as an easily customizable template.

## <a name="building_on_operating_system_level"></a> 2.1 Building On Operating System Level

### 2.1.1 Download the DDErl repository from GitHub:

    git clone https://github.com/KonnexionsGmbH/dderl`

### 2.1.2 Change to the DDErl directory

    cd dderl

### 2.1.3 Install all the dependencies for DDErl:

    cd priv/dev
    yarn install-build-prod

### 2.1.4 Build alternatively

#### either backend and frontend:

    rebar3 as ui compile

#### or backend only:

    rebar3 compile

#### or frontend only:

    bash ./build_fe.sh

## <a name="building_using_docker_containers"></a> 2.2 Building Using Docker Containers

The use of Konnexions development image makes the build process independent of the host operating system.
The only requirement is the installation of Docker Desktop and possibly Docker Compose (Unix operating systems).
The following instructions demonstrate how to use the Docker compose script. 

### 2.2.1. Start Docker compose in the DDErl root directory

This command creates the network `dderl_kxn_net` and the two docker containers `kxn_dev` and `kxn_db_ora`:

    docker-compose up -d
    
**Sample output:**    
    
![](priv/.BUILD_images/compose_up.png)

If the Docker images are not yet available, Docker compose will load them from DockerHub.
     
### 2.2.2. Optionally the database can be set up

    docker exec -it kxn_db_ora bash
    
**Sample database setup:**    
    
    sqlplus sys/oracle@kxn_db_ora:1521/orclpdb1 as sysdba
    
![](priv/.BUILD_images/sqlplus_1.png)
    
    create user scott identified by tiger;
    grant alter system to scott;
    grant create session to scott;
    grant unlimited tablespace to scott;
    grant create table to scott;
    grant create view to scott;

![](priv/.BUILD_images/sqlplus_2.png)
    
    exit
    
![](priv/.BUILD_images/sqlplus_3.png)

### 2.2.3. Building DDErl

#### 2.2.3.1 Enter the Konnexions development container:

    docker exec -it kxn_dev bash
    
**Sample output:**    
    
![](priv/.BUILD_images/docker_exec.png)    

Inside the  development container `kxn_dev` the database container `kxn_db_ora` can be addressed with the `kxn_db_ora` as hostname:  

    ping kxn_db_ora

![](priv/.BUILD_images/ping.png)

#### 2.2.3.2 First you need to download the DDErl repository from GitHub:

    git clone https://github.com/KonnexionsGmbH/dderl
    
**Sample output:**   
 
![](priv/.BUILD_images/git_clone.png)

#### 2.2.3.3 Then the dependencies of DDErl have to be satisfied:

    cd priv/dev
    yarn install-build-prod
    
**Sample output - start:**    
    
![](priv/.BUILD_images/yarn_start.png)    
    
**Sample output - end:**    
    
![](priv/.BUILD_images/yarn_end.png)

#### 2.2.3.4 Now you can either execute one of the commands from section 2.1 point 4 or start DDErl directly with `rebar3 shell`:

    cd ../..
    rebar3 shell
    
**Sample output - start:**    
    
![](priv/.BUILD_images/rebar3_shell_start.png)

**Sample output - end:**    
    
![](priv/.BUILD_images/rebar3_shell_end.png)

#### 2.2.3.5 Finally DDErl is ready and can be operated via a Browser

##### Login screen:

![](priv/.BUILD_images/Login.png)

User: `system` Password: `change_on_install`

##### Database connection:

![](priv/.BUILD_images/Connect.png)

##### Start browsing:

![](priv/.BUILD_images/Result.png)
