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

### 2.2.1. Building DDErl with Dockrer Compose in the DDErl root directory

This command installs an executable DDErl:

    docker-compose up -d
    
**Sample output:**    
    
![](priv/.BUILD_images/compose_up.png)

The following processing steps are performed:
1. If not already there, download the Oracle database image and create the container `kxn_db_ora` with an Oracle database (currently 19c).
1. If not yet available, download the Konnexion development image and create the corresponding container `kxn_dev`.
1. Both containers are assigned to network `dderl_kxn_net`.- 
1. After the database is ready, the schema `scott` is created with the password `tiger`. 
1. The repository `https://github.com/KonnexionsGmbH/dderl` is downloaded from Github.
1. The frontend to `DDErl` is created.
1. `DDErl` is compiled and started
   
### 2.2.2. Building DDErl manually

#### 2.2.2.1 Enter the Konnexions development container:

    docker exec -it kxn_dev bash
    
**Sample output:**    
    
![](priv/.BUILD_images/docker_exec.png)    

Inside the  development container `kxn_dev` the database container `kxn_db_ora` can be addressed with the `kxn_db_ora` as hostname:  

    ping kxn_db_ora

![](priv/.BUILD_images/ping.png)

#### 2.2.2.2 Optionally the database can be set up
    
    sqlplus sys/oracle@kxn_db_ora:1521/orclpdb1 as sysdba
    
![](priv/.BUILD_images/sqlplus_1.png)
    
    create user scott identified by tiger;
    grant alter system to scott;
    grant create session to scott;
    grant unlimited tablespace to scott;
    grant create table to scott;
    grant create view to scott;
    exit

![](priv/.BUILD_images/sqlplus_2.png) 

#### 2.2.2.3 Next you need to download the DDErl repository from GitHub:

    git clone https://github.com/KonnexionsGmbH/dderl
    cd dderl
    
**Sample output:**   
 
![](priv/.BUILD_images/git_clone.png)

#### 2.2.2.4 Then the dependencies of DDErl have to be satisfied:

    ./build_fe.sh
    
**Sample output - start:**    
    
![](priv/.BUILD_images/yarn_start.png)    
    
**Sample output - end:**    
    
![](priv/.BUILD_images/yarn_end.png)

#### 2.2.2.5 Now you can either execute one of the commands from section 2.1 point 4 or start DDErl directly with `rebar3 shell`:

    rebar3 shell
    
**Sample output - start:**    
    
![](priv/.BUILD_images/rebar3_shell_start.png)

**Sample output - end:**    
    
![](priv/.BUILD_images/rebar3_shell_end.png)

### 2.2.3 Finally DDErl is ready and can be operated via a Browser

#### Login screen:

![](priv/.BUILD_images/Login.png)

User: `system` Password: `change_on_install`

#### Database connection:

|           |                  |
| ---       | ---              |
| Service   | **`orclpdb1`**   |
| Host / IP | **`kxn_db_ora`** |
| Port      | **`1521`**       |
| User      | **`scott`**      |
| Password  | **`tiger`**      |

![](priv/.BUILD_images/Connect.png)

##### Start browsing:

![](priv/.BUILD_images/Result.png)
