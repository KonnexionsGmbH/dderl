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

### 1. Download the DDErl repository from GitHub:

    git clone https://github.com/KonnexionsGmbH/dderl`

### 2. Change to the DDErl directory

    cd dderl

### 3. Install all the dependencies for DDErl:

    cd priv/dev
    yarn install-build-prod

### 4. Build alternatively

#### 4.1 either backend and frontend:

    rebar3 as ui compile

#### 4.2 or backend only:

    rebar3 compile

#### 4.3 or frontend only:

    bash ./build_fe.sh

## <a name="building_using_docker_containers"></a> 2.2 Building Using Docker Containers

The use of Konnexions development image makes the build process independent of the host operating system.
The only requirement is the installation of Docker Desktop and possibly Docker Compose (Unix operating systems).
The following instructions demonstrate how to use the Docker compose script. 

### 1. Start Docker compose in the DDErl root directory

    docker-compose up -d
    
This command creates the network `dderl_kxn_net` and the two docker containers `kxn_dev` and `kxn_db_ora`:

    docker-compose up -d
    
Sample output:    
    
    Creating network "dderl_kxn_net" with the default driver
    Creating kxn_db_ora ... done
    Creating kxn_dev    ... done

If the Docker images are not yet available, Docker compose will load them from DockerHub.
     
### 2. Optionally the database can be set up

    docker exec -it kxn_db_ora bash
    
Sample database setup:    
    
    [oracle@accf872c2eae ~]$ sqlplus sys/oracle@localhost:1521/orclpdb1 as sysdba
    
    SQL*Plus: Release 19.0.0.0.0 - Production on Mon Aug 31 13:34:39 2020
    Version 19.3.0.0.0
    
    Copyright (c) 1982, 2019, Oracle.  All rights reserved.
        
    Connected to:
    Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production
    Version 19.3.0.0.0
    
    SQL> create user scott identified by tiger;
    
    User created.
    
    SQL> grant alter system to scott;
    grant create session to scott;
    grant unlimited tablespace to scott;
    grant create table to scott;
    grant create view to scott;
    
    Grant succeeded.
    
    SQL>
    Grant succeeded.
    
    SQL>
    Grant succeeded.
    
    SQL>
    Grant succeeded.
    
    SQL>
    Grant succeeded.
    
    SQL> exit
    Disconnected from Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production
    Version 19.3.0.0.0
    [oracle@accf872c2eae ~]$ exit
    exit

### 3. Building DDErl

#### 3.1 Enter the Konnexions development container:

    docker exec -it kxn_dev bash
    
Sample output:    
    
    root@7789897326da:/#    

#### 3.2 First you need to download the DDErl repository from GitHub:

    git clone https://github.com/KonnexionsGmbH/dderl
    
Sample output:    
    
    Cloning into 'dderl'...
    Username for 'https://github.com': walter-weinmann
    Password for 'https://walter-weinmann@github.com':
    remote: Enumerating objects: 318, done.
    remote: Counting objects: 100% (318/318), done.
    remote: Compressing objects: 100% (226/226), done.
    remote: Total 20351 (delta 210), reused 172 (delta 90), pack-reused 20033
    Receiving objects: 100% (20351/20351), 20.79 MiB | 13.26 MiB/s, done.
    Resolving deltas: 100% (14232/14232), done.
    root@7789897326da:/# cd dderl
    root@7789897326da:/dderl#

#### 3.3 Then the dependencies of DDErl have to be satisfied:

    cd priv/dev
    yarn install-build-prod
    
Sample output:    
    
    yarn run v1.22.4
    $ yarn install && yarn build-prod
    [1/4] Resolving packages...
    [2/4] Fetching packages...
    info fsevents@1.2.9: The platform "linux" is incompatible with this module.
    info "fsevents@1.2.9" is an optional dependency and failed compatibility check. Excluding it from installation.
    [3/4] Linking dependencies...
    [4/4] Building fresh packages...
    warning Your current version of Yarn is out of date. The latest version is "1.22.5", while you're on "1.22.4".
    info To upgrade, run the following command:
    $ sudo apt-get update && sudo apt-get install yarn
    $ yarn lint && yarn clean && webpack -p
    $ jshint --verbose static/index.js static/scripts static/dashboard static/dialogs static/graph static/slickgrid
    $ rimraf ../public/dist
    Hash: f7ac1b231bfa408ab853
    Version: webpack 3.10.0
    Time: 83638ms
                                                        Asset       Size  Chunks                    Chunk Names
                           ui-icons_222222_256x240.000cda.png    3.03 kB          [emitted]
                           ui-icons_454545_256x240.6448e8.png    3.03 kB          [emitted]
    ...                    
                                         editor.worker.js.map     920 kB      59  [emitted]         editor.worker
                                                 index.js.map    6.11 kB      60  [emitted]         index
      [44] (webpack)/buildin/global.js 509 bytes {53} {54} {55} {56} {57} {58} {59} [built]
      [76] ./static/dialogs/dialogs.js 7.12 kB {53} [built]
      [77] ./static/scripts/dderl.js 27.5 kB {53} [built]
     [140] ./static/jquery-ui-helper/helper.js 1.3 kB {53} [built]
     [271] ./static/scripts/login.js 14.5 kB {53} [built]
     [313] ./static/scripts/table-selection.js 798 bytes {53} [built]
     [354] ./static/scripts/connect.js 29.7 kB {53} [built]
     [485] ./static/scripts/dderl.sql.js 48.7 kB {53} [built]
     [589] ./static/index.js 3.93 kB {53} [built]
    [1371] ./static/styles/jquery-ui-smoothness/jquery-ui.css 1.05 kB {53} [built]
    [1379] ./static/styles/slick.grid.css 1.04 kB {53} [built]
    [1385] ./static/styles/slick.columnpicker.css 1.06 kB {53} [built]
    [1387] ./static/styles/dropdown.css 1.03 kB {53} [built]
    [1389] ./static/styles/dderl.sql.css 1.03 kB {53} [built]
    [1420] ./static/index.html 56 bytes {60} [built]
        + 1724 hidden modules
    Done in 133.18s.
    root@7789897326da:/dderl/priv/dev#

#### 3.4 Now you can either execute one of the commands from section 2.1 point 4 or start DDErl directly with `rebar3 shell`:

    cd ../..
    rebar3 shell
    
Sample output:    
    
    ===> Fetching rebar3_hex v6.9.6
    ===> Downloaded package, caching at /root/.cache/rebar3/hex/hexpm/packages/rebar3_hex-6.9.6.tar
    ===> Fetching hex_core v0.6.9
    ===> Downloaded package, caching at /root/.cache/rebar3/hex/hexpm/packages/hex_core-0.6.9.tar
    ===> Fetching verl v1.0.2
    ===> Downloaded package, caching at /root/.cache/rebar3/hex/hexpm/packages/verl-1.0.2.tar
    ===> Compiling verl
    ===> Compiling hex_core
    ===> Compiling rebar3_hex
    ...
    ===> Booted oranif
    ===> Booted prometheus
    ===> Booted dderl
    2020-08-31 15:58:42.591 [warning] [<0.5964.0>@dperl_cp:log_nodes_status:286] [dderl@127.0.0.1] job : Node added
    2020-08-31 15:58:46.592 [warning] [<0.5983.0>@dperl_cp:log_nodes_status:286] [dderl@127.0.0.1] service : Node added
    2020-08-31 15:58:49.669 [info] [imem<0.5606.0>@imem_snap:snap_log:928] snapshot created for ddView
    2020-08-31 15:58:49.701 [info] [imem<0.5606.0>@imem_snap:snap_log:928] snapshot created for ddCmd
    2020-08-31 15:58:49.746 [info] [imem<0.5606.0>@imem_snap:snap_log:928] snapshot created for ddAdapter
    2020-08-31 15:58:49.806 [info] [imem<0.5606.0>@imem_snap:snap_log:928] snapshot created for ddConfig
    2020-08-31 15:58:49.854 [info] [imem<0.5606.0>@imem_snap:snap_log:928] snapshot created for ddTable
    2020-08-31 15:58:49.888 [info] [imem<0.5606.0>@imem_snap:snap_log:928] snapshot created for ddAlias
    2020-08-31 15:59:10.180 [info] [imem<0.5606.0>@imem_snap:snap_log:928] snapshot created for ddConfig

#### 3.5 Finally DDErl is ready and can be operated via a Browser !!!
