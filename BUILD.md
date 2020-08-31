Buidling DDErl.
=====
``
### Table of Contents

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

#### 1. Download the DDErl repository from GitHub:

    git clone https://github.com/KonnexionsGmbH/dderl`

#### 2. Change to the DDErl directory

    cd dderl

#### 3. Install all the dependencies for DDErl:

    cd priv/dev
    yarn install-build-prod

#### 4. Build

##### 4.1 either backend and frontend:

    rebar3 as ui compile

##### 4.2 or backend only:

    rebar3 compile

##### 4.3 or frontend only:

    bash ./build_fe.sh

## <a name="building_using_docker_containers"></a> 2.2 Building Using Docker Containers

xxxx
