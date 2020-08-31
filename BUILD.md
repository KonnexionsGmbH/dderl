Buidling DDErl.
=====
``
### Table of Contents

**[1. Prerequisites](#prerequisites)**<br>
**[2. Building DDErl](#buildinf_dderl)**<br>
**[2.1 On Operating System Level](#on_operating_system_level)**<br>
**[2.2 Using Docker Containers](#using_docker_containers)**<br>

----

## <a name="prerequisites"></a> 1. Prerequisites

Building DDErl is only supported for Unix and similar systems.
All instructions have been tested with Ubuntu 20.04 (Focal Fossa). 

The following software components are required in addition to a Unix operating system:

| Component | from Version  | Source                                                   |
| --------- | ------------- | -------------------------------------------------------- |
| Erlang    | OTP 23.0      | https://www.erlang-solutions.com/resources/download.html |
| gcc       | 9.3.0         | https://gcc.gnu.org/                                     |
| git       | 2.28.0        | https://git-scm.com/                                     | 
| GNU make  | 4.2.1         | https://www.gnu.org/software/make/                       |
| rebar3    | V3.13.2       | https://www.rebar3.org/                                  |
| Yarn      | 1.22.5        | https://yarnpkg.com                                      |

## <a name="buildinf_dderl"></a> 2. Building DDErl

#### 1. Download the DDErl repository from GitHub:

`git clone https://github.com/KonnexionsGmbH/dderl `

#### 2. Change to the DDErl directory

`cd dderl`

#### 3. Install all the dependencies for DDErl:

`
cd priv/dev
yarn install-build-prod
`

#### 4. Build

- either backend and frontend:

`rebar3 as ui compile`

- or backend only:

`rebar3 compile`

- or frontend only:

`bash ./build_fe.sh`

## <a name="on_operating_system_level"></a> 2.1 On Operating System Level

xxxx

## <a name="using_docker_containers"></a> 2.2 Using Docker Containers

xxxx
