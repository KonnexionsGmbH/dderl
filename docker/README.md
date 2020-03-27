# Development Environment `dderl` Image

This image supports the use of a Docker container for the further development of `dderl` in a Debian environment. 

## 1 Introduction

## 2. Creating a new `dderl` Docker container

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

## 3 Working with an existing `dderl` Docker container

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
    8443
    9443    

### 4.2 Available software

The Docker Image is based on the latest official Erlang Image on Docker Hub, which is currently `22.3-slim`.
The following software components are thus available for development (`apt list --installed`):

    adduser/stable,now 3.118 all [installed,automatic]
    alien/stable,now 8.95 all [installed]
    apt/stable,now 1.8.2 amd64 [installed,automatic]
    autoconf/stable,now 2.69-11 all [installed]
    automake/stable,now 1:1.16.1-4 all [installed,automatic]
    autopoint/stable,now 0.19.8.1-9 all [installed,automatic]
    autotools-dev/stable,now 20180224.1 all [installed]
    base-files/stable,now 10.3+deb10u3 amd64 [installed,automatic]
    base-passwd/stable,now 3.5.46 amd64 [installed,automatic]
    bash/stable,now 5.0-4 amd64 [installed,automatic]
    binutils-common/stable,now 2.31.1-16 amd64 [installed,automatic]
    binutils-x86-64-linux-gnu/stable,now 2.31.1-16 amd64 [installed,automatic]
    binutils/stable,now 2.31.1-16 amd64 [installed,automatic]
    bsdmainutils/stable,now 11.1.2+b1 amd64 [installed,automatic]
    bsdutils/stable,now 1:2.33.1-0.1 amd64 [installed,automatic]
    build-essential/stable,now 12.6 amd64 [installed]
    bzip2/stable,now 1.0.6-9.2~deb10u1 amd64 [installed,automatic]
    ca-certificates/stable,now 20190110 all [installed,automatic]
    coreutils/stable,now 8.30-3 amd64 [installed,automatic]
    cpio/stable,now 2.12+dfsg-9 amd64 [installed,automatic]
    cpp-8/stable,now 8.3.0-6 amd64 [installed,automatic]
    cpp/stable,now 4:8.3.0-1 amd64 [installed,automatic]
    curl/stable,now 7.64.0-4+deb10u1 amd64 [installed]
    dash/stable,now 0.5.10.2-5 amd64 [installed,automatic]
    dbus/stable,now 1.12.16-1 amd64 [installed,automatic]
    debconf/stable,now 1.5.71 all [installed,automatic]
    debhelper/stable,now 12.1.1 all [installed,automatic]
    debian-archive-keyring/stable,now 2019.1 all [installed,automatic]
    debianutils/stable,now 4.8.6.1 amd64 [installed,automatic]
    debugedit/stable,now 4.14.2.1+dfsg1-1 amd64 [installed,automatic]
    dh-autoreconf/stable,now 19 all [installed,automatic]
    dh-strip-nondeterminism/stable,now 1.1.2-1 all [installed,automatic]
    diffutils/stable,now 1:3.7-3 amd64 [installed,automatic]
    dirmngr/stable,now 2.2.12-1+deb10u1 amd64 [installed,automatic]
    dos2unix/stable,now 7.4.0-1 amd64 [installed]
    dpkg-dev/stable,now 1.19.7 all [installed,automatic]
    dpkg/stable,now 1.19.7 amd64 [installed,automatic]
    dwz/stable,now 0.12-3 amd64 [installed,automatic]
    e2fsprogs/stable,now 1.44.5-1+deb10u3 amd64 [installed,automatic]
    fakeroot/stable,now 1.23-1 amd64 [installed,automatic]
    fdisk/stable,now 2.33.1-0.1 amd64 [installed,automatic]
    file/stable,stable,now 1:5.35-4+deb10u1 amd64 [installed,automatic]
    findutils/stable,now 4.6.0+git+20190209-2 amd64 [installed,automatic]
    g++-8/stable,now 8.3.0-6 amd64 [installed,automatic]
    g++/stable,now 4:8.3.0-1 amd64 [installed,automatic]
    gcc-8-base/stable,now 8.3.0-6 amd64 [installed,automatic]
    gcc-8/stable,now 8.3.0-6 amd64 [installed,automatic]
    gcc/stable,now 4:8.3.0-1 amd64 [installed,automatic]
    gettext-base/stable,now 0.19.8.1-9 amd64 [installed,automatic]
    gettext/stable,now 0.19.8.1-9 amd64 [installed,automatic]
    git-man/stable,stable,now 1:2.20.1-2+deb10u1 all [installed,automatic]
    git/stable,stable,now 1:2.20.1-2+deb10u1 amd64 [installed]
    gnupg-l10n/stable,now 2.2.12-1+deb10u1 all [installed,automatic]
    gnupg-utils/stable,now 2.2.12-1+deb10u1 amd64 [installed,automatic]
    gnupg/stable,now 2.2.12-1+deb10u1 all [installed,automatic]
    gpg-agent/stable,now 2.2.12-1+deb10u1 amd64 [installed,automatic]
    gpg-wks-client/stable,now 2.2.12-1+deb10u1 amd64 [installed,automatic]
    gpg-wks-server/stable,now 2.2.12-1+deb10u1 amd64 [installed,automatic]
    gpg/stable,now 2.2.12-1+deb10u1 amd64 [installed,automatic]
    gpgconf/stable,now 2.2.12-1+deb10u1 amd64 [installed,automatic]
    gpgsm/stable,now 2.2.12-1+deb10u1 amd64 [installed,automatic]
    gpgv/stable,now 2.2.12-1+deb10u1 amd64 [installed,automatic]
    grep/stable,now 3.3-1 amd64 [installed,automatic]
    groff-base/stable,now 1.22.4-3 amd64 [installed,automatic]
    gzip/stable,now 1.9-3 amd64 [installed,automatic]
    hostname/stable,now 3.21 amd64 [installed,automatic]
    htop/stable,now 2.2.0-1+b1 amd64 [installed]
    init-system-helpers/stable,now 1.56+nmu1 all [installed,automatic]
    intltool-debian/stable,now 0.35.0+20060710.5 all [installed,automatic]
    iproute2/stable,now 4.20.0-2 amd64 [installed]
    iputils-ping/stable,now 3:20180629-2 amd64 [installed]
    krb5-locales/stable,now 1.17-3 all [installed,automatic]
    less/stable,now 487-0.1+b1 amd64 [installed,automatic]
    libacl1/stable,now 2.2.53-4 amd64 [installed,automatic]
    libalgorithm-diff-perl/stable,now 1.19.03-2 all [installed,automatic]
    libalgorithm-diff-xs-perl/stable,now 0.04-5+b1 amd64 [installed,automatic]
    libalgorithm-merge-perl/stable,now 0.08-3 all [installed,automatic]
    libapparmor1/stable,now 2.13.2-10 amd64 [installed,automatic]
    libapt-pkg5.0/stable,now 1.8.2 amd64 [installed,automatic]
    libarchive-cpio-perl/stable,now 0.10-1 all [installed,automatic]
    libarchive-zip-perl/stable,now 1.64-1 all [installed,automatic]
    libarchive13/stable,stable,now 3.3.3-4+deb10u1 amd64 [installed,automatic]
    libasan5/stable,now 8.3.0-6 amd64 [installed,automatic]
    libassuan0/stable,now 2.5.2-1 amd64 [installed,automatic]
    libatomic1/stable,now 8.3.0-6 amd64 [installed,automatic]
    libattr1/stable,now 1:2.4.48-4 amd64 [installed,automatic]
    libaudit-common/stable,now 1:2.8.4-3 all [installed,automatic]
    libaudit1/stable,now 1:2.8.4-3 amd64 [installed,automatic]
    libbinutils/stable,now 2.31.1-16 amd64 [installed,automatic]
    libblkid1/stable,now 2.33.1-0.1 amd64 [installed,automatic]
    libbsd0/stable,now 0.9.1-2 amd64 [installed,automatic]
    libbz2-1.0/stable,now 1.0.6-9.2~deb10u1 amd64 [installed,automatic]
    libc-ares2/stable,now 1.14.0-1 amd64 [installed,automatic]
    libc-bin/stable,now 2.28-10 amd64 [installed,automatic]
    libc-dev-bin/stable,now 2.28-10 amd64 [installed,automatic]
    libc6-dev/stable,now 2.28-10 amd64 [installed,automatic]
    libc6/stable,now 2.28-10 amd64 [installed,automatic]
    libcap-ng0/stable,now 0.7.9-2 amd64 [installed,automatic]
    libcap2-bin/stable,now 1:2.25-2 amd64 [installed,automatic]
    libcap2/stable,now 1:2.25-2 amd64 [installed,automatic]
    libcc1-0/stable,now 8.3.0-6 amd64 [installed,automatic]
    libcom-err2/stable,now 1.44.5-1+deb10u3 amd64 [installed,automatic]
    libcroco3/stable,now 0.6.12-3 amd64 [installed,automatic]
    libcurl3-gnutls/stable,now 7.64.0-4+deb10u1 amd64 [installed,automatic]
    libcurl4/stable,now 7.64.0-4+deb10u1 amd64 [installed,automatic]
    libdb5.3/stable,now 5.3.28+dfsg1-0.5 amd64 [installed,automatic]
    libdbus-1-3/stable,now 1.12.16-1 amd64 [installed,automatic]
    libdebconfclient0/stable,now 0.249 amd64 [installed,automatic]
    libdpkg-perl/stable,now 1.19.7 all [installed,automatic]
    libdw1/stable,now 0.176-1.1 amd64 [installed,automatic]
    libedit2/stable,now 3.1-20181209-1 amd64 [installed,automatic]
    libelf1/stable,now 0.176-1.1 amd64 [installed,automatic]
    liberror-perl/stable,now 0.17027-2 all [installed,automatic]
    libevent-2.1-6/stable,now 2.1.8-stable-4 amd64 [installed,automatic]
    libexpat1/stable,stable,now 2.2.6-2+deb10u1 amd64 [installed,automatic]
    libext2fs2/stable,now 1.44.5-1+deb10u3 amd64 [installed,automatic]
    libfakeroot/stable,now 1.23-1 amd64 [installed,automatic]
    libfdisk1/stable,now 2.33.1-0.1 amd64 [installed,automatic]
    libffi6/stable,now 3.2.1-9 amd64 [installed,automatic]
    libfile-fcntllock-perl/stable,now 0.22-3+b5 amd64 [installed,automatic]
    libfile-stripnondeterminism-perl/stable,now 1.1.2-1 all [installed,automatic]
    libgcc-8-dev/stable,now 8.3.0-6 amd64 [installed,automatic]
    libgcc1/stable,now 1:8.3.0-6 amd64 [installed,automatic]
    libgcrypt20/stable,now 1.8.4-5 amd64 [installed,automatic]
    libgdbm-compat4/stable,now 1.18.1-4 amd64 [installed,automatic]
    libgdbm6/stable,now 1.18.1-4 amd64 [installed,automatic]
    libglib2.0-0/stable,now 2.58.3-2+deb10u2 amd64 [installed,automatic]
    libglib2.0-data/stable,now 2.58.3-2+deb10u2 all [installed,automatic]
    libgmp10/stable,now 2:6.1.2+dfsg-4 amd64 [installed,automatic]
    libgnutls30/stable,now 3.6.7-4+deb10u2 amd64 [installed,automatic]
    libgomp1/stable,now 8.3.0-6 amd64 [installed,automatic]
    libgpg-error0/stable,now 1.35-1 amd64 [installed,automatic]
    libgpm2/stable,now 1.20.7-5 amd64 [installed,automatic]
    libgssapi-krb5-2/stable,now 1.17-3 amd64 [installed,automatic]
    libhogweed4/stable,now 3.4.1-1 amd64 [installed,automatic]
    libicu63/stable,now 63.1-6 amd64 [installed,automatic]
    libidn2-0/stable,stable,now 2.0.5-1+deb10u1 amd64 [installed,automatic]
    libisl19/stable,now 0.20-2 amd64 [installed,automatic]
    libitm1/stable,now 8.3.0-6 amd64 [installed,automatic]
    libk5crypto3/stable,now 1.17-3 amd64 [installed,automatic]
    libkeyutils1/stable,now 1.6-6 amd64 [installed,automatic]
    libkrb5-3/stable,now 1.17-3 amd64 [installed,automatic]
    libkrb5support0/stable,now 1.17-3 amd64 [installed,automatic]
    libksba8/stable,now 1.3.5-2 amd64 [installed,automatic]
    libldap-2.4-2/stable,now 2.4.47+dfsg-3+deb10u1 amd64 [installed,automatic]
    libldap-common/stable,now 2.4.47+dfsg-3+deb10u1 all [installed,automatic]
    liblocale-gettext-perl/stable,now 1.07-3+b4 amd64 [installed,automatic]
    liblsan0/stable,now 8.3.0-6 amd64 [installed,automatic]
    libltdl-dev/stable,now 2.4.6-9 amd64 [installed,automatic]
    libltdl7/stable,now 2.4.6-9 amd64 [installed,automatic]
    liblua5.2-0/stable,now 5.2.4-1.1+b2 amd64 [installed,automatic]
    liblz4-1/stable,now 1.8.3-1 amd64 [installed,automatic]
    liblzma5/stable,now 5.2.4-1 amd64 [installed,automatic]
    libmagic-mgc/stable,stable,now 1:5.35-4+deb10u1 amd64 [installed,automatic]
    libmagic1/stable,stable,now 1:5.35-4+deb10u1 amd64 [installed,automatic]
    libmail-sendmail-perl/stable,now 0.80-1 all [installed,automatic]
    libmnl0/stable,now 1.0.4-2 amd64 [installed,automatic]
    libmount1/stable,now 2.33.1-0.1 amd64 [installed,automatic]
    libmpc3/stable,now 1.1.0-1 amd64 [installed,automatic]
    libmpfr6/stable,now 4.0.2-1 amd64 [installed,automatic]
    libmpx2/stable,now 8.3.0-6 amd64 [installed,automatic]
    libncurses6/stable,now 6.1+20181013-2+deb10u2 amd64 [installed,automatic]
    libncursesw6/stable,now 6.1+20181013-2+deb10u2 amd64 [installed,automatic]
    libnettle6/stable,now 3.4.1-1 amd64 [installed,automatic]
    libnghttp2-14/stable,stable,now 1.36.0-2+deb10u1 amd64 [installed,automatic]
    libnode64/stable,now 10.15.2~dfsg-2 amd64 [installed,automatic]
    libnpth0/stable,now 1.6-1 amd64 [installed,automatic]
    libnspr4/stable,now 2:4.20-1 amd64 [installed,automatic]
    libnss3/stable,stable,now 2:3.42.1-1+deb10u2 amd64 [installed,automatic]
    libodbc1/stable,now 2.3.6-0.1 amd64 [installed]
    libp11-kit0/stable,now 0.23.15-2 amd64 [installed,automatic]
    libpam-modules-bin/stable,now 1.3.1-5 amd64 [installed,automatic]
    libpam-modules/stable,now 1.3.1-5 amd64 [installed,automatic]
    libpam-runtime/stable,now 1.3.1-5 all [installed,automatic]
    libpam0g/stable,now 1.3.1-5 amd64 [installed,automatic]
    libpcre2-8-0/stable,now 10.32-5 amd64 [installed,automatic]
    libpcre3/stable,now 2:8.39-12 amd64 [installed,automatic]
    libperl5.28/stable,now 5.28.1-6 amd64 [installed,automatic]
    libpipeline1/stable,now 1.5.1-2 amd64 [installed,automatic]
    libpopt0/stable,now 1.16-12 amd64 [installed,automatic]
    libprocps7/stable,now 2:3.3.15-2 amd64 [installed,automatic]
    libpsl5/stable,now 0.20.2-2 amd64 [installed,automatic]
    libquadmath0/stable,now 8.3.0-6 amd64 [installed,automatic]
    libreadline7/stable,now 7.0-5 amd64 [installed,automatic]
    librpm8/stable,now 4.14.2.1+dfsg1-1 amd64 [installed,automatic]
    librpmbuild8/stable,now 4.14.2.1+dfsg1-1 amd64 [installed,automatic]
    librpmio8/stable,now 4.14.2.1+dfsg1-1 amd64 [installed,automatic]
    librpmsign8/stable,now 4.14.2.1+dfsg1-1 amd64 [installed,automatic]
    librtmp1/stable,now 2.4+20151223.gitfa8646d.1-2 amd64 [installed,automatic]
    libsasl2-2/stable,stable,now 2.1.27+dfsg-1+deb10u1 amd64 [installed,automatic]
    libsasl2-modules-db/stable,stable,now 2.1.27+dfsg-1+deb10u1 amd64 [installed,automatic]
    libsasl2-modules/stable,stable,now 2.1.27+dfsg-1+deb10u1 amd64 [installed,automatic]
    libsctp1/stable,now 1.0.18+dfsg-1 amd64 [installed]
    libseccomp2/stable,now 2.3.3-4 amd64 [installed,automatic]
    libselinux1/stable,now 2.8-1+b1 amd64 [installed,automatic]
    libsemanage-common/stable,now 2.8-2 all [installed,automatic]
    libsemanage1/stable,now 2.8-2 amd64 [installed,automatic]
    libsepol1/stable,now 2.8-1 amd64 [installed,automatic]
    libsigsegv2/stable,now 2.12-2 amd64 [installed,automatic]
    libsmartcols1/stable,now 2.33.1-0.1 amd64 [installed,automatic]
    libsqlite3-0/stable,now 3.27.2-3 amd64 [installed,automatic]
    libss2/stable,now 1.44.5-1+deb10u3 amd64 [installed,automatic]
    libssh2-1/stable,now 1.8.0-2.1 amd64 [installed,automatic]
    libssl1.1/stable,stable,now 1.1.1d-0+deb10u2 amd64 [installed]
    libstdc++-8-dev/stable,now 8.3.0-6 amd64 [installed,automatic]
    libstdc++6/stable,now 8.3.0-6 amd64 [installed,automatic]
    libsys-hostname-long-perl/stable,now 1.5-1 all [installed,automatic]
    libsystemd0/stable,now 241-7~deb10u3 amd64 [installed,automatic]
    libtasn1-6/stable,now 4.13-3 amd64 [installed,automatic]
    libtinfo6/stable,now 6.1+20181013-2+deb10u2 amd64 [installed,automatic]
    libtool/stable,now 2.4.6-9 all [installed,automatic]
    libtsan0/stable,now 8.3.0-6 amd64 [installed,automatic]
    libubsan1/stable,now 8.3.0-6 amd64 [installed,automatic]
    libuchardet0/stable,now 0.0.6-3 amd64 [installed,automatic]
    libudev1/stable,now 241-7~deb10u3 amd64 [installed,automatic]
    libunistring2/stable,now 0.9.10-1 amd64 [installed,automatic]
    libutempter0/stable,now 1.1.6-3 amd64 [installed,automatic]
    libuuid1/stable,now 2.33.1-0.1 amd64 [installed,automatic]
    libuv1/stable,now 1.24.1-1 amd64 [installed,automatic]
    libx11-6/stable,now 2:1.6.7-1 amd64 [installed,automatic]
    libx11-data/stable,now 2:1.6.7-1 all [installed,automatic]
    libxau6/stable,now 1:1.0.8-1+b2 amd64 [installed,automatic]
    libxcb1/stable,now 1.13.1-2 amd64 [installed,automatic]
    libxdmcp6/stable,now 1:1.1.2-3 amd64 [installed,automatic]
    libxext6/stable,now 2:1.3.3-1+b2 amd64 [installed,automatic]
    libxml2/stable,now 2.9.4+dfsg1-7+b3 amd64 [installed,automatic]
    libxmuu1/stable,now 2:1.1.2-2+b3 amd64 [installed,automatic]
    libxtables12/stable,now 1.8.2-4 amd64 [installed,automatic]
    libzstd1/stable,now 1.3.8+dfsg-3 amd64 [installed,automatic]
    linux-libc-dev/stable,now 4.19.98-1 amd64 [installed,automatic]
    login/stable,now 1:4.5-1.1 amd64 [installed,automatic]
    lsb-base/stable,now 10.2019051400 all [installed,automatic]
    m4/stable,now 1.4.18-2 amd64 [installed,automatic]
    make/stable,now 4.2.1-1.2 amd64 [installed,automatic]
    man-db/stable,now 2.8.5-2 amd64 [installed,automatic]
    manpages-dev/stable,now 4.16-2 all [installed,automatic]
    manpages/stable,now 4.16-2 all [installed,automatic]
    mawk/stable,now 1.3.3-17+b3 amd64 [installed,automatic]
    mount/stable,now 2.33.1-0.1 amd64 [installed,automatic]
    ncurses-base/stable,now 6.1+20181013-2+deb10u2 all [installed,automatic]
    ncurses-bin/stable,now 6.1+20181013-2+deb10u2 amd64 [installed,automatic]
    netbase/stable,now 5.6 all [installed,automatic]
    nodejs-doc/stable,now 10.15.2~dfsg-2 all [installed,automatic]
    nodejs/stable,now 10.15.2~dfsg-2 amd64 [installed,automatic]
    openssh-client/stable,now 1:7.9p1-10+deb10u2 amd64 [installed,automatic]
    openssl/stable,stable,now 1.1.1d-0+deb10u2 amd64 [installed]
    oracle-instantclient19.6-basic/now 19.6.0.0.0-2 amd64 [installed,local]
    passwd/stable,now 1:4.5-1.1 amd64 [installed,automatic]
    patch/stable,stable,now 2.7.6-3+deb10u1 amd64 [installed,automatic]
    perl-base/stable,now 5.28.1-6 amd64 [installed,automatic]
    perl-modules-5.28/stable,now 5.28.1-6 all [installed,automatic]
    perl/stable,now 5.28.1-6 amd64 [installed,automatic]
    pinentry-curses/stable,now 1.1.0-2 amd64 [installed,automatic]
    po-debconf/stable,now 1.0.21 all [installed,automatic]
    procps/stable,now 2:3.3.15-2 amd64 [installed]
    psmisc/stable,now 23.2-1 amd64 [installed,automatic]
    publicsuffix/stable,now 20190415.1030-1 all [installed,automatic]
    readline-common/stable,now 7.0-5 all [installed,automatic]
    rpm-common/stable,now 4.14.2.1+dfsg1-1 amd64 [installed,automatic]
    rpm2cpio/stable,now 4.14.2.1+dfsg1-1 amd64 [installed,automatic]
    rpm/stable,now 4.14.2.1+dfsg1-1 amd64 [installed,automatic]
    sed/stable,now 4.7-1 amd64 [installed,automatic]
    sensible-utils/stable,now 0.0.12 all [installed,automatic]
    shared-mime-info/stable,now 1.10-1 amd64 [installed,automatic]
    sysvinit-utils/stable,now 2.93-8 amd64 [installed,automatic]
    tar/stable,now 1.30+dfsg-6 amd64 [installed,automatic]
    tmux/stable,now 2.8-3 amd64 [installed]
    tzdata/stable,stable-updates,now 2019c-0+deb10u1 all [installed,automatic]
    util-linux/stable,now 2.33.1-0.1 amd64 [installed,automatic]
    vim-common/stable,now 2:8.1.0875-5 all [installed,automatic]
    vim-runtime/stable,now 2:8.1.0875-5 all [installed,automatic]
    vim/stable,now 2:8.1.0875-5 amd64 [installed]
    wget/stable,now 1.20.1-1.1 amd64 [installed]
    xauth/stable,now 1:1.0.10-1 amd64 [installed,automatic]
    xdg-user-dirs/stable,now 0.17-2 amd64 [installed,automatic]
    xxd/stable,now 2:8.1.0875-5 amd64 [installed,automatic]
    xz-utils/stable,now 5.2.4-1 amd64 [installed,automatic]
    yarn/stable,stable,now 1.22.4-1 all [installed]
    zlib1g/stable,now 1:1.2.11.dfsg-1 amd64 [installed,automatic]
