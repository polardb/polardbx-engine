Name: t-rds-galaxyengine-80
Version:8.0.18
Release: %(echo $RELEASE)%{?dist}
License: GPL
#URL: http://gitlab.alibaba-inc.com/rds_mysql/RDS_80
Group: applications/database
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildRequires: cmake >= 2.8.12
%if "%{?dist}" == ".alios7" || "%{?dist}" == ".el7"
BuildRequires: libarchive
BuildRequires: ncurses-devel
BuildRequires: bison
%define use_gcc system
%define with_7u ON
%else
BuildRequires: devtoolset-7-gcc
BuildRequires: devtoolset-7-gcc-c++
BuildRequires: devtoolset-7-binutils
BuildRequires: libaio-devel
%define use_gcc devtoolset
%define with_7u OFF
%endif
BuildRequires: zlib-devel
Packager: kongzhi.kz@alibaba-inc.com
Autoreq: no
#Source: %{name}-%{version}.tar.gz
Prefix: /opt/galaxy_engine
Summary: RDS MySQL GALAXYENGINE 8.0 based on Oracle MySQL 8.0

# force using gcc 7
%define use_gcc devtoolset
BuildRequires: devtoolset-7-gcc
BuildRequires: devtoolset-7-gcc-c++
BuildRequires: devtoolset-7-binutils

%description
The MySQL(TM) software delivers a very fast, multi-threaded, multi-user,
and robust SQL (Structured Query Language) database server. MySQL Server
is intended for mission-critical, heavy-load production systems as well
as for embedding into mass-deployed software.

%define MYSQL_USER root
%define MYSQL_GROUP root
%define __os_install_post %{nil}
%define commit_id %(git rev-parse --short HEAD)
%define release_date 20230526

%prep
cd $OLDPWD/../

#%setup -q

%build
cd $OLDPWD/../

if [ "%{use_gcc}" == "system" ]; then
    CC=gcc
    CXX=g++
    CMAKE_BIN=cmake
else
    CC=/opt/rh/devtoolset-7/root/usr/bin/gcc
    CXX=/opt/rh/devtoolset-7/root/usr/bin/g++
    CMAKE_BIN=cmake
fi

CFLAGS="-O3 -g -fexceptions -static-libgcc -fno-omit-frame-pointer -fno-strict-aliasing"
CXXFLAGS="-O3 -g -fexceptions -static-libgcc -fno-omit-frame-pointer -fno-strict-aliasing"
export CC CFLAGS CXX CXXFLAGS

$CMAKE_BIN .                            \
  -DFORCE_INSOURCE_BUILD=ON          \
  -DSYSCONFDIR:PATH=%{prefix}           \
  -DCMAKE_INSTALL_PREFIX:PATH=%{prefix} \
  -DCMAKE_BUILD_TYPE:STRING=RelWithDebInfo  \
  -DWITH_NORMANDY_CLUSTER=ON         \
  -DWITH_7U:BOOL=%{with_7u}          \
  -DWITH_PROTOBUF:STRING=bundled     \
  -DINSTALL_LAYOUT=STANDALONE        \
  -DMYSQL_MAINTAINER_MODE=0          \
  -DWITH_EMBEDDED_SERVER=0           \
  -DWITH_SSL=openssl                 \
  -DWITH_ZLIB=bundled                \
  -DWITH_MYISAM_STORAGE_ENGINE=1     \
  -DWITH_INNOBASE_STORAGE_ENGINE=1   \
  -DWITH_PARTITION_STORAGE_ENGINE=1  \
  -DWITH_CSV_STORAGE_ENGINE=1        \
  -DWITH_ARCHIVE_STORAGE_ENGINE=1    \
  -DWITH_BLACKHOLE_STORAGE_ENGINE=1  \
  -DWITH_FEDERATED_STORAGE_ENGINE=1  \
  -DWITH_PERFSCHEMA_STORAGE_ENGINE=1 \
  -DWITH_EXAMPLE_STORAGE_ENGINE=0    \
  -DWITH_TEMPTABLE_STORAGE_ENGINE=1  \
  -DWITH_XENGINE_STORAGE_ENGINE=0    \
  -DUSE_CTAGS=0                      \
  -DWITH_EXTRA_CHARSETS=all          \
  -DWITH_DEBUG=0                     \
  -DENABLE_DEBUG_SYNC=0              \
  -DENABLE_DTRACE=0                  \
  -DENABLED_PROFILING=1              \
  -DENABLED_LOCAL_INFILE=1           \
  -DWITH_BOOST="extra/boost/boost_1_70_0.tar.gz" \
  -DDOWNLOAD_BOOST=1 \
  -DWITH_BOOST=extra/boost \
  -DDOWNLOAD_BOOST_TIMEOUT=6000 \
  -DRDS_RELEASE_DATE=%{release_date} \
  -DRDS_COMMIT_ID=%{commit_id}

make -j `cat /proc/cpuinfo | grep processor| wc -l`

%install
cd $OLDPWD/../
make DESTDIR=$RPM_BUILD_ROOT install
find $RPM_BUILD_ROOT -name '.git' -type d -print0|xargs -0 rm -rf

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-, %{MYSQL_USER}, %{MYSQL_GROUP})
%attr(755, %{MYSQL_USER}, %{MYSQL_GROUP}) %{prefix}/*
%dir %attr(755,  %{MYSQL_USER}, %{MYSQL_GROUP}) %{prefix}
%exclude %{prefix}/mysql-test

%pre
## in %pre step, $1 is 1 for install, 2 for upgrade
## %{prefix} exists is error for install, but not for upgrade
if [ $1 -eq 1 ] && [ -d %{prefix} ]; then
    echo "ERROR: %{prefix}" exists
    exit 1
fi

%preun

%changelog

