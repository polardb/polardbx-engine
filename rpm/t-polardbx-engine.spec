Name: t-polardbx-engine
Version:2.3.0
Release: %(git rev-parse --short HEAD)%{?dist}
License: GPL
Group: applications/database
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildRequires: libarchive
BuildRequires: ncurses-devel
BuildRequires: bison
BuildRequires: libaio-devel
BuildRequires: zlib-devel
BuildRequires: openssl-devel
Packager: yzl268570@alibaba-inc.com
Autoreq: no
Prefix: /opt/polardbx_engine
Summary: PolarDB-X engine 8.0 based on Oracle MySQL 8.0

%description
PolarDB-X Engine is a MySQL branch originated from Alibaba Group. It is based on the MySQL official release and has many features and performance enhancements, PolarDB-X Engine has proven to be very stable and efficient in production environment. It can be used as a free, fully compatible, enhanced and open source drop-in replacement for MySQL.

%define MYSQL_USER root
%define MYSQL_GROUP root
%define __os_install_post %{nil}

%prep
cd $OLDPWD/../

#%setup -q

%build
cd $OLDPWD/../

CC=gcc
CXX=g++
CMAKE_BIN=cmake
CFLAGS="-O3 -g -fexceptions -static-libgcc -fno-omit-frame-pointer -fno-strict-aliasing"
CXXFLAGS="-O3 -g -fexceptions -static-libgcc -fno-omit-frame-pointer -fno-strict-aliasing"
export CC CFLAGS CXX CXXFLAGS

$CMAKE_BIN .                            \
  -DFORCE_INSOURCE_BUILD=ON          \
  -DSYSCONFDIR:PATH=%{prefix}           \
  -DCMAKE_INSTALL_PREFIX:PATH=%{prefix} \
  -DCMAKE_BUILD_TYPE:STRING=RelWithDebInfo  \
  -DWITH_NORMANDY_CLUSTER=ON         \
  -DWITH_7U:BOOL=OFF                 \
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
  -DDOWNLOAD_BOOST_TIMEOUT=6000

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

