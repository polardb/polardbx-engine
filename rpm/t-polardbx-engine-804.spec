# Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.
# 
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License, version 2.0, as published by the
# Free Software Foundation.
# 
# This program is also distributed with certain software (including but not
# limited to OpenSSL) that is licensed under separate terms, as designated in a
# particular file or component or in included license documentation. The authors
# of MySQL hereby grant you an additional permission to link the program and
# your derivative works with the separately licensed software that they have
# included with MySQL.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
# for more details.
# 
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA


%define polardb_version 'PolarDB V2.0'
%define product_version 2.5.0
%define release_date %(echo $RELEASE | cut -c 1-8)
%define version_extra X-Cluster

Name: t-polardbx-engine-804
Version: 8.4.20
Release: %(echo $RELEASE)%{?dist}
License: GPL
#URL: http://gitlab.alibaba-inc.com/polardbx/polardbx-engine
Group: applications/database
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root


%if "%{?dist}" == ".alios7"
BuildRequires: perl-IPC-Cmd
BuildRequires: cmake >= 3.8.2, make, autoconf, libstdc++-static, alios7u-2_32-gcc-10-repo
BuildRequires: gcc >= 10.2.1, gcc-c++ >= 10.2.1, libstdc++-devel >= 10.2.1, binutils >= 2.35
BuildRequires: zlib-devel, snappy-devel, lz4-devel, bzip2-devel
BuildRequires: libev-devel, libcurl-devel, libaio-devel, libarchive, ncurses-devel
BuildRequires: bison, libudev-devel, python-sphinx, procps-ng-devel, rsync
%endif


Packager: jianwei.zhao@alibaba-inc.com
Autoreq: no
Prefix: /u01/xcluster80_%{release_date}_current
Summary: PolarDB-X MySQL XCluster 8.0 based on Oracle MySQL 8.0

%description
The MySQL(TM) software delivers a very fast, multi-threaded, multi-user,
and robust SQL (Structured Query Language) database server. MySQL Server
is intended for mission-critical, heavy-load production systems as well
as for embedding into mass-deployed software.

%define MYSQL_USER root
%define MYSQL_GROUP root
%define __os_install_post %{nil}
%define commit_id %(git rev-parse --short HEAD)
%define base_dir /u01/xcluster80
%define copy_dir /u01/xcluster80_%{release_date}


%prep
echo "dist" %{?dist}
echo "_arch" %{?_arch}
echo "_target_cpu" %{?_target_cpu}
echo "_target_os" %{?_target_os}
echo "_target_vendor" %{?_target_vendor}

echo "_host_cpu" %{?_host_cpu}
echo "_host_os" %{?_host_os}
echo "__host_vendor" %{?__host_vendor}

echo "_build_cpu" %{?_build_cpu}
echo "_build_os" %{?_build_os}
echo "_build_vendor" %{?_build_vendor}
if [-f /etc/redhat-release]; then
    cat /etc/redhat-release
fi

%build
cd $OLDPWD/../

%if "%{?dist}" == ".alios7"
    CC=gcc
    CXX=g++
    CMAKE_BIN=cmake
%else
    CC=/opt/rh/devtoolset-10/root/usr/bin/gcc
    CXX=/opt/rh/devtoolset-10/root/usr/bin/g++
    CMAKE_BIN=cmake3
%endif

%if "%{?_arch}" == "aarch64"
    CFLAGS="-O3 -g -fexceptions -static-libgcc -static-libstdc++ -fno-omit-frame-pointer -fno-strict-aliasing -Wl,-Bsymbolic"
    CXXFLAGS="-O3 -g -fexceptions -static-libgcc -static-libstdc++ -fno-omit-frame-pointer -fno-strict-aliasing -Wl,-Bsymbolic"
%else
    CFLAGS="-O3 -g -fexceptions  -static-libgcc -static-libstdc++ -fno-omit-frame-pointer -fno-strict-aliasing"
    CXXFLAGS="-O3 -g -fexceptions -static-libgcc -static-libstdc++ -fno-omit-frame-pointer -fno-strict-aliasing"
%endif
export CC CXX CFLAGS CXXFLAGS CMAKE_BIN

cat extra/boost/boost_1_77_0.tar.bz2.*  > extra/boost/boost_1_77_0.tar.bz2

sed -i 's/^MYSQL_VERSION_PATCH=32$/MYSQL_VERSION_PATCH=41/' MYSQL_VERSION

$CMAKE_BIN .                            \
%ifarch aarch64
  -DWITH_JEMALLOC="no" \
%endif
  -DFORCE_INSOURCE_BUILD=ON          \
  -DSYSCONFDIR:PATH=%{prefix}           \
  -DCMAKE_INSTALL_PREFIX:PATH=%{prefix} \
  -DCMAKE_BUILD_TYPE:STRING=RelWithDebInfo  \
  -DWITH_PROTOBUF:STRING=bundled     \
  -DINSTALL_LAYOUT=STANDALONE        \
  -DMYSQL_MAINTAINER_MODE=0          \
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
  -DUSE_CTAGS=0                      \
  -DWITH_EXTRA_CHARSETS=all          \
  -DWITH_DEBUG=0                     \
  -DENABLE_DEBUG_SYNC=0              \
  -DENABLE_DTRACE=0                  \
  -DENABLED_PROFILING=1              \
  -DENABLED_LOCAL_INFILE=1           \
  -DWITH_BOOST="./extra/boost/boost_1_77_0.tar.bz2" \
  -DPOLARDBX_RELEASE_DATE=%{release_date} \
  -DPOLARDBX_ENGINE_VERSION=%{version} \
  -DPOLARDB_VERSION=%{polardb_version} \
  -DPOLARDBX_PRODUCT_VERSION=%{product_version} \
  -DPOLARDBX_VERSION_EXTRA=%{version_extra} \
  -DWITH_TESTS=0                     \
  -DWITH_UNIT_TESTS=0

%install
cd $OLDPWD/../
MIN_PARALLEL=$(($(cat /proc/cpuinfo | grep processor | wc -l) < 40 ? $(cat /proc/cpuinfo | grep processor | wc -l) : 40))
make DESTDIR=$RPM_BUILD_ROOT -j $MIN_PARALLEL install
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

## if another version mysqld exists under timestamp dir, abort installation with error
if [ -d %{copy_dir} ]; then
    old_commit_id=$(%{copy_dir}/bin/mysqld --version | grep -E "Build Commit|commit id" | awk '{print $4}')
    if [ "$old_commit_id" != "%{commit_id}" ]; then
       echo "ERROR: %{copy_dir} exists and old commit id '$old_commit_id' diff with new '%{commit_id}'"
       exit 1
    fi
fi

%post
## for basedir, mysqld version confliction is expected.
## basedir is used as the target dir for system links, and must be updated with
## latest bin and so file. we use rm && cp, instead of overwriting force copy
## which is dangerous for so file(can lead to calling process crash).
if [ -d %{base_dir} ]; then
    old_commit_id=$(%{base_dir}/bin/mysqld --version | grep -E "Build Commit|commit id" | awk '{print $4}')
    if [ "$old_commit_id" != "%{commit_id}" ]; then
        echo "Removing %{base_dir} and copying %{prefix} to %{base_dir}"
        rm -rf %{base_dir} && cp -rf %{prefix} %{base_dir}
    else
        echo "%{base_dir} already contains the right version mysqld"
    fi
else
    echo "Copying %{prefix} to %{base_dir}"
    cp -rf %{prefix} %{base_dir}
fi

## conflict version has been checked in %pre step, so only two possible scenes here:
## 1. copy_dir exists and with equal version, means already installed, just skip
## 2. copy_dir not exists, copy to it
if [ -d %{copy_dir} ]; then
    old_commit_id=$(%{copy_dir}/bin/mysqld --version | grep -E "Build Commit|commit id" | awk '{print $4}')
    if [ "$old_commit_id" == "%{commit_id}" ]; then
        echo "%{copy_dir} already contains the right version mysqld"
    fi
else
    echo "Copying %{prefix} to %{copy_dir}"
    cp -rf %{prefix} %{copy_dir}
fi


rm -rf %{prefix}

%preun

%changelog
