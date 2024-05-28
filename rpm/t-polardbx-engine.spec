%define version_extra X-Cluster
%define release_date 20240523
%define engine_version 8.4.19
Version: 2.4.0

Name: t-polardbx-engine
Release: %(git rev-parse --short HEAD)%{?dist}
License: GPL
#URL: http://gitlab.alibaba-inc.com/polardbx/polardbx-engine
Group: applications/database
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildRequires: cmake >= 3.8.2

%if "%{?dist}" == ".alios7" || "%{?dist}" == ".el7"
BuildRequires: libarchive, ncurses-devel, bison, libstdc++-static, autoconf
%endif
 
BuildRequires: zlib-devel, snappy-devel, lz4-devel, bzip2-devel libaio-devel


Packager: xiedao.yy@alibaba-inc.com
Autoreq: no
#Source: %{name}-%{version}.tar.gz
Prefix: /opt/polardbx_engine
Summary: PolarDB-X engine 8.0 based on Oracle MySQL 8.0

%description
PolarDB-X Engine is a MySQL branch originated from Alibaba Group. It is based on the MySQL official release and has many features and performance enhancements, PolarDB-X Engine has proven to be very stable and efficient in production environment. It can be used as a free, fully compatible, enhanced and open source drop-in replacement for MySQL.

%define MYSQL_USER root
%define MYSQL_GROUP root
%define __os_install_post %{nil}
%define commit_id %(git rev-parse --short HEAD)
%define base_dir /u01/xcluster80
%define copy_dir /u01/xcluster80_%{release_date}

%prep
cd $OLDPWD/../

#%setup -q

%build
cd $OLDPWD/../

cat extra/boost/boost_1_77_0.tar.bz2.*  > extra/boost/boost_1_77_0.tar.bz2

mach_type=`uname -m`;

if [ x"$mach_type" = x"aarch64" ]; then
    CFLAGS="-O3 -g -fexceptions -fno-strict-aliasing -Wl,-Bsymbolic"
    CXXFLAGS="-O3 -g -fexceptions -fno-strict-aliasing -Wl,-Bsymbolic"
else
    CFLAGS="-O3 -g -fexceptions  -static-libgcc -static-libstdc++ -fno-omit-frame-pointer -fno-strict-aliasing"
    CXXFLAGS="-O3 -g -fexceptions -static-libgcc -static-libstdc++ -fno-omit-frame-pointer -fno-strict-aliasing"
fi

CC=gcc
CXX=g++
CMAKE_BIN=cmake

export CC CFLAGS CXX CXXFLAGS

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
  -DPOLARDBX_ENGINE_VERSION=%{engine_version} \
  -DPOLARDBX_VERSION_EXTRA=%{version_extra} \
  -DWITH_TESTS=0                     \
  -DWITH_UNIT_TESTS=0

make -j `cat /proc/cpuinfo | grep processor| wc -l`

%install
cd $OLDPWD/../
make DESTDIR=$RPM_BUILD_ROOT install
# releaseNote.txt
# cp releaseNote.txt $RPM_BUILD_ROOT%{prefix}
find $RPM_BUILD_ROOT -name '.git' -type d -print0|xargs -0 rm -rf

# mkdir -p $RPM_BUILD_ROOT%{prefix}/mysqlmisc
# for misc in `ls $RPM_BUILD_ROOT%{prefix} | grep -v "bin\|man\|share\|include\|lib\|mysql-test\|mysqlmisc"`
# do
#         cp -rf $RPM_BUILD_ROOT%{prefix}/${misc} $RPM_BUILD_ROOT%{prefix}/mysqlmisc
# done

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
