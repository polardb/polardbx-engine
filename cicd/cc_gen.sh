#!/bin/bash

diff_line_range() {
  local START_LINE=
  local LINE_NUM=
  while read; do
    if [[ $REPLY =~ @@\ -[0-9]+(,[0-9]+)?\ \+([0-9]+)(,[1-9][0-9]*)?\ @@.* ]]; then
      START_LINE=${BASH_REMATCH[2]}
      LINE_NUM=${BASH_REMATCH[3]}
      if [ ! $LINE_NUM ]; then
        LINE_NUM=1
      else
        LINE_NUM=${LINE_NUM:1}
      fi
      echo $START_LINE
      echo $LINE_NUM
    elif [[ $REPLY =~ new\ file.* ]]; then
      START_LINE=1
      LINE_NUM=$1
      echo $START_LINE
      echo $LINE_NUM
      break
    fi
  done
}

cal_codecoverage() {
  x=$(($1 * 100))
  y=$2
  rate=100.0
  if [ $y -eq 0 ]; then
    rate=100.0
  else
    rate=$(echo $x $y | awk '{ printf "%0.2f\n",$1/$2}')
  fi
  echo $rate
}

WORK_DIR=$(pwd)
RESULT_DIR=result/code_coverage

while getopts "c:o:" opt; do
  case $opt in
  c)
    DIFF_COMMIT=${OPTARG}
    ;;
  o)
    RESULT_DIR=${OPTARG}
    ;;
  \?)
    exit 1
    ;;
  esac
done

if [[ ! -d ${RESULT_DIR} ]]; then
  mkdir -p ${RESULT_DIR}
fi

# Define an associate array to store file-diff_line_range information
declare -A MAP

# Avoid diff files number limitation
git config diff.renameLimit 10000

# If DIFF_COMMIT not provided, calculate the code coverage of contents in the
# last commit.
if [[ -z ${DIFF_COMMIT} ]]; then
  DIFF_COMMIT=$(git rev-parse HEAD^)
else
  # get previous commit with init_commit
  DIFF_COMMIT="${DIFF_COMMIT}^"
fi

# test single commit, end_commit=init_commit
if [[ -z ${END_COMMIT} ]]; then
  END_COMMIT="HEAD"
fi

# Get the git delta
declare -A GIT_FILES_MAP
echo "git diff ${DIFF_COMMIT} ${END_COMMIT} --name-only"
for GIT_FILE in $(git diff ${DIFF_COMMIT} ${END_COMMIT} --name-only); do
  GIT_FILES_MAP[$GIT_FILE]="has no gcda file"
done

EXTRA_PREFIX="$(git rev-parse --show-toplevel)/"

# Only interested in coverage files related to the git delta
for GCDA_FILE in $(find . -name '*.gcda'); do
  # Guess git file from gcdata file
  # ./sql/CMakeFiles/sql_main.dir/histograms/histogram.cc.gcda
  #    => sql/histograms/histogram.cc
  # ./libmysql/CMakeFiles/clientlib.dir/__/sql-common/get_password.cc.gcda
  #    => libmysql/sql-common/get_password.cc
  GIT_FILE=$(echo $GCDA_FILE | sed 's/^..//;s/CMakeFiles\/[^\/]*.dir\/\(__\/\)\?//;s/.gcda$//')
  FILE_TYPE=${GIT_FILE##*.}
  FILE_NAME=${GIT_FILE##*/}
  FILE_DIR=${GIT_FILE%/*}

  GCNO_FILE=$(echo ${GCDA_FILE} | sed s/gcda$/gcno/)

  # If the file is not in the git delta
  if [[ -z "${GIT_FILES_MAP[$GIT_FILE]+unset}" ]]; then
    continue
  fi

  # echo "wc -l ${EXTRA_PREFIX}${GIT_FILE} | awk '{printf("%s", $1);}"
  LINE_NUM=$(wc -l ${EXTRA_PREFIX}${GIT_FILE} | awk '{printf("%s", $1);}')
  # echo "git diff ${DIFF_COMMIT} -U0 -- ${EXTRA_PREFIX}${GIT_FILE} | diff_line_range ${LINE_NUM}"
  DIFF_LINE_RANGES=$(git diff ${DIFF_COMMIT} -U0 -- ${EXTRA_PREFIX}${GIT_FILE} | diff_line_range ${LINE_NUM})
  if [[ -z "${DIFF_LINE_RANGES}" ]]; then
    GIT_FILES_MAP[$GIT_FILE]="$GCNO_FILE is skipped due to diff_line_range"
    continue
  fi

  GIT_FILES_MAP[$GIT_FILE]="$GCNO_FILE copied to $RESULT_DIR/$FILE_DIR"
  MAP[${GIT_FILE}]=${DIFF_LINE_RANGES}
  echo "$RESULT_DIR/$FILE_DIR"
  mkdir -p $RESULT_DIR/$FILE_DIR
  echo "cp $GCNO_FILE $GCDA_FILE $RESULT_DIR/$FILE_DIR"
  cp $GCNO_FILE $GCDA_FILE $RESULT_DIR/$FILE_DIR
done

# Add delta coverage file has no gcda file
for i in "${!GIT_FILES_MAP[@]}"; do
  FILE_TYPE=${i##*.}
  FILE_NAME=${i##*/}
  if [[ x${GIT_FILES_MAP[$i]} = x"has no gcda file" ]]; then
    if [[ x$FILE_TYPE = x"h" || x$FILE_TYPE = x"ic" || x$FILE_TYPE = x"cc" || x$FILE_TYPE = x"c" ]]; then
      if [ -n "${mymap[key2]+_}" ]; then
        echo "FILE_NAME=${FILE_NAME} exists"
      else
        echo "FILE_NAME=${FILE_NAME}, FILE_TYPE=${FILE_TYPE}"
        echo "wc -l ${EXTRA_PREFIX}${i} | awk '{printf(\"%s\", \$1);}'"
        LINE_NUM=$(wc -l ${EXTRA_PREFIX}${i} | awk '{printf("%s", $1);}')
        echo "git diff ${DIFF_COMMIT} -U0 -- ${EXTRA_PREFIX}${i} | diff_line_range ${LINE_NUM}"
        DIFF_LINE_RANGES=$(git diff ${DIFF_COMMIT} -U0 -- ${EXTRA_PREFIX}${i} | diff_line_range ${LINE_NUM})
        if [[ -z "${DIFF_LINE_RANGES}" ]]; then
          GIT_FILES_MAP[$i]="$GCNO_FILE is skipped due to diff_line_range"
          continue
        fi

        MAP[${i}]=${DIFF_LINE_RANGES}
      fi
    fi
  fi
done

# Report git delta coverage file status
for i in "${!GIT_FILES_MAP[@]}"; do
  echo "Commit file $i: ${GIT_FILES_MAP[$i]}"
done

for FILE in ${!MAP[@]}; do
  FILE_NAME="${FILE##*/}"
  DIFF_LINE_RANGE=${MAP[${FILE}]}
  echo "FILE_NAME=$FILE_NAME"
done

cd ${RESULT_DIR}

LCOV_MAJOR_VERSION=$(lcov --version | awk '{print $4}' | awk -F '.' {'print $1}')
echo "LCOV MAJOR VERSION = $LCOV_MAJOR_VERSION"
if [ ${LCOV_MAJOR_VERSION} -gt 1 ]; then
  lcov --ignore-errors inconsistent,inconsistent,negative,source --capture --directory . -o app.info
  lcov --ignore-errors inconsistent,inconsistent,unused,unused --remove app.info "/usr/include/*" "/usr/local/*" "*asio/*" "*googletest/*" -o filtered_app.info
  genhtml --ignore-errors source,inconsistent --synthesize-missing -o reports filtered_app.info
else
  lcov --capture --directory . -o app.info
  lcov --remove app.info "/usr/include/*" "/usr/local/*" "*asio/*" "*googletest/*" -o filtered_app.info
  genhtml --ignore-errors "source" -o reports filtered_app.info
fi

cd reports

echo "Current work path is $(pwd)"

# Add special mark on diff lines.
for FILE in ${!MAP[@]}; do
  FILE_NAME="${FILE##*/}"
  HTML_FILE_NAME=${FILE_NAME}.gcov.html
  HTML_FILE=$(find . -name ${HTML_FILE_NAME})

  #generate diff lines for dict0mem.cc to storage/innobase/dict/dict0mem.cc.gcov.html
  echo "generate diff lines for $FILE_NAME to $HTML_FILE"
  CUR_PATH=$(pwd)
  # File has been deleted in current version.
  if [ ! -f "${HTML_FILE}" ]; then
    echo "Warning! Html hasn't been generated for ${FILE}."
    continue
  fi

  HTML_DIR="${HTML_FILE%/*}"
  HTML_NAME="${HTML_FILE##*/}"

  DIRS="${HTML_DIR//\// }"
  RELATIVE_DIR=
  for DIR in ${DIRS}; do
    if [ ${DIR} != "." ]; then
      RELATIVE_DIR="${RELATIVE_DIR}\\/${DIR}"
    fi
  done

  RELATIVE_DIR=${RELATIVE_DIR:2}
  sed -i -E "s/<a href=\"${RELATIVE_DIR}\/index.html\">${RELATIVE_DIR}<\/a>/<a href=\"${RELATIVE_DIR}\/index.html\" style=\"background:yellow\">${RELATIVE_DIR}<\/a>/" ./index.html
  sed -i -E "s/<a href=\"${HTML_NAME}\">(.+)<\/a>/<a href=\"${HTML_NAME}\" style=\"background:yellow\">\1<\/a>/" ${HTML_DIR}/index.html

  DIFF_LINE_RANGE=${MAP[${FILE}]}
  IS_EVEN=false
  START_LINE=
  LINE_NUM=

  for ELEMENT in ${DIFF_LINE_RANGE}; do
    if [ ${IS_EVEN} = false ]; then
      START_LINE=${ELEMENT}
      IS_EVEN=true
    else
      LINE_NUM=${ELEMENT}
      IS_EVEN=false
      for ((i = 0; i < ${LINE_NUM}; i++)); do
        CURRENT_LINE=$(expr ${START_LINE} + ${i})
        # echo "sed -i -E \"s/<span class=\"lineNum\">(\s{3})?(\s*${CURRENT_LINE}<\/span>)/<span class=\"lineNum\" style=\"background:yellow\">+++\2/\" $HTML_FILE"
        sed -i -E "s/<span class=\"lineNum\">(\s{3})?(\s*${CURRENT_LINE}(\s{1})?<\/span>)/<span class=\"lineNum\" style=\"background:yellow\">+++\2/" $HTML_FILE
      done
    fi
  done
done

# Calculate Delta Code Coverage
cd $WORK_DIR
cd ${RESULT_DIR}/reports

covline_total_count=0
nocovline_total_count=0
files_str=""
for file in $(find . -name "*.gcov.html" -print); do
  file_total_count=0
  covline=$(cat $file | grep "+++" | grep -E "tlaGNC|lineCov" | wc -l)
  nocovline=$(cat $file | grep "+++" | grep -E "tlaUNC|lineNoCov" | wc -l)
  file_total_count=$(($covline + $nocovline))
  if [ $file_total_count = 0 ]; then
    continue
  fi

  covline_total_count=$(($covline_total_count + $covline))
  nocovline_total_count=$(($nocovline_total_count + $nocovline))
  rate=$(cal_codecoverage $covline $file_total_count)
  realname=$(echo "$file" | sed 's/\.gcov\.html//')
  coverPer="coverPerHi"
  coverNum="coverNumHi"

  if [ $((100 * $covline / $file_total_count)) -lt 75 ]; then
    coverPer="coverPerLo"
    coverNum="coverNumLo"
  elif [ $((100 * $covline / $file_total_count)) -lt 85 ]; then
    coverPer="coverPerMed"
    coverNum="coverNumMed"
  else
    coverPer="coverPerHi"
    coverNum="coverNumHi"
  fi

  files_str="$files_str<tr><td class='coverFile'><a href='$realname.gcov.html'>$realname</a></td><td class="$coverPer">$rate%</td><td class="$coverNum">$covline / $file_total_count</tr>"
done

total_count=$(($covline_total_count + $nocovline_total_count))
total_rate=$(cal_codecoverage $covline_total_count $total_count)

headerCovTableEntry="headerCovTableEntryHi"

if [ $total_count -eq 0 ]; then
  headerCovTableEntry="headerCovTableEntryHi"
elif [ $((100 * $covline_total_count / $total_count)) -lt 75 ]; then
  headerCovTableEntry="headerCovTableEntryLo"
elif [ $((100 * $covline_total_count / $total_count)) -lt 85 ]; then
  headerCovTableEntry="headerCovTableEntryMed"
else
  headerCovTableEntry="headerCovTableEntryHi"
fi

export CODE_COVERAGE_DELTA=$total_rate

cat >delta.cov.html <<EOF
<!DOCTYPE html><html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="X-UA-Compatible" content="ie=edge">
<title>delta.cov</title>
<link rel="stylesheet" type="text/css" href="gcov.css">
</head>
<body>
<center>
<table width="80%" cellpadding="1" cellspacing="1" border="0">
  <tbody><tr><td class="title">Delta code coverage report</td></tr>
    <tr><td class="ruler"><img src="glass.png" width="3" height="3" alt=""></td></tr>
    <tr>
      <td width="100%">
        <table cellpadding="1" border="0" width="100%">
          <tbody><tr>
            <td width="5%"></td>
            <td width="15%"></td>
            <td width="10%" class="headerCovTableHead">Hit</td>
            <td width="10%" class="headerCovTableHead">Total</td>
            <td width="15%" class="headerCovTableHead">Coverage</a></td>
          </tr>
          <tr>
            <td></td>
            <td class="headerItem">Lines:</td>
            <td class="headerCovTableEntry">$covline_total_count</td>
            <td class="headerCovTableEntry">$total_count</td>
            <td class="$headerCovTableEntry">$total_rate %</td>
          </tr>
          <tr><td><img src="glass.png" width="3" height="3" alt=""></td></tr>
        </tbody></table>
      </td>
    </tr>

    <tr><td class="ruler"><img src="glass.png" width="3" height="3" alt=""></td></tr>
  </tbody>
</table>
<table width="80%" cellpadding="1" cellspacing="1" border="0">
  <tbody>
    <tr>
      <td width="50%" class="tableHead">Directory</td>
      <td width="50%" class="tableHead" colspan="2">Line Coverage</td>
    </tr>
    ${files_str}
  </tbody>
</table>
</center>
EOF

sed -i -E "s/Coverage/Coverage\/<a href=\"delta.cov.html\">Delta Coverage<\/a>/" index.html
