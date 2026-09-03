### 说明：

- cicd文件夹下的脚本，可以同步到 precheck, daily-run 两条流水线
    - precheck：基于MR的源代码分支
        - ./cicd/build.sh
        - ./cicd/unittest.sh
    - daily-run：基于polardbx_8032_release分支
        - ./build.sh
        - ./cicd/mtr.sh

