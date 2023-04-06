import shlex
import _judger
import json
import os
import hashlib
import shutil
import uuid
import pwd
from multiprocessing import Pool
import grp
import psutil

JUDGER_WORKSPACE_BASE = "/judger/run"
LOG_BASE = "/root/test_code/log"

COMPILER_LOG_PATH = os.path.join(LOG_BASE, "compile.log")
JUDGER_RUN_LOG_PATH = os.path.join(LOG_BASE, "judger.log")
SERVER_LOG_PATH = os.path.join(LOG_BASE, "judge_server.log")

RUN_USER_UID = pwd.getpwnam("root").pw_uid
RUN_GROUP_GID = grp.getgrnam("root").gr_gid

COMPILER_USER_UID = pwd.getpwnam("root").pw_uid
COMPILER_GROUP_GID = grp.getgrnam("root").gr_gid


class InitSubmissionEnv(object):
    def __init__(self, judger_workspace, submission_id, init_test_case_dir=False):
        self.work_dir = os.path.join(judger_workspace, submission_id)
        self.init_test_case_dir = init_test_case_dir
        if init_test_case_dir:
            self.test_case_dir = os.path.join(
                self.work_dir, "submission_" + submission_id)
        else:
            self.test_case_dir = None

    def __enter__(self):
        try:
            os.mkdir(self.work_dir)
            if self.init_test_case_dir:
                os.mkdir(self.test_case_dir)
            os.chown(self.work_dir, COMPILER_USER_UID, RUN_GROUP_GID)
            os.chmod(self.work_dir, 0o711)
        except Exception as e:
            logger.exception(e)
            raise JudgeClientError("failed to create runtime dir")
        return self.work_dir, self.test_case_dir

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            shutil.rmtree(self.work_dir)
        except Exception as e:
            logger.exception(e)
            raise JudgeClientError("failed to clean runtime dir")


class JudgeServerException(Exception):
    def __init__(self, message):
        super().__init__()
        self.message = message


class CompileError(JudgeServerException):
    pass


class SPJCompileError(JudgeServerException):
    pass


class TokenVerificationFailed(JudgeServerException):
    pass


class JudgeClientError(JudgeServerException):
    pass


class JudgeServiceError(JudgeServerException):
    pass


def _run(instance, test_case_file_id):
    if test_case_file_id:
        return instance._judge_one(test_case_file_id)
    else:
        return instance._judge_one()


class SelfTest(object):
    def __init__(self, run_config, exe_path, max_cpu_time, max_memory, test_case_dir,
                 submission_dir, output=True):
        self._run_config = run_config
        self._exe_path = exe_path
        self._max_cpu_time = max_cpu_time
        self._max_memory = max_memory
        self._max_real_time = self._max_cpu_time * 3
        self._test_case_dir = test_case_dir
        self._submission_dir = submission_dir

        self._pool = Pool(processes=psutil.cpu_count())
        self._test_case_info = self._load_test_case_info()

        self._output = output

    def _load_test_case_info(self):
        try:
            with open(os.path.join(self._test_case_dir, "info")) as f:
                return json.load(f)
        except IOError:
            raise JudgeClientError("Test case not found")
        except ValueError:
            raise JudgeClientError("Bad test case config")

    def _compare_output(self, user_output_file):
        with open(user_output_file, "rb") as f:
            content = f.read()
        output_md5 = hashlib.md5(content.rstrip()).hexdigest()
        result = output_md5 == self._test_case_info["stripped_output_md5"]
        return output_md5, result

    def _judge_one(self, ):
        in_file = os.path.join(self._test_case_dir,
                               self._test_case_info["input_name"])

        real_user_output_file = user_output_file = os.path.join(
            self._submission_dir, "self_test.out")
        kwargs = {"input_path": in_file, "output_path": real_user_output_file,
                  "error_path": real_user_output_file}

        command = self._run_config["command"].format(exe_path=self._exe_path, exe_dir=os.path.dirname(self._exe_path),
                                                     max_memory=int(self._max_memory / 1024))
        command = shlex.split(command)
        env = ["PATH=" + os.environ.get("PATH", "")] + \
            self._run_config.get("env", [])

        seccomp_rule = self._run_config["seccomp_rule"]['Standard IO']
        if isinstance(seccomp_rule, dict):
            seccomp_rule = seccomp_rule['Standard IO']

        run_result = _judger.run(max_cpu_time=self._max_cpu_time,
                                 max_real_time=self._max_real_time,
                                 max_memory=self._max_memory,
                                 #  max_stack=128 * 1024 * 1024,
                                 #  max_output_size=max(self._test_case_info.get("output_size", 0) * 2, 1024 * 1024 * 16),
                                 max_stack=128 * 1024 * 1024,
                                 max_output_size=20 * 1024 * 1024,
                                 max_process_number=_judger.UNLIMITED,
                                 exe_path=command[0],
                                 args=command[1::],
                                 env=env,
                                 log_path=JUDGER_RUN_LOG_PATH,
                                 seccomp_rule_name=None,
                                 uid=RUN_USER_UID,
                                 gid=RUN_GROUP_GID,
                                 **kwargs)
        run_result["test_case"] = 'self_test'

        # if progress exited normally, then we should check output result
        run_result["output_md5"] = None
        run_result["output"] = None
        if run_result["result"] == _judger.RESULT_SUCCESS:
            if not os.path.exists(user_output_file):
                run_result["result"] = _judger.RESULT_WRONG_ANSWER
            else:
                run_result["output_md5"], is_ac = self._compare_output(
                    user_output_file)
                # -1 == Wrong Answer
                if not is_ac:
                    run_result["result"] = _judger.RESULT_WRONG_ANSWER

        if self._output:
            try:
                with open(user_output_file, "rb") as f:
                    run_result["output"] = f.read().decode(
                        "utf-8", errors="backslashreplace")
            except Exception:
                pass

        return run_result

    def run(self):
        tmp_result = []
        result = None
        tmp_result.append(self._pool.apply_async(_run, (self, None)))
        self._pool.close()
        self._pool.join()
        for item in tmp_result:
            # exception will be raised, when get() is called
            # # http://stackoverflow.com/questions/22094852/how-to-catch-exceptions-in-workers-in-multiprocessing
            result = item.get()
        return result

    def __getstate__(self):
        # http://stackoverflow.com/questions/25382455/python-notimplementederror-pool-objects-cannot-be-passed-between-processes
        self_dict = self.__dict__.copy()
        del self_dict["_pool"]
        return self_dict


class Compiler(object):
    def compile(self, compile_config, src_path, output_dir):
        command = compile_config["compile_command"]
        exe_path = os.path.join(output_dir, compile_config["exe_name"])
        command = command.format(
            src_path=src_path, exe_dir=output_dir, exe_path=exe_path)
        compiler_out = os.path.join(output_dir, "compiler.out")
        _command = shlex.split(command)

        os.chdir(output_dir)
        env = compile_config.get("env", [])
        env.append("PATH=" + os.getenv("PATH"))
        result = _judger.run(max_cpu_time=compile_config["max_cpu_time"],
                             max_real_time=compile_config["max_real_time"],
                             max_memory=compile_config["max_memory"],
                             max_stack=128 * 1024 * 1024,
                             max_output_size=20 * 1024 * 1024,
                             max_process_number=_judger.UNLIMITED,
                             exe_path=_command[0],
                             # /dev/null is best, but in some system, this will call ioctl system call
                             input_path=src_path,
                             output_path=compiler_out,
                             error_path=compiler_out,
                             args=_command[1::],
                             env=env,
                             log_path=COMPILER_LOG_PATH,
                             seccomp_rule_name=None,
                             uid=COMPILER_USER_UID,
                             gid=COMPILER_GROUP_GID)
        if result["result"] != _judger.RESULT_SUCCESS:
            if os.path.exists(compiler_out):
                with open(compiler_out, encoding="utf-8") as f:
                    error = f.read().strip()
                    os.remove(compiler_out)
                    print(error)
                    if error:
                        raise CompileError(error)
            raise CompileError(
                "Compiler runtime error, info: %s" % json.dumps(result))
        else:
            os.remove(compiler_out)
            return exe_path


def self_test(language_config, src, max_cpu_time, max_memory, test_case,
              output=True):

    # init
    compile_config = language_config.get("compile")
    run_config = language_config["run"]
    submission_id = uuid.uuid4().hex
    init_test_case_dir = True
    with InitSubmissionEnv(JUDGER_WORKSPACE_BASE, submission_id=str(submission_id), init_test_case_dir=init_test_case_dir) as dirs:
        submission_dir, test_case_dir = dirs
        if compile_config:
            src_path = os.path.join(submission_dir, compile_config["src_name"])

            # write source code into file
            with open(src_path, "w", encoding="utf-8") as f:
                f.write(src)
            os.chown(src_path, COMPILER_USER_UID, 0)
            os.chmod(src_path, 0o400)

            # compile source code, return exe file path
            exe_path = Compiler().compile(compile_config=compile_config,
                                          src_path=src_path,
                                          output_dir=submission_dir)

            try:
                # Java exe_path is SOME_PATH/Main, but the real path is SOME_PATH/Main.class
                # We ignore it temporarily
                os.chown(exe_path, RUN_USER_UID, 0)
                os.chmod(exe_path, 0o500)
            except Exception:
                pass
        else:
            exe_path = os.path.join(submission_dir, run_config["exe_name"])
            with open(exe_path, "w", encoding="utf-8") as f:
                f.write(src)
        # write test case
        item_info = {}

        input_name = "1.in"
        item_info["input_name"] = input_name
        input_data = test_case["input"].encode("utf-8")
        item_info["input_size"] = len(input_data)

        with open(os.path.join(test_case_dir, input_name), "wb") as f:
            f.write(input_data)

        output_name = "1.out"
        item_info["output_name"] = output_name
        output_data = test_case["output"].encode("utf-8")
        item_info["output_md5"] = hashlib.md5(output_data).hexdigest()
        item_info["output_size"] = len(output_data)
        item_info["stripped_output_md5"] = hashlib.md5(
            output_data.rstrip()).hexdigest()

        with open(os.path.join(test_case_dir, output_name), "wb") as f:
            f.write(output_data)

        '''
        {
            'input_name':'1.in',
            'input_size':,
            'output_name':,
            'output_md5':,
            'output_size':,
            'stripped_output_md5':,
        }
        '''
        with open(os.path.join(test_case_dir, "info"), "w") as f:
            json.dump(item_info, f)

        self_test_runner = SelfTest(run_config=language_config["run"],
                                    exe_path=exe_path,
                                    max_cpu_time=max_cpu_time,
                                    max_memory=max_memory,
                                    test_case_dir=test_case_dir,
                                    submission_dir=submission_dir,
                                    )
        run_result = self_test_runner.run()

        return run_result


src = '''#include<iostream>
using namespace std;
int main(){
    int a,b;
    cin>>a>>b;
    cout<<a+b;
    return 0;
}
'''
config = {
    "run": {
        "env": [
            "LANG=en_US.UTF-8",
            "LANGUAGE=en_US:en",
            "LC_ALL=en_US.UTF-8"
        ],
        "command": "{exe_path}",
        "seccomp_rule": {
            "File IO": "c_cpp_file_io",
            "Standard IO": "c_cpp"
        }
    },
    "compile": {
        "exe_name": "main",
        "src_name": "main.cpp",
        "max_memory": 1073741824,
        "max_cpu_time": 10000,
        "max_real_time": 20000,
        "compile_command": "/usr/bin/g++ -DONLINE_JUDGE -O2 -w -fmax-errors=3 -std=c++14 {src_path} -lm -o {exe_path}"
    },
}
result = self_test(config, src, max_cpu_time=10000,
                   max_memory=1073741824,
                   test_case={'input': '1 2', 'output': '3'})
print(result)
