__author__ = "David Lähnemann, Johannes Köster, Christian Meesters"
__copyright__ = "Copyright 2023, Brian Fulton-Howard, David Lähnemann, Johannes Köster, Christian Meesters"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

import os
import subprocess
import sys
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.real import RealExecutor
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_executor_plugins.settings import ExecMode, CommonSettings


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Define whether your executor plugin implies that there is no shared
    # filesystem (True) or not (False).
    # This is e.g. the case for cloud execution.
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=False,
    spawned_jobs_assume_shared_fs=True
)


# Required:
# Implementation of your executor
class Executor(RealExecutor):
    def __post_init__(self):
        # These environment variables are set by SLURM.
        # only needed for commented out jobstep handling below
        self.jobid = os.getenv("LSB_JOBID")

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.

        jobsteps = dict()
        # TODO revisit special handling for group job levels via srun at a later stage
        # if job.is_group():

        #     def get_call(level_job, aux=""):
        #         # we need this calculation, because of srun's greediness and
        #         # SLURM's limits: it is not able to limit the memory if we divide the
        #         # job per CPU by itself.

        #         level_mem = level_job.resources.get("mem_mb")
        #         if isinstance(level_mem, TBDString):
        #             level_mem = 100

        #         mem_per_cpu = max(level_mem // level_job.threads, 100)
        #         exec_job = self.format_job_exec(level_job)

        #         # Note: The '--exlusive' flag is a prevention for triggered job steps
        #         #       within an allocation to oversubscribe within a given c-group.
        #         #       As we are dealing only with smp software
        #         #       the '--ntasks' is explicitly set to 1 by '-n1' per group job
        #         #       (step).
        #         return (
        #             f"srun -J {job.groupid} --jobid {self.jobid}"
        #             f" --mem-per-cpu {mem_per_cpu} -c {level_job.threads}"
        #             f" --exclusive -n 1 {aux} {exec_job}"
        #         )

        #     for level in list(job.toposorted):
        #         # we need to ensure order - any:
        #         level_list = list(level)
        #         for level_job in level_list[:-1]:
        #             jobsteps[level_job] = subprocess.Popen(
        #                 get_call(level_job), shell=True
        #             )
        #         # now: the last one
        #         # this way, we ensure that level jobs depending on the current level
        #         # get started
        #         jobsteps[level_list[-1]] = subprocess.Popen(
        #             get_call(level_list[-1], aux="--dependency=singleton"), shell=True
        #         )

        call = self.format_job_exec(job)

        self.logger.debug(job.is_group())
        self.logger.debug(call)
        # this dict is to support the to be implemented feature of oversubscription in
        # "ordinary" group jobs.
        jobsteps[job] = subprocess.Popen(call, shell=True)

        job_info = SubmittedJobInfo(job)
        self.report_job_submission(job_info)

        # wait until all steps are finished
        if any(proc.wait() != 0 for proc in jobsteps.values()):
            self.report_job_error(job_info)
        else:
            self.report_job_success(job_info)

    def cancel(self):
        pass

    def shutdown(self):
        pass

    def get_python_executable(self):
        return sys.executable

    @property
    def cores(self):
        return "all"

    def get_exec_mode(self) -> ExecMode:
        return ExecMode.REMOTE
