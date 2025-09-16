#!/usr/bin/env python3

import os
import click
from enum import Enum, unique
from Pegasus.api import *


@unique
class SitesAvailable(Enum):
    CONDORPOOL = 1
    SLURM = 2
    REMOTE_SLURM = 3
    LSF = 4
    SGE = 5
    SUMMIT_GLITE = 6
    SUMMIT_KUBERNETES = 7


SitesAvailableDescription = {
    SitesAvailable.CONDORPOOL: "Local Machine Condor Pool",
    SitesAvailable.SLURM: "Local SLURM Cluster",
    SitesAvailable.REMOTE_SLURM: "Remote SLURM Cluster",
    SitesAvailable.LSF: "Local LSF Cluster",
    SitesAvailable.SGE: "Local SGE Cluster",
    SitesAvailable.SUMMIT_GLITE: "OLCF Summit from OLCF Headnode",
    SitesAvailable.SUMMIT_KUBERNETES: "OLCF Summit from OLCF Hosted Kubernetes Pod"
}

SitesRequireQueue = [
    SitesAvailable.SLURM,
    SitesAvailable.REMOTE_SLURM,
    SitesAvailable.LSF,
    SitesAvailable.SGE
]

SitesRequirePegasusHome = [
    SitesAvailable.SLURM,
    SitesAvailable.REMOTE_SLURM,
    SitesAvailable.LSF,
    SitesAvailable.SGE
]

SitesMayRequireProject = [
    SitesAvailable.SLURM,
    SitesAvailable.REMOTE_SLURM,
    SitesAvailable.LSF,
    SitesAvailable.SGE
]

SitesRequireProject = [
    SitesAvailable.SUMMIT_GLITE,
    SitesAvailable.SUMMIT_KUBERNETES
]

SitesRequireScratch = [
    SitesAvailable.REMOTE_SLURM
]

SitesAreRemote = [
    SitesAvailable.REMOTE_SLURM
]


class MySite():
    def __init__(self, scratch_parent_dir, storage_parent_dir, target_site: SitesAvailable, project_name="",
                 queue_name="", pegasus_home="", login_host="",
                 transfer_endpoint="",
                 remote_shared_scratch_parent_dir=""):

        self.shared_scratch_parent_dir = scratch_parent_dir
        self.local_storage_parent_dir = storage_parent_dir

        self.sc = SiteCatalog()

        local = Site("local") \
            .add_directories(
            Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, "scratch"))
            .add_file_servers(
                FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, "scratch"), Operation.ALL)),

            Directory(Directory.LOCAL_STORAGE, os.path.join(self.local_storage_parent_dir, "output"))
            .add_file_servers(
                FileServer("file://" + os.path.join(self.local_storage_parent_dir, "output"), Operation.ALL))
        )

        self.sc.add_sites(local)

        self.exec_site_arch = None
        if target_site is SitesAvailable.CONDORPOOL:
            self.exec_site_name = "condorpool"
            self.condorpool()
        elif target_site is SitesAvailable.SLURM:
            self.exec_site_name = "slurm"
            self.slurm(project_name, queue_name, pegasus_home)
        elif target_site is SitesAvailable.REMOTE_SLURM:
            self.exec_site_name = "remote_slurm"
            self.remote_slurm(project_name, queue_name, pegasus_home, login_host, transfer_endpoint,
                              remote_shared_scratch_parent_dir)
        elif target_site is SitesAvailable.LSF:
            self.exec_site_name = "lsf"
            self.lsf(project_name, queue_name, pegasus_home)
        elif target_site is SitesAvailable.SGE:
            self.exec_site_name = "sge"
            self.sge(project_name, queue_name, pegasus_home)
        elif target_site is SitesAvailable.SUMMIT_GLITE:
            self.exec_site_name = "summit"
            self.exec_site_arch = Arch.PPC64LE
            self.summit_glite(project_name, "batch")
        elif target_site is SitesAvailable.SUMMIT_KUBERNETES:
            self.exec_site_name = "summit"
            self.exec_site_arch = Arch.PPC64LE
            self.summit_kubernetes(project_name, "batch")

    def write(self):
        self.sc.write()

    def condorpool(self):
        condorpool = Site(self.exec_site_name) \
            .add_pegasus_profile(
            style="condor",
            data_configuration="condorio",
            clusters_num=2
        ) \
            .add_condor_profile(
            universe="vanilla",
            periodic_remove="(JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 10)"
        )

        self.sc.add_sites(condorpool)

    def osg_isi(self):
        osg = Site(self.exec_site_name) \
            .add_pegasus_profile(
            style="condor",
            data_configuration="condorio",
            clusters_num=2
        ) \
            .add_condor_profile(
            universe="vanilla",
            periodic_remove="(JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 10)",
            requirements="OSGVO_OS_STRING == \"RHEL 6\" && Arch == \"X86_64\" &&  HAS_MODULES == True"
        ) \
            .add_profiles(Namespace.CONDOR, key="+ProjectName", value="PegasusTraining")

        self.sc.add_sites(osg)

    def slurm(self, project_name, queue_name, pegasus_home):
        slurm = Site(self.exec_site_name) \
            .add_directories(
            Directory(Directory.SHARED_SCRATCH,
                      os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
            .add_file_servers(
                FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"),
                           Operation.ALL))
        ) \
            .add_condor_profile(grid_resource="batch slurm") \
            .add_pegasus_profile(
            style="glite",
            queue=queue_name,
            data_configuration="sharedfs",
            auxillary_local="true",
            nodes=1,
            ppn=1,
            runtime=1800,
            clusters_num=2
        )

        if project_name:
            slurm.add_pegasus_profile(project=project_name)

        if pegasus_home:
            slurm.add_env(key="PEGASUS_HOME", value=pegasus_home)

        self.sc.add_sites(slurm)

    def remote_slurm(self, project_name, queue_name, pegasus_home, login_host="", transfer_endpoint="",
                     remote_shared_scratch_parent_dir=""):
        if not transfer_endpoint:
            transfer_endpoint = "file://"

        auxillary_local = "true" if transfer_endpoint == "file://" else "false"

        slurm = (Site(self.exec_site_name) \
            .add_directories(
            Directory(Directory.SHARED_SCRATCH,
                      os.path.join(remote_shared_scratch_parent_dir, self.exec_site_name, "scratch"))
            .add_file_servers(
                FileServer(
                    transfer_endpoint + os.path.join(remote_shared_scratch_parent_dir, self.exec_site_name, "scratch"),
                    Operation.ALL))
        ) \
            .add_grids(
            Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.SLURM, contact=login_host,
                 job_type=SupportedJobs.COMPUTE)
        )
            .add_grids(
            Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.SLURM, contact=login_host,
                 job_type=SupportedJobs.AUXILLARY)
        )
            .add_pegasus_profile(
            style="ssh",
            queue=queue_name,
            data_configuration="sharedfs",
            auxillary_local=auxillary_local,
            nodes=1,
            ppn=1,
            runtime=1800,
            clusters_num=2
        ))

        if project_name:
            slurm.add_pegasus_profile(project=project_name)

        if pegasus_home:
            slurm.add_env(key="PEGASUS_HOME", value=pegasus_home)

        self.sc.add_sites(slurm)

    def lsf(self, project_name, queue_name, pegasus_home):
        lsf = Site(self.exec_site_name) \
            .add_directories(
            Directory(Directory.SHARED_SCRATCH,
                      os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
            .add_file_servers(
                FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"),
                           Operation.ALL))
        ) \
            .add_condor_profile(grid_resource="batch lsf") \
            .add_pegasus_profile(
            style="glite",
            queue=queue_name,
            data_configuration="sharedfs",
            auxillary_local="true",
            nodes=1,
            ppn=1,
            runtime=1800,
            clusters_num=2
        )

        if project_name:
            lsf.add_pegasus_profile(project=project_name)

        if pegasus_home:
            lsf.add_env(key="PEGASUS_HOME", value=pegasus_home)

        self.sc.add_sites(lsf)

    def sge(self, project_name, queue_name, pegasus_home):
        sge = Site(self.exec_site_name) \
            .add_directories(
            Directory(Directory.SHARED_SCRATCH,
                      os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
            .add_file_servers(
                FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"),
                           Operation.ALL))
        ) \
            .add_condor_profile(grid_resource="batch sge") \
            .add_pegasus_profile(
            style="glite",
            queue=queue_name,
            data_configuration="sharedfs",
            auxillary_local="true",
            nodes=1,
            ppn=1,
            runtime=1800,
            clusters_num=2
        )

        if project_name:
            sge.add_pegasus_profile(project=project_name)

        if pegasus_home:
            sge.add_env(key="PEGASUS_HOME", value=pegasus_home)

        self.sc.add_sites(sge)

    def summit_glite(self, project_name, queue_name):
        summit = Site(self.exec_site_name) \
            .add_directories(
            Directory(Directory.SHARED_SCRATCH,
                      os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
            .add_file_servers(
                FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"),
                           Operation.ALL))
        ) \
            .add_condor_profile(grid_resource="batch lsf") \
            .add_pegasus_profile(
            style="glite",
            queue=queue_name,
            data_configuration="sharedfs",
            auxillary_local="true",
            nodes=1,
            project=project_name,
            job_aggregator="mpiexec",
            runtime=1800,
            clusters_num=2
        ) \
            .add_env(key="PEGASUS_HOME", value="/ccs/proj/csc355/summit/pegasus/stable")

        self.sc.add_sites(summit)

    def summit_kubernetes(self, project_name, queue_name):
        summit = Site(self.exec_site_name) \
            .add_grids(
            Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.LSF, contact="${USER}@dtn.ccs.ornl.gov",
                 job_type=SupportedJobs.COMPUTE),
            Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.LSF, contact="${USER}@dtn.ccs.ornl.gov",
                 job_type=SupportedJobs.AUXILLARY)
        ) \
            .add_directories(
            Directory(Directory.SHARED_SCRATCH,
                      os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
            .add_file_servers(
                FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"),
                           Operation.ALL))
        ) \
            .add_pegasus_profile(
            style="ssh",
            queue=queue_name,
            auxillary_local="true",
            data_configuration="sharedfs",
            change_dir="true",
            nodes=1,
            project=project_name,
            job_aggregator="mpiexec",
            runtime=1800,
            clusters_num=2
        ) \
            .add_env(key="PEGASUS_HOME", value="/ccs/proj/csc355/summit/pegasus/stable")

        self.sc.add_sites(summit)


#### Support standalone invocation ####

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    "--execution-site",
    type=click.Choice(SitesAvailable.__members__, case_sensitive=True),
    prompt="What's the targeted execution site",
    help="Target execution site"
)
@click.option(
    "--project-name",
    default=None,
    show_default=True,
    help="Project allocation"
)
@click.option(
    "--queue-name",
    default=None,
    help="Target execution site's queue"
)
@click.option(
    "--pegasus-home",
    default=None,
    show_default=True,
    help="Pegasus home directory location"
)
@click.option(
    "--scratch-parent-dir",
    default=os.getcwd(),
    show_default=True,
    help="Parent directory of scratch folder"
)
@click.option(
    "--storage-parent-dir",
    default=os.getcwd(),
    show_default=True,
    help="Parent directory of output folder"
)
@click.option(
    "--login-host",
    default=None,
    show_default=True,
    help="login host for a remote cluster to which jobs are to be submitted"
)
@click.option(
    "--transfer-endpoint",
    default=None,
    show_default=True,
    help="transfer endpoint for the remote cluster to use for data transfers"
)
@click.option(
    "--remote-scratch-parent-dir",
    default=None,
    show_default=True,
    help="the scratch dir to use for the remote cluster"
)
def main(scratch_parent_dir, storage_parent_dir, execution_site, project_name, queue_name, pegasus_home, login_host,
         transfer_endpoint, remote_scratch_parent_dir):
    execution_site = SitesAvailable[execution_site]

    if execution_site in SitesRequireQueue and queue_name is None:
        queue_name = click.prompt("What's the execution site's queue")

    if execution_site in SitesRequireProject and not project_name:
        project_name = click.prompt("What's your project's name")
    elif execution_site in SitesMayRequireProject and project_name is None:
        project_name = click.prompt("What's your project's name", default="", show_default=True)

    if execution_site in SitesRequirePegasusHome and pegasus_home is None:
        pegasus_home = click.prompt("What's the location of the PEGASUS_HOME dir on the compute nodes", default="",
                                    show_default=True)

    click.echo("Generating a Pegasus site catalog for {}".format(execution_site))
    if queue_name:
        click.echo("The site catalog will target queue \"{}\"".format(queue_name))

    if project_name:
        click.echo("The project allocation used is \"{}\"".format(project_name))

    if pegasus_home:
        click.echo("The PEGASUS_HOME location is \"{}\"".format(pegasus_home))

    exec_site = MySite(scratch_parent_dir, storage_parent_dir, execution_site, project_name=project_name,
                       queue_name=queue_name, pegasus_home=pegasus_home, login_host=login_host,
                       transfer_endpoint=transfer_endpoint, remote_shared_scratch_parent_dir=remote_scratch_parent_dir )
    exec_site.write()


if __name__ == "__main__":
    main()
