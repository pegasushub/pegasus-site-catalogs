#!/usr/bin/env python3

import os
from enum import Enum, unique
from Pegasus.api import *

@unique
class SitesAvailable(Enum):
    LOCAL = 1
    USC_HPCC = 2
    OSG_ISI = 3
    SLURM_DEFAULT = 4
    LSF_DEFAULT = 5
    SUMMIT_GLITE = 6
    SUMMIT_KUBERNETES = 7


SitesRequireQueue = [ SitesAvailable.SLURM_DEFAULT,
                     SitesAvailable.LSF_DEFAULT ]


SitesRequireProject = [ SitesAvailable.SLURM_DEFAULT,
                        SitesAvailable,LSF_DEFAULT,
                        SitesAvailable.SUMMIT_GLITE, 
                        SitesAvailable.SUMMIT_KUBERNETES ]


class MySite():
    def __init__(self, scratch_parent_dir, storage_parent_dir, target_site:SitesAvailable, project_name="", queue_name=""):
        self.shared_scratch_parent_dir = scratch_parent_dir
        self.local_storage_parent_dir = storage_parent_dir

        self.sc = SiteCatalog()

        local = Site("local")\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, "scratch"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, "scratch"), Operation.ALL)),

                        Directory(Directory.LOCAL_STORAGE, os.path.join(self.local_storage_parent_dir, "storage"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.local_storage_parent_dir, "storage"), Operation.ALL))
                    )

        self.sc.add_sites(local)
        
        if target_site is SitesAvailable.LOCAL:
            self.exec_site_name = "condorpool"
            self.condorpool()
        elif target_site is SitesAvailable.USC_HPCC:
            self.exec_site_name = "usc-hpcc"
            self.usc_hpcc("quick")
        elif target_site is SitesAvailable.OSG_ISI:
            self.exec_site_name = "osg-isi"
            self.osg_isi()
        elif target_site is SitesAvailable.SLURM_DEFAULT:
            self.exec_site_name = "slurm_default"
            self.slurm_default(project_name, queue_name)
        elif target_site is SitesAvailable.LSF_DEFAULT:
            self.exec_site_name = "lsf_default"
            self.lsf_default(project_name, queue_name)
        elif target_site is SitesAvailable.SUMMIT_GLITE:
            self.exec_site_name = "summit"
            self.summit_glite(project_name, "batch")
        elif target_site is SitesAvailable.SUMMIT_KUBERNETES:
            self.exec_site_name = "summit"
            self.summit_kubernetes(project_name, "batch")


    def write(self):
        self.sc.write()


    def condorpool(self):
        condorpool = Site(self.exec_site_name)\
                        .add_pegasus_profile(
                            style="condor",
                            data_configuration="condorio",
                            clusters_num=2
                        )\
                        .add_condor_profile(
                            universe="vanilla",
                            periodic_remove="(JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 10)"
                        )

        self.sc.add_sites(condorpool)


    def usc_hpcc(self, queue_name):
        usc = Site(self.exec_site_name)\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"), Operation.ALL))
                    )\
                    .add_condor_profile(grid_resource="batch slurm")\
                    .add_pegasus_profile(
                        style="glite",
                        queue=queue_name,
                        data_configuration="sharedfs",
                        auxillary_local=True,
                        clusters_num=2,
                        job_aggregator="mpiexec"
                    )\
                    .add_env(key="PEGASUS_HOME", value="/home/rcf-proj/gmj/pegasus/SOFTWARE/pegasus/default")
        
        self.sc.add_sites(usc)


    def osg_isi(self):
        osg = Site(self.exec_site_name)\
                .add_pegasus_profile(
                    style="condor",
                    data_configuration="condorio",
                    clusters_num=2
                )\
                .add_condor_profile(
                    universe="vanilla",
                    periodic_remove="(JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 10)",
                    requirements="OSGVO_OS_STRING == \"RHEL 6\" && Arch == \"X86_64\" &&  HAS_MODULES == True"
                )\
                .add_profiles(Namespace.CONDOR, key="+ProjectName", value="PegasusTraining")

        self.sc.add_sites(osg)


    def slurm_default(self, project_name, queue_name):
        slurm = Site(self.exec_site_name)\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"), Operation.ALL))
                    )\
                    .add_condor_profile(grid_resource="batch slurm")\
                    .add_pegasus_profile(
                        style="glite",
                        queue=queue_name,
                        data_configuration="sharedfs",
                        auxillary_local="true",
                        nodes=1,
                        ppn=1,
                        project=project_name,
                        runtime=1800,
                        clusters_num=2
                    )
        
        self.sc.add_sites(slurm)


    def lsf_default(self, project_name, queue_name):
        lsf = Site(self.exec_site_name)\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"), Operation.ALL))
                    )\
                    .add_condor_profile(grid_resource="batch lsf")\
                    .add_pegasus_profile(
                        style="glite",
                        queue=queue_name,
                        data_configuration="sharedfs",
                        auxillary_local="true",
                        nodes=1,
                        project=project_name,
                        runtime=1800,
                        clusters_num=2
                    )
        
        self.sc.add_sites(lsf)
                        

    def summit_glite(self, project_name, queue_name):
        summit = Site(self.exec_site_name)\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"), Operation.ALL))
                    )\
                    .add_condor_profile(grid_resource="batch lsf")\
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
                    )\
                    .add_env(key="PEGASUS_HOME", value="/ccs/proj/csc355/summit/pegasus/stable")
        
        self.sc.add_sites(summit)
                        

    def summit_kubernetes(self, project_name, queue_name):
        summit = Site(self.exec_site_name)\
                    .add_grids(
                        Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.LSF, contact="${USER}@dtn.ccs.ornl.gov", job_type=SupportedJobs.COMPUTE),
                        Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.LSF, contact="${USER}@dtn.ccs.ornl.gov", job_type=SupportedJobs.AUXILLARY)
                    )\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"), Operation.ALL))
                    )\
                    .add_pegasus_profile(
                        style="ssh",
                        queue=queue_name,
                        auxillary_local="true",
                        change_dir="true",
                        nodes=1,
                        project=project_name,
                        job_aggregator="mpiexec",
                        runtime=1800,
                        clusters_num=2
                    )\
                    .add_env(key="PEGASUS_HOME", value="/ccs/proj/csc355/summit/pegasus/stable")
        
        self.sc.add_sites(summit)
                        

