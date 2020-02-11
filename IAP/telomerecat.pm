#!/usr/bin/perl -w

#######################################################
### telomerecat.pm
### - Run Telomerecat
###
### Authors: M.J.vanRoosmalen
###
#######################################################

package IAP::telomerecat;

use strict;
use POSIX qw(tmpnam);
use FindBin;
use lib "$FindBin::Bin"; #locates pipeline directory
use IAP::sge;


sub runTelomerecat {
    ###
    # Run Telomerecat per sample
    ###
    my $configuration = shift;
    my %opt = %{$configuration};
    my @telomerecat_jobs;

    foreach my $sample (@{$opt{SAMPLES}}){
	###
	# Setup sample variables
	###
	my $sample_bam = "$opt{OUTPUT_DIR}/$sample/mapping/$opt{BAM_FILES}->{$sample}";
	my $log_dir = $opt{OUTPUT_DIR}."/".$sample."/logs/";
	my $tmp_dir = $opt{OUTPUT_DIR}."/".$sample."/tmp/";
	my $job_dir = $opt{OUTPUT_DIR}."/".$sample."/jobs/";
	my $output_dir = $opt{OUTPUT_DIR}."/".$sample."/";
	my $command;
	my @running_jobs;

	if (-e "$log_dir/TELOMERECAT_$sample.done"){
	    print "WARNING: $log_dir/TELOMERECAT_$sample.done exists, skipping TELOMERECAT analysis for $sample \n";
	} else {
	    ## Setup telomerecat sh script
	    my $jobID = "TELOMERECAT_$sample\_".get_job_id();
	    my $bashFile = $job_dir.$jobID.".sh";
	    my $output_csv = $sample."_telomerecat_length.csv";
	    #my $output_baf = $sample."_BAF.txt";
	    #my $output_bafplot = $sample."_BAF.pdf";

	    open TELOMERE_SH, ">$bashFile" or die "cannot open file $bashFile \n";
	    print TELOMERE_SH "#!/bin/bash\n\n";
	    print TELOMERE_SH "bash $opt{CLUSTER_PATH}/settings.sh\n\n";
	    print TELOMERE_SH "cd $tmp_dir\n\n";

	    ## Running jobs
	    if ( @{$opt{RUNNING_JOBS}->{$sample}} ){
		push( @running_jobs, @{$opt{RUNNING_JOBS}->{$sample}} );
	    }

		### Build telomerecat command
		$command = "telomerecat bam2length -x -p $opt{TELOMERECAT_THREADS} --output $output_csv $sample_bam";

		#Create UG bash script
		print TELOMERE_SH "echo \"Start Telomerecat\t\" `date` \"\t\" `uname -n` >> $log_dir/TELOMERECAT_$sample.log\n";
		print TELOMERE_SH " . $opt{TELOMERECAT_PATH}\n\n";

		print TELOMERE_SH "if [ -s $sample_bam ]\n";
		print TELOMERE_SH "then\n";
		print TELOMERE_SH "\t$command\n";
		print TELOMERE_SH "else\n";
		print TELOMERE_SH "\techo \"ERROR: Sample bam file do not exist.\" >&2\n";
		print TELOMERE_SH "fi\n\n";

		print TELOMERE_SH "if [ \"\$(tail -n 1 $output_csv | cut -f 1 -d',')\" = \"\$(basename $sample_bam)\" ]\n";
		print TELOMERE_SH "then\n";
		print TELOMERE_SH "\tmv $output_csv $output_dir\n";
		print TELOMERE_SH "\ttouch $log_dir/TELOMERECAT_$sample.done\n";
		print TELOMERE_SH "fi\n";
		print TELOMERE_SH "echo \"Finished Telomerecat\t\" `date` \"\t\" `uname -n` >> $log_dir/TELOMERE_$sample.log\n\n";
	    ###
	    # Submit TELOMERECAT JOB
	    ###
	    my $qsub = &qsubJava(\%opt,"TELOMERECAT");
	    if (@running_jobs){
		system "$qsub -o $log_dir/TELOMERECAT_$sample.out -e $log_dir/TELOMERECAT_$sample.err -N $jobID -hold_jid ".join(",",@running_jobs)." $bashFile";
	    } else {
		system "$qsub -o $log_dir/TELOMERECAT_$sample.out -e $log_dir/TELOMERECAT_$sample.err -N $jobID $bashFile";
	    }
	    push(@telomerecat_jobs, $jobID);
	}
  }
    return \@telomerecat_jobs;
}

############
sub get_job_id {
    my $id = tmpnam();
    $id=~s/\/tmp\/file//;
    return $id;
}
############

1;
