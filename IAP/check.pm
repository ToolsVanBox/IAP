#!/usr/bin/perl -w

########################################################################
### illumina_check.pm
### - Check the result of the pipeline based on the selected modules.
### - Remove tmp files if pipeline completed successfully.
### Author: R.F.Ernst
########################################################################

package IAP::check;

use strict;
use POSIX qw(tmpnam);
use FindBin;
use lib "$FindBin::Bin"; #locates pipeline directory
use IAP::sge;

sub runCheck {
    ###
    # Run checks and email result
    ###
    my $configuration = shift;
    my %opt = %{$configuration};
    my $runName = (split("/", $opt{OUTPUT_DIR}))[-1];
    my $doneFile;
    my @runningJobs;

    ### Create bash file
    my $jobID = $runName."_".get_job_id();
    my $bashFile = "$opt{OUTPUT_DIR}/jobs/check_".$jobID.".sh";
    open (BASH,">$bashFile") or die "ERROR: Couldn't create $bashFile\n";
    print BASH "\#!/bin/sh\n . $opt{CLUSTER_PATH}/settings.sh\n\n";

    ### Log file
    my $logFile = "$opt{OUTPUT_DIR}/logs/PipelineCheck.log";
    print BASH "failed=false \n";
    print BASH "rm $logFile \n";
    print BASH "echo \"Check and cleanup for run: $runName \" >>$logFile\n";

    ### pipeline version
    my $version = `git --git-dir $FindBin::Bin/.git describe --tags`;
    print BASH "echo \"Pipeline version: $version \" >>$logFile\n\n";
    print BASH "echo \"\">>$logFile\n\n"; ## empty line between samples

    ### Check fastq steps
    if( $opt{FASTQ} ){
	foreach my $fastq_file (keys %{$opt{FASTQ}}){
	    my $coreName = (split("/", $fastq_file))[-1];
	    $coreName =~ s/\.fastq.gz//;
	    my ($sampleName) = split("_", $coreName);
	    print BASH "echo \"Fastq: $sampleName - $coreName\" >>$logFile\n";
	    if($opt{PRESTATS} eq "yes" ){
		$doneFile = $opt{OUTPUT_DIR}."/$sampleName/logs/PreStats_$coreName.done";
		print BASH "if [ -f $doneFile ]; then\n";
		print BASH "\techo \"\t PreStats: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t PreStats: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
	    }
	}
	if ( $opt{RUNNING_JOBS}->{'preStats'} ){
	    push( @runningJobs, @{$opt{RUNNING_JOBS}->{'preStats'}} );
	}
    }

    print BASH "echo \"\" >>$logFile\n";

    ### Check sample steps
    foreach my $sample (@{$opt{SAMPLES}}){
	if(! $opt{VCF}) {
	    print BASH "echo \"Sample: $sample\" >>$logFile\n";
	    if($opt{MAPPING} eq "yes" && ! $opt{BAM}){
		$doneFile = $opt{OUTPUT_DIR}."/$sample/logs/Mapping_$sample.done";
		print BASH "if [ -f $doneFile ]; then\n";
		print BASH "\techo \"\t Mapping: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t Mapping: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
	    }
	    if($opt{INDELREALIGNMENT} eq "yes"){
		$doneFile = $opt{OUTPUT_DIR}."/$sample/logs/Realignment_$sample.done";
		print BASH "if [ -f $doneFile ]; then\n";
		print BASH "\techo \"\t Indel realignment: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t Indel realignment: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
	    }
	    if($opt{BASEQUALITYRECAL} eq "yes"){
		$doneFile = $opt{OUTPUT_DIR}."/$sample/logs/BaseRecalibration_$sample.done";
		print BASH "if [ -f $doneFile ]; then\n";
		print BASH "\techo \"\t Base recalibration: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t Base recalibration: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
	    }
	    if($opt{BAF} eq "yes"){
		$doneFile = $opt{OUTPUT_DIR}."/$sample/logs/BAF_$sample.done";
		print BASH "if [ -f $doneFile ]; then\n";
		print BASH "\techo \"\t BAF analysis: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t BAF analysis: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
		if ( $opt{RUNNING_JOBS}->{'baf'} ){
		    push( @runningJobs, @{$opt{RUNNING_JOBS}->{'baf'}} );
		}
	    }
      if($opt{TELOMERECAT} eq "yes"){
    $doneFile = $opt{OUTPUT_DIR}."/$sample/logs/TELOMERECAT_$sample.done";
    print BASH "if [ -f $doneFile ]; then\n";
    print BASH "\techo \"\t TELOMERECAT analysis: done \" >>$logFile\n";
    print BASH "else\n";
    print BASH "\techo \"\t TELOMERECAT analysis: failed \">>$logFile\n";
    print BASH "\tfailed=true\n";
    print BASH "fi\n";
    if ( $opt{RUNNING_JOBS}->{'telomerecat'} ){
        push( @runningJobs, @{$opt{RUNNING_JOBS}->{'telomerecat'}} );
    }
      }
	    if($opt{CALLABLE_LOCI} eq "yes"){
		$doneFile = $opt{OUTPUT_DIR}."/$sample/logs/CallableLoci_$sample.done";
		print BASH "if [ -f $doneFile ]; then\n";
		print BASH "\techo \"\t CallableLoci analysis: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t CallableLoci analysis: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
		if ( $opt{RUNNING_JOBS}->{'callable_loci'} ){
		    push( @runningJobs, @{$opt{RUNNING_JOBS}->{'callable_loci'}} );
		}
	    }
	    if($opt{FINGERPRINT} eq "yes"){
		$doneFile = $opt{OUTPUT_DIR}."/logs/Fingerprint_$sample.done";
		print BASH "if [ -f $doneFile ]; then\n";
		print BASH "\techo \"\t Fingerprint analysis: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t Fingerprint analysis: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
		if ( $opt{RUNNING_JOBS}->{'fingerprint'} ){
		    push( @runningJobs, $opt{RUNNING_JOBS}->{'fingerprint'} );
		}
	    }

	    print BASH "echo \"\">>$logFile\n\n"; ## empty line between samples
	}
	## Running jobs
	if ( @{$opt{RUNNING_JOBS}->{$sample}} ){
	    push( @runningJobs, @{$opt{RUNNING_JOBS}->{$sample}} );
	}
    }

    ### Check run steps
    if($opt{POSTSTATS} eq "yes" && ! $opt{VCF}){
	$doneFile = $opt{OUTPUT_DIR}."/logs/PostStats.done";
	print BASH "if [ -f $doneFile ]; then\n";
	print BASH "\techo \"PostStats: done \" >>$logFile\n";
	print BASH "else\n";
	print BASH "\techo \"PostStats: failed \">>$logFile\n";
	print BASH "\tfailed=true\n";
	print BASH "fi\n";
	if ( $opt{RUNNING_JOBS}->{'postStats'} ){
	    push( @runningJobs, $opt{RUNNING_JOBS}->{'postStats'} );
	}
    }
    if($opt{NIPT} eq "yes" && ! $opt{VCF}){
	$doneFile = $opt{OUTPUT_DIR}."/logs/NIPT.done";
	print BASH "if [ -f $doneFile ]; then\n";
	print BASH "\techo \"NIPT: done \" >>$logFile\n";
	print BASH "else\n";
	print BASH "\techo \"NIPT: failed \">>$logFile\n";
	print BASH "\tfailed=true\n";
	print BASH "fi\n";
	if ( $opt{RUNNING_JOBS}->{'nipt'} ){
	    push( @runningJobs, $opt{RUNNING_JOBS}->{'nipt'} );
	}
    }
    if($opt{VARIANT_CALLING} eq "yes" && ! $opt{VCF}){
	$doneFile = $opt{OUTPUT_DIR}."/logs/VariantCaller.done";
	print BASH "if [ -f $doneFile ]; then\n";
	print BASH "\techo \"Variant caller: done \" >>$logFile\n";
	print BASH "else\n";
	print BASH "\techo \"Variant caller: failed \">>$logFile\n";
	print BASH "\tfailed=true\n";
	print BASH "fi\n";
    }
    if($opt{SOMATIC_VARIANTS} eq "yes" && ! $opt{VCF}){
	print BASH "echo \"Somatic variants:\" >>$logFile\n";
	foreach my $sample (keys(%{$opt{SOMATIC_SAMPLES}})){
	    foreach my $sample_tumor (@{$opt{SOMATIC_SAMPLES}{$sample}{'tumor'}}){
		foreach my $sample_ref (@{$opt{SOMATIC_SAMPLES}{$sample}{'ref'}}){
		    my $sample_tumor_name = "$sample_ref\_$sample_tumor";
		    my $done_file = "$opt{OUTPUT_DIR}/somaticVariants/$sample_tumor_name/logs/$sample_tumor_name.done";
		    print BASH "if [ -f $done_file ]; then\n";
		    print BASH "\techo \"\t $sample_tumor_name: done \" >>$logFile\n";
		    print BASH "else\n";
		    print BASH "\techo \"\t $sample_tumor_name: failed \">>$logFile\n";
		    print BASH "\tfailed=true\n";
		    print BASH "fi\n";
		}
	    }
	}
	if ( $opt{RUNNING_JOBS}->{'somVar'} ){
	    push( @runningJobs, @{$opt{RUNNING_JOBS}->{'somVar'}} );
	}
    }
    if($opt{COPY_NUMBER} eq "yes" && ! $opt{VCF}){
	print BASH "echo \"Copy number analysis:\" >>$logFile\n";
	if($opt{CNV_MODE} eq "sample_control"){
	    foreach my $sample (keys(%{$opt{SOMATIC_SAMPLES}})){
		foreach my $sample_tumor (@{$opt{SOMATIC_SAMPLES}{$sample}{'tumor'}}){
		    foreach my $sample_ref (@{$opt{SOMATIC_SAMPLES}{$sample}{'ref'}}){
			my $sample_tumor_name = "$sample_ref\_$sample_tumor";
			my $done_file = "$opt{OUTPUT_DIR}/copyNumber/$sample_tumor_name/logs/$sample_tumor_name.done";
			print BASH "if [ -f $done_file ]; then\n";
			print BASH "\techo \"\t $sample_tumor_name: done \" >>$logFile\n";
			print BASH "else\n";
			print BASH "\techo \"\t $sample_tumor_name: failed \">>$logFile\n";
			print BASH "\tfailed=true\n";
			print BASH "fi\n";
		    }
		}
	    }
	} elsif($opt{CNV_MODE} eq "sample"){
	    foreach my $sample (@{$opt{SAMPLES}}){
		my $done_file = "$opt{OUTPUT_DIR}/copyNumber/$sample/logs/$sample.done";
		print BASH "if [ -f $done_file ]; then\n";
		print BASH "\techo \"\t $sample: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t $sample: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
	    }
	}
	if ( $opt{RUNNING_JOBS}->{'CNV'} ){
	    push( @runningJobs, @{$opt{RUNNING_JOBS}->{'CNV'}} );
	}
    }
    if($opt{SV_CALLING} eq "yes" && ! $opt{VCF}){
	print BASH "echo \"SV calling:\" >>$logFile\n";
  if($opt{SV_GRIDSS} eq "yes"){
	    # per sv type done file check
	  my $done_file = "$opt{OUTPUT_DIR}/structuralVariants/gridss/logs/GRIDSS.done";
		print BASH "if [ -f $done_file ]; then\n";
		print BASH "\techo \"\t GRIDSS: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t GRIDSS: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
  }
  if($opt{SV_DELLY} eq "yes"){
	    # per sv type done file check
	    my @svTypes = split/\t/, $opt{DELLY_SVTYPE};
	    foreach my $type (@svTypes){
		my $done_file = "$opt{OUTPUT_DIR}/structuralVariants/delly/logs/DELLY_$type.done";
		print BASH "if [ -f $done_file ]; then\n";
		print BASH "\techo \"\t Delly $type: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t Delly $type: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
	    }
	}
	if($opt{SV_MANTA} eq "yes"){
	    # Check single samples
	    foreach my $sample (@{$opt{SINGLE_SAMPLES}}){
		my $done_file = "$opt{OUTPUT_DIR}/structuralVariants/manta/logs/SV_MANTA_$sample.done";
		print BASH "if [ -f $done_file ]; then\n";
		print BASH "\techo \"\t Manta $sample: done \" >>$logFile\n";
		print BASH "else\n";
		print BASH "\techo \"\t Manta $sample: failed \">>$logFile\n";
		print BASH "\tfailed=true\n";
		print BASH "fi\n";
	    }
	    # Check somatic samples
	    foreach my $sample (keys(%{$opt{SOMATIC_SAMPLES}})){
		foreach my $sample_tumor (@{$opt{SOMATIC_SAMPLES}{$sample}{'tumor'}}){
		    foreach my $sample_ref (@{$opt{SOMATIC_SAMPLES}{$sample}{'ref'}}){
			my $sample_tumor_name = "$sample_ref\_$sample_tumor";
			my $done_file = "$opt{OUTPUT_DIR}/structuralVariants/manta/logs/SV_MANTA_$sample_tumor_name.done";
			print BASH "if [ -f $done_file ]; then\n";
			print BASH "\techo \"\t Manta $sample_tumor_name: done \" >>$logFile\n";
			print BASH "else\n";
			print BASH "\techo \"\t Manta $sample_tumor_name: failed \">>$logFile\n";
			print BASH "\tfailed=true\n";
			print BASH "fi\n";
		    }
		}
	    }
	}
	if ( $opt{RUNNING_JOBS}->{'sv'} ){
	    push( @runningJobs, @{$opt{RUNNING_JOBS}->{'sv'}} );
	}
    }
    if($opt{FILTER_VARIANTS} eq "yes"){
	$doneFile = $opt{OUTPUT_DIR}."/logs/VariantFilter.done";
	print BASH "if [ -f $doneFile ]; then\n";
	print BASH "\techo \"Variant filter: done \" >>$logFile\n";
	print BASH "else\n";
	print BASH "\techo \"Variant filter: failed \">>$logFile\n";
	print BASH "\tfailed=true\n";
	print BASH "fi\n";
    }
    if($opt{ANNOTATE_VARIANTS} eq "yes"){
	$doneFile = $opt{OUTPUT_DIR}."/logs/VariantAnnotation.done";
	print BASH "if [ -f $doneFile ]; then\n";
	print BASH "\techo \"Variant annotation: done \" >>$logFile\n";
	print BASH "else\n";
	print BASH "\techo \"Variant annotation: failed \">>$logFile\n";
	print BASH "\tfailed=true\n";
	print BASH "fi\n";
    }
    if($opt{VCF_UTILS} eq "yes"){
	$doneFile = $opt{OUTPUT_DIR}."/logs/VCF_UTILS.done";
	print BASH "if [ -f $doneFile ]; then\n";
	print BASH "\techo \"VCF Utils: done \" >>$logFile\n";
	print BASH "else\n";
	print BASH "\techo \"VCF Utils: failed \">>$logFile\n";
	print BASH "\tfailed=true\n";
	print BASH "fi\n";
	if ( $opt{RUNNING_JOBS}->{'VCF_UTILS'} ){
	    push( @runningJobs, $opt{RUNNING_JOBS}->{'VCF_UTILS'} );
	}
    }

    ### Check failed variable and mail report
    print BASH "echo \"\">>$logFile\n\n"; ## empty line after stats

    ### Pipeline failed
    print BASH "if [ \"\$failed\" = true  ]\n";
    print BASH "then\n";
    print BASH "\techo \"One or multiple step(s) of the pipeline failed. \" >>$logFile\n";
    print BASH "\tmail -s \"IAP FAILED $runName\" \"$opt{MAIL}\" < $logFile\n";

    ### Pipeline done
    print BASH "else\n";
    print BASH "\techo \"The pipeline completed successfully. The md5sum file will be created.\">>$logFile\n";

    # Remove files/folders based on CHECKING_RM and empty logs except .done files if pipeline completed successfully
    my @removeFiles = split(",", $opt{CHECKING_RM});
    foreach my $removeFile (@removeFiles){
	print BASH "\tfind $opt{OUTPUT_DIR} -name $removeFile -print0 | xargs -0 rm -r -- \n";
    }
    print BASH "\tfind $opt{OUTPUT_DIR}/logs -size 0 -not -name \"*.done\" -delete\n";
    print BASH "\tfind $opt{OUTPUT_DIR}/*/logs -size 0 -not -name \"*.done\" -delete\n";
    print BASH "\tfind $opt{OUTPUT_DIR}/somaticVariants/*/logs -size 0 -not -name \"*.done\" -delete\n";
    if($opt{INDELREALIGNMENT} eq "yes"){
	foreach my $sample (@{$opt{SAMPLES}}){
	    if($opt{MARKDUP_LEVEL} eq "sample" || $opt{MARKDUP_LEVEL} eq "lane"){
		print BASH "\trm $opt{OUTPUT_DIR}/$sample/mapping/$sample\_dedup.ba*\n";
	    } else {
		print BASH "\trm $opt{OUTPUT_DIR}/$sample/mapping/$sample.ba*\n";
	    }
	}
    }

    print BASH "\n\tcd $opt{OUTPUT_DIR}\n";

    # Run cleanup script if set to yes
    if($opt{CHECKING_CLEANUP} eq "yes"){
	print BASH "\tpython $opt{CHECKING_CLEANUP_SCRIPT} > logs/checking_cleanup.log 2> logs/checking_cleanup.log \n";
    }

    # Send email.
    print BASH "\tmail -s \"IAP DONE $runName\" \"$opt{MAIL}\" < $logFile\n";

    # Create md5sum.txt
    print BASH "\tfind . -type f \\( ! -iname \"md5sum.txt\" \\) -exec md5sum \"{}\" \\; > md5sum.txt\n";

    print BASH "fi\n";

    #Sleep to ensure that email is send from cluster.
    print BASH "sleep 5s \n";

    #Start main bash script
    my $qsub = &qsubTemplate(\%opt,"CHECKING");
    if (@runningJobs){
	system "$qsub -o /dev/null -e /dev/null -N check_$jobID -hold_jid ".join(",",@runningJobs)." $bashFile";
    } else {
	system "$qsub -o /dev/null -e /dev/null -N check_$jobID $bashFile";
    }
}

############
sub get_job_id {
    my $id = tmpnam();
    $id=~s/\/tmp\/file//;
    return $id;
}
############

1;
