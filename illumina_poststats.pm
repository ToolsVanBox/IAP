#!/usr/bin/perl -w

##################################################################################################################################################
###This script is designed to run picard metrics per sample and create a PDF summary using the ..... tool generated by R.F. Ernst
###
###
###Author: S.W.Boymans
###Latest change: Created skeleton
###
###TODO: A lot
##################################################################################################################################################

package illumina_poststats;

use strict;
use POSIX qw(tmpnam);
use FindBin;

sub runPostStats {
    my $configuration = shift;
    my %opt = %{readConfiguration($configuration)};
    my $picard = "java -Xmx16G -jar $opt{PICARD_PATH}"; ## Edit memory here!! threads x maxMem?????
    my @runningJobs; #internal job array
    
    ### Run Picard for each sample
    foreach my $sample (@{$opt{SAMPLES}}){
	my $jobID;
	my $bam = $opt{OUTPUT_DIR}."/".$sample."/mapping/".$sample."_dedup.bam";

	my $picardOut = $opt{OUTPUT_DIR}."/".$sample."/picardStats/";
	unless(-e $picardOut or mkdir $picardOut) { die "Unable to create $picardOut \n"; }
	
	### Multiple metrics
	my $command = $picard."/CollectMultipleMetrics.jar VALIDATION_STRINGENCY=LENIENT R=$opt{GENOME} ASSUME_SORTED=TRUE OUTPUT=".$picardOut.$sample."_MultipleMetrics.txt INPUT=$bam PROGRAM=CollectAlignmentSummaryMetrics PROGRAM=CollectInsertSizeMetrics PROGRAM=QualityScoreDistribution PROGRAM=QualityScoreDistribution\n";
	$jobID = bashAndSubmit($command,$sample,\%opt);
	push(@runningJobs, $jobID);
	
	### Library Complexity
	$command = $picard."/EstimateLibraryComplexity.jar VALIDATION_STRINGENCY=LENIENT OUTPUT=".$picardOut.$sample."_LibComplexity.txt INPUT=$bam";
	$jobID = bashAndSubmit($command,$sample,\%opt);
	push(@runningJobs, $jobID);
	
	### Calculate HSMetrics -> only if target/bait file are present.
	if ( ($opt{POSTSTATS_TARGETS}) && ($opt{POSTSTATS_BAITS}) ) {
	    $command = $picard."/CalculateHsMetrics.jar VALIDATION_STRINGENCY=LENIENT R=$opt{GENOME} OUTPUT=".$picardOut.$sample."_HSMetrics.txt INPUT=$bam BAIT_INTERVALS=$opt{POSTSTATS_BAITS} TARGET_INTERVALS=$opt{POSTSTATS_TARGETS} METRIC_ACCUMULATION_LEVEL=SAMPLE";
	    $jobID = bashAndSubmit($command,$sample,\%opt);
	    push(@runningJobs, $jobID);
	}
    }
    ### Run plotilluminametrics
    my $command = "perl $FindBin::Bin/modules/plotIlluminaMetrics/plotIlluminaMetrics.pl ".join(" ",@{$opt{SAMPLES}});
    
    my $jobID = get_job_id();
    my $bashFile = $opt{OUTPUT_DIR}."/jobs/PICARD_".$jobID.".sh";
    my $logDir = $opt{OUTPUT_DIR}."/logs";
        
    open OUT, ">$bashFile" or die "cannot open file $bashFile\n";
    print OUT "#!/bin/bash\n\n";
    print OUT "cd $opt{OUTPUT_DIR}\n";
    print OUT "$command\n";
    system "qsub -q $opt{POSTSTATS_QUEUE} -pe threaded $opt{POSTSTATS_THREADS} -o $logDir -e $logDir -N PICARD_$jobID -hold_jid ".join(",",@runningJobs)." $bashFile"; #require two slots for memory reasons

}

sub readConfiguration{
    my $configuration = shift;
    
    my %opt = (
	
	'PICARD_PATH'		=> undef,
	'POSTSTATS_THREADS'	=> undef,
	'POSTSTATS_QUEUE'	=> undef,,
	'POSTSTATS_TARGETS'	=> undef,
	'POSTSTATS_BAITS'	=> undef,
	'GENOME'		=> undef,
	'OUTPUT_DIR'		=> undef,
	'RUNNING_JOBS'		=> {}, #do not use in .conf file
	'SAMPLES'		=> undef #do not use in .conf file
    );

    foreach my $key (keys %{$configuration}){
	$opt{$key} = $configuration->{$key};
    }

    if(! $opt{PICARD_PATH}){ die "ERROR: No PICARD_PATH found in .conf file\n" }
    if(! $opt{POSTSTATS_THREADS}){ die "ERROR: No POSTSTATS_THREADS found in .ini file\n" }
    if(! $opt{POSTSTATS_QUEUE}){ die "ERROR: No POSTSTATS_THREADS found in .ini file\n" }
    if(! $opt{GENOME}){ die "ERROR: No GENOME found in .conf file\n" }
    if(! $opt{OUTPUT_DIR}){ die "ERROR: No OUTPUT_DIR found in .conf file\n" }
    if(! $opt{SAMPLES}){ die "ERROR: No SAMPLES found\n" }
    return \%opt;
}


############
sub get_job_id {
   my $id = tmpnam(); 
      $id=~s/\/tmp\/file//;
   return $id;
}

sub bashAndSubmit {
    my $command = shift;
    my $sample = shift;
    my %opt = %{shift()};
    
    my $jobID = get_job_id();
    my $bashFile = $opt{OUTPUT_DIR}."/".$sample."/jobs/PICARD_".$sample."_".$jobID.".sh";
    my $logDir = $opt{OUTPUT_DIR}."/".$sample."/logs";
    
    open OUT, ">$bashFile" or die "cannot open file $bashFile\n";
    print OUT "#!/bin/bash\n\n";
    print OUT "cd $opt{OUTPUT_DIR}\n";
    print OUT "$command\n";
    
    if ( $opt{RUNNING_JOBS}->{$sample} ){
	system "qsub -q $opt{POSTSTATS_QUEUE} -pe threaded $opt{POSTSTATS_THREADS} -o $logDir -e $logDir -N PICARD_$jobID -hold_jid ".join(",",@{$opt{RUNNING_JOBS}->{$sample} })." $bashFile";
    } else {
	system "qsub -q $opt{POSTSTATS_QUEUE} -pe threaded $opt{POSTSTATS_THREADS} -o $logDir -e $logDir -N PICARD_$jobID $bashFile";
    }
    return "PICARD_$jobID";
}

############ 

1;