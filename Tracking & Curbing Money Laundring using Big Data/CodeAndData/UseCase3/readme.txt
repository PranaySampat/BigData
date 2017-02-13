Run 3 stages with 3 Drivers
Stage 1
org.abm.mapreduce1.Driver1 with 2 args
arg0 input file(appended outputs from usecase1 & usecase2)
arg1 output path

Stage2
SpeciesIterator.SpeciesIterDriver with 4 args
arg0 op of stage 1
arg1 op path
arg2 no of recurssion
arg3 initial seed


Stage3

speciesViewer.SpeciesViewerDriver with 2 args
arg0- op of last iteration of stage2
arg1 - op 
