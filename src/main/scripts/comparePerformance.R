drawPerfLine <- function(perf, col, maxFP) {
  calls <- c(perf$Calls, min(perf$Calls) - min(perf$TP), 0)
  tp <- c(perf$TP, 0, 0)
  fp <- calls - tp
  lines(fp[fp <= maxFP], tp[fp <= maxFP], col=col, lwd=3)
}

plotROC <- function(perfs, perfNames, totalDels, main, sim=TRUE) {
  maxFP <- totalDels
  plot(0, type="n", ylim=c(0, totalDels), xlim=c(0,maxFP), xlab=ifelse(sim, "False Positives", "Novel Predictions"), ylab="True Positives", main=main)
  perfCols <- rainbow(length(perfs))
  mapply(drawPerfLine, perfs, perfCols, MoreArgs=list(maxFP=maxFP))  
  legend("bottomright", legend=perfNames, col=perfCols, lwd=3, cex=.9)
}

#chr2 gt 125
totalDels <- 197
hydraPerf <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim/hydra_perf_chr2_sim_gt125.txt', header=TRUE)
svpipelinePerf <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim/jcvi_chr2_sim_t180_nosd_or_piledup_deletion_scores_perf_mw3_gt125.txt', header=TRUE)
breakdancerPerf <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim/breakdancer_bedpe_perf_gt125.txt', header=TRUE)

perfsList <- list(svpipeline=svpipelinePerf, hydra=hydraPerf, breakdancer=breakdancerPerf)
plotROC(perfsList, c("SVPipeline", "Hydra", "Breakdancer"), totalDels)

#chr2 gt 50
totalDels <- 372
hydraPerf <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim/chr2/hydra.perf.txt', header=TRUE)
cloudbreakPerf <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim/maxins_25000_sdseqfilter_t180.perf.txt', header=TRUE)
breakdancerPerf <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim/chr2/breakdancer.perf.txt', header=TRUE)
gasvPerf <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim/chr2/gasv.perf.txt', header=TRUE)
dellyPerf <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim/chr2/delly.perf.txt', header=TRUE)

perfsList <- list(hydra=hydraPerf, breakdancer=breakdancerPerf, gasv=gasvPerf, cloudbreak=cloudbreakPerf, delly=dellyPerf)
pdf('~/Documents/gene_rearrange/svpipeline/cslu_seminar_08132012/CHR2SIM_ROC_NEW.pdf')
plotROC(perfsList, c("Hydra", "Breakdancer","GASV", "Cloudbreak", "Delly"), totalDels, "chr2 Simulated (30X)")
dev.off()

#chr2 gt 50 LOW COVERAGE
totalDels <- 372
hydraPerf <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim/chr2_lc/hydra.perf.txt', header=TRUE)
cloudbreak <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim_chr2_lc/maxins_25000_sdseqfilter.perf.txt', header=TRUE)
cloudbreakPerf2 <- cloudbreak[seq(1,dim(cloudbreakPerf)[1],by=12),]
breakdancerPerf <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim/chr2_lc/breakdancer_35_2.perf.txt', header=TRUE)
gasvPerf <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim/chr2_lc/gasv.perf.txt', header=TRUE)
dellyPerf <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim/chr2_lc/delly.perf.txt', header=TRUE)
#breakdancerSensitive <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_lc/breakdancer_rmdup_sensitive_perf.txt', header=TRUE)
#cloudbreak.new <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_lc/test_max_insert_25000_sdseqfilter_t180.perf.txt', header=TRUE)

perfsList <- list(hydra=hydraPerf, breakdancer=breakdancerPerf, gasv=gasvPerf, cloudbreak=cloudbreakPerf2, delly=dellyPerf)
pdf('~/Documents/gene_rearrange/svpipeline/CHR2SIMLC_ROC_NEW.pdf')
plotROC(perfsList, c("Hydra", "Breakdancer", "GASV", "Cloudbreak", "Delly"), totalDels, "chr2 Simulated (5X)")
dev.off()

#chr2 gt 50 V. LOW COVERAGE
totalDels <- 372
hydraPerf <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_vlc/hydra_perf.txt', header=TRUE)
cloudbreak <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_vlc/b292f1f35e6fc997581e19b773ae4dd33beaf58e_r25_perf.txt', header=TRUE)
breakdancerPerf <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_vlc/breakdancer_rmdup_perf.txt', header=TRUE)
breakdancerSensitive <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_vlc/breakdancer_rmdup_sensitive_perf.txt', header=TRUE)
gasvPerf <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_vlc/gasv_perf.txt', header=TRUE)

perfsList <- list(hydra=hydraPerf, breakdancer=breakdancerPerf, gasv=gasvPerf, cloudbreak=cloudbreak)
pdf('~/Documents/svpipeline/CHR2SIM_VLC_ROC.pdf')
plotROC(perfsList, c("Hydra", "Breakdancer", "GASV", "Cloudbreak"), totalDels, "chr2 Simulated (3X)")
dev.off()


#chr2 gt 50 V. LOW COVERAGE w / mutations
totalDels <- 372
hydraPerf <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_vlc_mut/hydra_perf.txt', header=TRUE)
cloudbreak <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_vlc_mut/b292f1f35e6fc997581e19b773ae4dd33beaf58e_r25_perf.txt', header=TRUE)
breakdancerPerf <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_vlc_mut/breakdancer_rmdup_perf.txt', header=TRUE)
breakdancerSensitive <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_vlc_mut/breakdancer_rmdup_sensitive_perf.txt', header=TRUE)
gasvPerf <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_sim_vlc_mut/gasv_perf.txt', header=TRUE)

perfsList <- list(hydra=hydraPerf, breakdancer=breakdancerPerf, gasv=gasvPerf, cloudbreak=cloudbreak)
pdf('~/Documents/svpipeline/CHR2SIM_VLC_MUT_ROC.pdf')
plotROC(perfsList, c("Hydra", "Breakdancer", "GASV", "Cloudbreak"), totalDels, "chr2 Simulated (3X)")
dev.off()

# chr17
totalDels <- 199
hydra <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr17_sim/hydra2_perf_gt50.txt', header=TRUE)
breakdancer <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim/chr17/breakdancer_35_2_f4.perf.txt', header=TRUE)
gasv <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim/chr17/gasv_f4.perf.txt', header=TRUE)
cloudbreak <- read.table('~/Documents/gene_rearrange/sv/jcvi_sim_chr17/test_max_insert_25000_sdseqfilter_t180_f4.perf.txt', header=TRUE)

perfsList <- list(hydra, breakdancer, gasv, cloudbreak)
pdf('~/Documents/gene_rearrange/svpipeline/cslu_seminar_08132012/CHR17SIM_ROC.pdf')
plotROC(perfsList, c("Hydra", "Breakdancer", "GASV", "Cloudbreak"), totalDels, "chr17 Simulated (100X)")
dev.off()

# NA07051
totalDels <- 2548
hydra <- read.table('~/Documents/gene_rearrange/svpipeline/NA07051/hydra_perf.txt', header=TRUE)
breakdancer <- read.table('~/Documents/gene_rearrange/sv/1000genomes/NA07051_new/breakdancer_35_2_f4.perf.txt', header=TRUE)
gasv <- read.table('~/Documents/gene_rearrange/sv/1000genomes/NA07051_new/gasv_f4.perf.txt', header=TRUE)
cloudbreak <- read.table('~/Documents/gene_rearrange/sv/NA07051_new/test_max_insert_25000_sdhsd_filter_t120_f4.perf.txt', header=TRUE)

perfsList <- list(hydra, breakdancer, gasv, cloudbreak)
pdf('~/Documents/gene_rearrange/svpipeline/cslu_seminar_08132012/NA07051_ROC_NEW.pdf')
plotROC(perfsList, c("Hydra", "Breakdancer", "GASV", "Cloudbreak"), totalDels, "NA07051 (4X)", sim=FALSE)
dev.off()

# NA12156
totalDels <- 2179
breakdancer <- read.table('~/Documents/gene_rearrange/sv/1000genomes/NA12156/breakdancer_35_2_f4.perf.txt', header=TRUE)
gasv <- read.table('~/Documents/gene_rearrange/sv/1000genomes/NA12156/gasv_f4.perf.txt', header=TRUE)
cloudbreak <- read.table('~/Documents/gene_rearrange/sv/NA12156/test_max_insert_25000_sdhsd_filter_t135_f4.perf.txt', header=TRUE)
perfsList <- list(breakdancer, gasv, cloudbreak)
pdf('~/Documents/gene_rearrange/svpipeline/cslu_seminar_08132012/NA12156_ROC_NEW.pdf')
plotROC(perfsList, c("Breakdancer", "GASV", "Cloudbreak"), totalDels, "NA12156 (2X)", sim=FALSE)
dev.off()


#chr2 gt 50 100bp diploid
totalDels <- 500
cloudbreak <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/chr2mwf5_readGroupsRazerS3_new3_i94_s99_m1000_None_None_25000_25_sam_6_32fc04d_-1_-1_3_lrHeterozygous.perf.txt', header=TRUE, sep="\t")
breakdancer <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/final_predictions/breakdancer/bwa_new3_sort_35_2_4.bd.perf.txt', header=TRUE)
gasv <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/final_predictions/GASVPro/BAMToGASV.gasvpro.in.ALL.MCMCThreshold.clusters.pruned.clusters.perf.txt', header=TRUE)
delly <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/final_predictions/delly/delly_q0_c5_del_bwa_new3.perf.txt', header=TRUE)
pindel <- read.table('~/Documents/gene_rearrange/svpipeline/venter_chr2_100bp_dip/final_predictions/pindel/bwa_new3.pindel.perf.txt', header=TRUE)

cloudbreak <- cloudbreak[seq(1,dim(cloudbreak)[1],by=4),]

perfsList <- list(breakdancer=breakdancer, gasv=gasv, delly=delly, pindel=pindel, cloudbreak=cloudbreak)
pdf('~/Documents/gene_rearrange/svpipeline/CHR2SIM_ROC_NEW.pdf')
plotROC(perfsList, c("Breakdancer","GASVPro", "DELLY", "Pindel", "Cloudbreak"), totalDels, "diploid chr2 Simulated (30X)")
dev.off()

